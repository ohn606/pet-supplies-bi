"""
宠物用品电商 - BI数据可视化平台 (纯Python内置HTTP服务器)
使用Python标准库实现，无需任何第三方依赖
"""
import os
import re
import sys
import json
import time
import sqlite3
import urllib.parse
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WAREHOUSE_DIR = os.path.join(BASE_DIR, "warehouse")
REC_DIR = os.path.join(BASE_DIR, "recommendation")
ANALYTICS_DIR = os.path.join(BASE_DIR, "analytics")
DASHBOARD_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(WAREHOUSE_DIR, "pet_supplies_dw.db")
TEMPORAL_FILE = os.path.join(ANALYTICS_DIR, "temporal_rules.json")
LIFECYCLE_FILE = os.path.join(ANALYTICS_DIR, "pet_lifecycle.json")
AB_SIM_FILE = os.path.join(ANALYTICS_DIR, "ab_simulation_results.json")

# 判定"某月行为样本是否足以计算比率"的最小浏览 UV。
# 依据：2017-07/09/10 三个月的浏览 UV 分别只有 1 / 2 / 5，是 ETL 拼接时
# 落在估计窗口边缘的残留行；把它们当作有效月份会制造"有 5 期数据"的假象。
MIN_VALID_BROWSE_UV = 1000

# ---- 数据时间范围：由真实数据集构建阶段写入，避免硬编码 ----
DATA_PERIOD_FILE = os.path.join(BASE_DIR, "data", "raw", "data_period.json")
DEFAULT_START, DEFAULT_END = "2025-07-01", "2026-06-30"
SOURCE_TYPE = "simulated"
if os.path.exists(DATA_PERIOD_FILE):
    try:
        with open(DATA_PERIOD_FILE, "r", encoding="utf-8") as _f:
            _p = json.load(_f)
        DEFAULT_START = _p.get("start_date", DEFAULT_START)
        DEFAULT_END = _p.get("end_date", DEFAULT_END)
    except Exception:
        pass
# 若 ODS 数据字典标明为真实数据集，则标注来源类型供前端展示
try:
    with open(os.path.join(BASE_DIR, "data", "raw", "data_dictionary.json"), "r", encoding="utf-8") as _f:
        SOURCE_TYPE = json.load(_f).get("data_source_type", "simulated")
except Exception:
    pass


def query_json(sql, params=None):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        if params:
            rows = conn.execute(sql, params).fetchall()
        else:
            rows = conn.execute(sql).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


# ---- 输入净化：内网演示服务原样把 category/start/end 拼进 f-string，
#      虽无多语句注入风险（sqlite3 execute 单语句），但可被引号闭合篡改查询逻辑。
#      统一在此白名单/格式校验，SQL 拼接点只接受净化后的值。 ----
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_CATEGORY_WHITELIST = {"猫用品", "狗用品", "小宠用品", "水族用品",
                       "鸟类用品", "通用用品"}
_BEHAVIOR_WHITELIST = {"浏览", "搜索", "收藏", "加购", "下单", "评价"}


def clean_date(value, fallback):
    s = (value or "").strip()
    return s if _DATE_RE.match(s) else fallback


def clean_category(value):
    s = (value or "").strip()
    return s if s in _CATEGORY_WHITELIST else None


def clean_behavior_type(value):
    s = (value or "").strip()
    return s if s in _BEHAVIOR_WHITELIST else None


def clean_limit(value, default, lo=1, hi=10000):
    try:
        n = int(value)
    except (TypeError, ValueError):
        return default
    return max(lo, min(hi, n))

_temporal_cache = {"mtime": 0, "data": None}
_lifecycle_cache = {"mtime": 0, "data": None}

# ---- 行为事实表读取源：优先 ADS 预聚合表 ----
# fact_user_behavior 2,648,323 行，任何 COUNT(DISTINCT user_id) 都要全表扫描 +
# 建临时去重树，实测单次 10s。ads_user_behavior_daily 是同语义的
# (date_id, user_id, behavior_type) 粒度汇总，378,632 行（7× 压缩，
# 由 etl/build_ads_user_behavior_daily.py 构建并通过 9 项对账）。
# 表缺失（未跑 ETL / 旧环境）时自动降级到明细表，保证功能不中断。
_ROLLUP_TABLE = "ads_user_behavior_daily"
_rollup_ready = None


def behavior_source():
    """返回行为类查询应使用的 (表名, 事件数表达式)。"""
    global _rollup_ready
    if _rollup_ready is None:
        try:
            conn = sqlite3.connect(DB_PATH)
            row = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (_ROLLUP_TABLE,)).fetchone()
            conn.close()
            _rollup_ready = row is not None
        except Exception:
            _rollup_ready = False
    return (_ROLLUP_TABLE, "cnt") if _rollup_ready else ("fact_user_behavior", "1")


# ---- 只读结果缓存 ----
# 同一份数仓文件在一次演示/评审中不会被写入，重复请求返回同样的结果。
# 对 COUNT(DISTINCT) 这类重查询按「接口名 + 参数」记忆 5 分钟，
# 让第二次进入同一页面（或并发打开多个子页）从秒级降到毫秒级。
_CACHE = {}
_CACHE_TTL = 300.0


def cache_get(key):
    hit = _CACHE.get(key)
    if hit is not None and time.time() - hit[0] < _CACHE_TTL:
        return hit[1]
    return None


def cache_put(key, value):
    _CACHE[key] = (time.time(), value)


def memo(key, producer):
    """「有则取缓存，无则计算并写入」的便捷封装，供表级物化读取函数使用。"""
    hit = cache_get(key)
    if hit is not None:
        return hit
    value = producer()
    cache_put(key, value)
    return value


# 只读 GET 接口的统统可缓存：演示/评审期间数仓文件不会被写入，
# 同一「路径 + 参数」在 5 分钟内必然返回同一结果。
CACHEABLE_APIS = frozenset("""
overview sales_trend behavior_trend category_sales heatmap behavior_heatmap_detail
user_profile top_products product_sales_trend product_sales_detail product_summary
rfm_analysis recommendation_stats recommendation_daily
brand_analysis price_distribution rating_distribution category_detail
conversion_funnel user_path_analysis user_retention time_analysis
user_segments user_search_terms user_summary
pet_top pet_category_share pet_product_trend
temporal_rules repurchase_cycle replenishment_candidates pet_lifecycle
ab_presets
""".split())


def load_lifecycle():
    """加载宠物生命周期推断结果（按文件 mtime 缓存）"""
    try:
        mtime = os.path.getmtime(LIFECYCLE_FILE)
    except OSError:
        return None
    if _lifecycle_cache["data"] is None or _lifecycle_cache["mtime"] != mtime:
        try:
            with open(LIFECYCLE_FILE, "r", encoding="utf-8") as f:
                _lifecycle_cache["data"] = json.load(f)
            _lifecycle_cache["mtime"] = mtime
        except Exception:
            return None
    return _lifecycle_cache["data"]


def load_temporal():
    """加载时序购物篮分析结果（按文件 mtime 缓存）"""
    try:
        mtime = os.path.getmtime(TEMPORAL_FILE)
    except OSError:
        return None
    if _temporal_cache["data"] is None or _temporal_cache["mtime"] != mtime:
        try:
            with open(TEMPORAL_FILE, "r", encoding="utf-8") as f:
                _temporal_cache["data"] = json.load(f)
            _temporal_cache["mtime"] = mtime
        except Exception:
            return None
    return _temporal_cache["data"]


def hourly_activity_rows():
    """24 小时活跃分布。

    优先读 ADS 预聚合表 `ads_behavior_hourly`（24 行，由
    etl/build_ads_user_behavior_daily.py 构建并通过对账）；表缺失时降级到明细表，
    保证任何环境都能出图。原实现对 264 万行做 GROUP BY + 24 个
    COUNT(DISTINCT user_id)，实测 4.7s —— 这是 P1 首屏最慢的一环。
    """
    def produce():
        try:
            rows = query_json("SELECT behavior_hour, cnt, uv "
                              "FROM ads_behavior_hourly ORDER BY behavior_hour")
            if rows:
                return rows
        except sqlite3.Error:
            pass
        return query_json("SELECT behavior_hour, COUNT(*) as cnt, "
                          "COUNT(DISTINCT user_id) as uv "
                          "FROM fact_user_behavior GROUP BY behavior_hour "
                          "ORDER BY behavior_hour")
    return memo("hourly_activity", produce)


class APIHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        t0 = time.time()
        self.t0 = t0  # 供 log_message 计算真实耗时（慢请求打 SLOW 标记）
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        params = urllib.parse.parse_qs(parsed.query)

        # 静态文件
        if path == "/" or path == "":
            # 默认首页：v2.0 全新 SaaS 风格看板（登录 → 业务视角）。
            # 旧版暖色黏土风看板仍可通过 /dashboard.html 访问。
            self.serve_html("petops.html")
            self._access_log(path, t0, 200)
            return
        if path.endswith(".html"):
            self.serve_html(path.lstrip("/"))
            self._access_log(path, t0, 200)
            return
        # 静态资源：除 css/js 外还需放行图片，否则 /static/img/*.png 会落入
        # API 分支返回 JSON（{"error": ...}），浏览器解码失败导致商品图空白。
        if path.endswith((".css", ".js", ".png", ".jpg", ".jpeg",
                          ".gif", ".svg", ".webp", ".ico")):
            self.serve_static(path.lstrip("/"))
            self._access_log(path, t0, 200)
            return

        # CSV 流式导出（不走 JSON 序列化，避免大结果集内存翻倍）
        if path == "/api/export/data" and params.get("format", [""])[0] == "csv":
            try:
                self._export_csv(params)
                self._access_log(path, t0, 200)
            except Exception as e:
                self.send_error(500, str(e))
                self._access_log(path, t0, 500)
            return

        # API路由
        api = path[len("/api/"):] if path.startswith("/api/") else ""
        ckey = None
        if api and api in CACHEABLE_APIS and "format" not in params:
            ckey = (api, tuple(sorted((k, tuple(v)) for k, v in params.items())))
            cached = cache_get(ckey)
            if cached is not None:
                self.send_json(cached)
                self._access_log(path + " [cache]", t0, 200)
                return
        try:
            data = self.handle_api(path, params)
            if ckey is not None:
                cache_put(ckey, data)
            self.send_json(data)
            self._access_log(path, t0, 200)
        except Exception as e:
            self.send_error(500, str(e))
            self._access_log(path, t0, 500)

    def _access_log(self, path, t0, code):
        """访问日志：路径 + 状态码 + 耗时，追加写入 dashboard/server.log。

        原实现 log_message 被完全静默，线上排障无任何痕迹可查。
        """
        try:
            with open(os.path.join(DASHBOARD_DIR, "server.log"), "a",
                      encoding="utf-8") as f:
                f.write("%s %s %s %dms\n" % (
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    path, code, int((time.time() - t0) * 1000)))
        except Exception:
            pass  # 日志失败不影响请求本身
    
    def handle_api(self, path, params):
        if path == "/api/data_period":
            return self.api_data_period()
        elif path == "/api/overview":
            return self.api_overview()
        elif path == "/api/sales_trend":
            return self.api_sales_trend(params)
        elif path == "/api/sales_trend_range":
            return self.api_sales_trend_range(params)
        elif path == "/api/behavior_trend":
            return self.api_behavior_trend(params)
        elif path == "/api/category_sales":
            return self.api_category_sales()
        elif path == "/api/heatmap":
            return self.api_heatmap()
        elif path == "/api/behavior_heatmap_detail":
            return self.api_behavior_heatmap_detail(params)
        elif path == "/api/user_profile":
            return self.api_user_profile()
        elif path == "/api/top_products":
            return self.api_top_products(params)
        elif path == "/api/product_sales_trend":
            return self.api_product_sales_trend(params)
        elif path == "/api/product_sales_detail":
            return self.api_product_sales_detail(params)
        elif path == "/api/product_summary":
            return self.api_product_summary(params)
        elif path == "/api/rfm_analysis":
            return self.api_rfm_analysis()
        elif path == "/api/recommendation_stats":
            return self.api_recommendation_stats()
        elif path == "/api/recommendation_daily":
            return self.api_recommendation_daily(params)
        elif path == "/api/brand_analysis":
            return self.api_brand_analysis(params)
        elif path == "/api/price_distribution":
            return self.api_price_distribution(params)
        elif path == "/api/rating_distribution":
            return self.api_rating_distribution()
        elif path == "/api/category_detail":
            return self.api_category_detail(params)
        elif path == "/api/conversion_funnel":
            return self.api_conversion_funnel(params)
        elif path == "/api/user_path_analysis":
            return self.api_user_path_analysis(params)
        elif path == "/api/user_retention":
            return self.api_user_retention(params)
        elif path == "/api/time_analysis":
            return self.api_time_analysis()
        elif path == "/api/user_segments":
            return self.api_user_segments()
        elif path == "/api/user_search_terms":
            return self.api_user_search_terms(params)
        elif path == "/api/user_summary":
            return self.api_user_summary(params)
        elif path == "/api/pet_top":
            return self.api_pet_top(params)
        elif path == "/api/pet_category_share":
            return self.api_pet_category_share(params)
        elif path == "/api/pet_product_trend":
            return self.api_pet_product_trend(params)
        elif path == "/api/export/data":
            return self.api_export_data(params)
        elif path == "/api/temporal_rules":
            return self.api_temporal_rules(params)
        elif path == "/api/repurchase_cycle":
            return self.api_repurchase_cycle(params)
        elif path == "/api/replenishment_candidates":
            return self.api_replenishment_candidates(params)
        elif path == "/api/pet_lifecycle":
            return self.api_pet_lifecycle(params)
        elif path == "/api/ab_presets":
            return self.api_ab_presets(params)
        elif path == "/api/ab_simulate":
            return self.api_ab_simulate(params)
        else:
            return {"error": f"Unknown API: {path}"}
    
    def api_overview(self):
        conn = sqlite3.connect(DB_PATH)
        try:
            total_users = conn.execute("SELECT COUNT(*) FROM dim_user").fetchone()[0]
            total_orders = conn.execute("SELECT COUNT(*) FROM fact_orders WHERE order_status='已完成'").fetchone()[0]
            total_revenue = conn.execute("SELECT SUM(actual_amount) FROM fact_orders WHERE order_status='已完成'").fetchone()[0] or 0
            total_products = conn.execute("SELECT COUNT(*) FROM dim_product WHERE shelf_status=1").fetchone()[0]
            repurchase = conn.execute("SELECT COUNT(*) FROM dim_user WHERE lifetime_orders >= 2").fetchone()[0]
            total_reviews = conn.execute("SELECT COUNT(*) FROM fact_reviews").fetchone()[0]
            rfm = query_json("SELECT rfm_segment, COUNT(*) as cnt FROM dim_user WHERE lifetime_orders>0 GROUP BY rfm_segment")
            return {
                "total_users": total_users,
                "total_orders": total_orders,
                "total_revenue": round(total_revenue, 2),
                "total_products": total_products,
                "repurchase_users": repurchase,
                "repurchase_rate": round(repurchase / total_users * 100, 1) if total_users else 0,
                "total_reviews": total_reviews,
                "rfm_distribution": rfm
            }
        finally:
            conn.close()
    
    def api_data_period(self):
        """返回当前 ODS 数据的时间范围与来源类型，供前端日期筛选器自适应"""
        return {"start_date": DEFAULT_START, "end_date": DEFAULT_END,
                "source_type": SOURCE_TYPE}

    @staticmethod
    def _limit_sql(params, default=0):
        """days<=0（默认）表示返回完整数据周期，不再硬截断。

        原实现默认 LIMIT 90、前端传 270，均会截掉数据窗口尾部，
        导致趋势图只画前半段且与 KPI 口径不符。
        """
        try:
            days = int(params.get("days", [default])[0])
        except (TypeError, ValueError):
            days = default
        return f"LIMIT {days}" if days and days > 0 else ""

    def api_sales_trend(self, params):
        limit_sql = self._limit_sql(params)
        return query_json(f"""
            SELECT dd.date_str, dd.day_name, dd.is_weekend,
                   COALESCE(ads.order_count, 0) as order_count,
                   COALESCE(ads.actual_amount, 0) as revenue,
                   COALESCE(ads.user_count, 0) as user_count,
                   COALESCE(ads.avg_order_amount, 0) as avg_order
            FROM dim_date dd
            LEFT JOIN ads_daily_sales ads ON dd.date_id = ads.date_id
            WHERE dd.date_str >= '{DEFAULT_START}'
              AND dd.date_str <= '{DEFAULT_END}'
            ORDER BY dd.date_id
            {limit_sql}
        """)
    
    def api_behavior_trend(self, params):
        limit_sql = self._limit_sql(params)
        return query_json(f"""
            SELECT dd.date_str, dd.day_name,
                   COALESCE(adb.pv, 0) as pv,
                   COALESCE(adb.uv, 0) as uv,
                   COALESCE(adb.collect_count, 0) as collect_count,
                   COALESCE(adb.cart_add_count, 0) as cart_add_count
            FROM dim_date dd
            LEFT JOIN ads_daily_behavior adb ON dd.date_id = adb.date_id
            WHERE dd.date_str >= '{DEFAULT_START}'
              AND dd.date_str <= '{DEFAULT_END}'
            ORDER BY dd.date_id
            {limit_sql}
        """)
    
    def api_category_sales(self):
        return query_json("""
            SELECT main_category, sub_category,
                   SUM(sales_amount) as revenue,
                   SUM(order_count) as orders,
                   SUM(sales_quantity) as quantity
            FROM ads_category_sales
            GROUP BY main_category, sub_category
            ORDER BY revenue DESC
        """)
    
    def api_heatmap(self):
        """24 小时活跃分布。

        真实数据集中订单仅具备日期粒度（Amazon 源），小时级分布改用行为事实表
        （淘宝 UserBehavior 含秒级时间戳）统计，保证时段分析结论真实可信。
        """
        rows = hourly_activity_rows()
        return [{"order_hour": r["behavior_hour"], "cnt": r["cnt"],
                 "revenue": 0, "avg_amount": 0, "uv": r["uv"]} for r in rows]
    
    def api_user_profile(self):
        pet_data = query_json("SELECT pet_type, COUNT(*) as cnt FROM dim_user GROUP BY pet_type")
        city_data = query_json("SELECT city_tier, COUNT(*) as cnt FROM dim_user GROUP BY city_tier")
        consume_data = query_json("""
            SELECT consume_level, COUNT(*) as cnt, AVG(lifetime_amount) as avg_amount
            FROM dim_user WHERE lifetime_orders>0 GROUP BY consume_level
        """)
        age_data = query_json("""
            SELECT CASE WHEN age IS NULL OR age <= 0 THEN '未知'
                        WHEN age < 25 THEN '18-24' WHEN age < 30 THEN '25-29'
                        WHEN age < 35 THEN '30-34' WHEN age < 40 THEN '35-39'
                        WHEN age < 45 THEN '40-44' ELSE '45+' END as age_group,
                   COUNT(*) as cnt
            FROM dim_user GROUP BY age_group ORDER BY age_group
        """)
        return {"pet_type": pet_data, "city_tier": city_data,
                "consume_level": consume_data, "age_group": age_data}
    
    def api_top_products(self, params):
        limit = clean_limit(params.get("limit", [20])[0], 20)
        return query_json(f"""
            SELECT product_name, main_category, sub_category,
                   price, total_sales, total_revenue,
                   ROUND(avg_rating, 1) as avg_rating, review_count
            FROM dim_product WHERE total_sales > 0
            ORDER BY total_sales DESC LIMIT {limit}
        """)
    
    def api_rfm_analysis(self):
        return query_json("""
            SELECT rfm_segment, COUNT(*) as cnt,
                   ROUND(AVG(lifetime_amount), 1) as avg_amount,
                   ROUND(AVG(lifetime_orders), 1) as avg_orders,
                   ROUND(AVG(avg_order_amount), 1) as avg_aov
            FROM dim_user WHERE lifetime_orders>0 GROUP BY rfm_segment
        """)
    
    # 模型综合评分权重：单一权威出处，随响应下发给前端，
    # 避免权重散落在 JS 硬编码（曾与论文口径脱节的风险点）。
    REC_SCORE_WEIGHTS = {"f1": 0.30, "hit_rate": 0.20, "coverage": 0.20,
                         "diversity": 0.15, "novelty": 0.15}

    def api_recommendation_stats(self):
        result_path = os.path.join(REC_DIR, "recommendation_results.json")
        if os.path.exists(result_path):
            with open(result_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                # 权重以训练脚本写入的为准，服务端仅在缺失时补默认值
                data.setdefault("score_weights", dict(self.REC_SCORE_WEIGHTS))
            return data
        return {"error": "推荐结果尚未生成", "results": {},
                "score_weights": dict(self.REC_SCORE_WEIGHTS)}
    
    def api_brand_analysis(self, params):
        """品牌销售分析（**随筛选联动**）

        修正说明（审计 C4 / Q3）：原实现读 `dim_product.total_revenue` 等累计字段，
        结果恒为全周期口径，时间筛选器与类目筛选对该图完全失效——用户改了日期
        而图不动，会被判定为"看板坏了"。此处改为对 `fact_order_items` 按筛选期
        实时聚合，与页面 KPI、趋势图保持同源同口径。
        """
        start, end = self._get_date_range(params)
        category = clean_category(params.get("category", [None])[0])
        cat_sql = " AND dp.main_category = ?" if category else ""
        sql_params = [start, end] + ([category] if category else [])
        return query_json(f"""
            SELECT dp.brand,
                   COUNT(DISTINCT dp.product_id) as product_count,
                   SUM(foi.quantity) as total_sales,
                   ROUND(SUM(foi.item_amount), 0) as total_revenue,
                   ROUND(AVG(dp.price), 1) as avg_price,
                   ROUND(AVG(dp.avg_rating), 2) as avg_rating
            FROM fact_order_items foi
            JOIN dim_product dp ON foi.product_id = dp.product_id
            JOIN dim_date dd ON foi.order_date_id = dd.date_id
            WHERE dd.date_str >= ? AND dd.date_str <= ? {cat_sql}
            GROUP BY dp.brand
            ORDER BY total_revenue DESC LIMIT 30
        """, sql_params)

    def api_price_distribution(self, params):
        """价格区间分布

        口径说明：价格是**商品主数据属性**（非交易事实），随时间变化的幅度极小，
        历史上这一页改的是销售额而非价格本身。此处保持全周期商品分布，但对
        `revenue/sales` 改为筛选期聚合并保留 `product_count` 全周期，由前端在图头
        标注「商品数=全周期 / 销售额=筛选期」，避免读者误读为同一口径。
        """
        start, end = self._get_date_range(params)
        # 筛选期内动销的商品品数贡献，用于叠加真实销售表现
        period_rows = query_json("""
            SELECT CASE
                        WHEN dp.price < 30 THEN '0-30元'
                        WHEN dp.price < 80 THEN '30-80元'
                        WHEN dp.price < 150 THEN '80-150元'
                        WHEN dp.price < 300 THEN '150-300元'
                        WHEN dp.price < 600 THEN '300-600元'
                        ELSE '600元以上'
                   END as price_range,
                   SUM(foi.quantity) as period_sales,
                   ROUND(SUM(foi.item_amount), 0) as period_revenue
            FROM fact_order_items foi
            JOIN dim_product dp ON foi.product_id = dp.product_id
            JOIN dim_date dd ON foi.order_date_id = dd.date_id
            WHERE dd.date_str >= ? AND dd.date_str <= ?
            GROUP BY price_range
        """, (start, end))
        period_map = {r["price_range"]: r for r in period_rows}
        rows = query_json("""
            SELECT
                CASE
                    WHEN price < 30 THEN '0-30元'
                    WHEN price < 80 THEN '30-80元'
                    WHEN price < 150 THEN '80-150元'
                    WHEN price < 300 THEN '150-300元'
                    WHEN price < 600 THEN '300-600元'
                    ELSE '600元以上'
                END as price_range,
                COUNT(*) as product_count,
                SUM(total_sales) as total_sales,
                ROUND(SUM(total_revenue), 0) as revenue
            FROM dim_product GROUP BY price_range ORDER BY MIN(price)
        """)
        out = []
        for r in rows:
            p = period_map.get(r["price_range"], {})
            out.append({**r,
                        "period_sales": p.get("period_sales", 0),
                        "period_revenue": p.get("period_revenue", 0),
                        "note": ("product_count=全周期在架商品数；"
                                 "period_revenue=筛选期销售额")})
        return out
    
    def api_rating_distribution(self):
        """评分分布"""
        return query_json("""
            SELECT 
                CASE 
                    WHEN avg_rating < 3.0 THEN '3.0以下'
                    WHEN avg_rating < 3.5 THEN '3.0-3.5'
                    WHEN avg_rating < 4.0 THEN '3.5-4.0'
                    WHEN avg_rating < 4.5 THEN '4.0-4.5'
                    ELSE '4.5-5.0'
                END as rating_range,
                COUNT(*) as product_count,
                SUM(total_sales) as total_sales
            FROM dim_product WHERE total_sales > 0
            GROUP BY rating_range ORDER BY MIN(avg_rating)
        """)
    
    def api_category_detail(self, params):
        """类目详细分析（**随筛选联动**）

        同 `api_brand_analysis` 的修正：由 `dim_product` 累计字段改为筛选期
        `fact_order_items` 实时聚合；类目筛选命中时收敛到指定一级类目，使页面上的
        树图与堆叠柱图与 KPI、趋势图完全同源。
        """
        start, end = self._get_date_range(params)
        category = clean_category(params.get("category", [None])[0])
        cat_sql = " AND dp.main_category = ?" if category else ""
        sql_params = [start, end] + ([category] if category else [])
        return query_json(f"""
            SELECT dp.main_category, dp.sub_category,
                   COUNT(DISTINCT dp.product_id) as product_count,
                   SUM(foi.quantity) as total_sales,
                   ROUND(SUM(foi.item_amount), 0) as revenue,
                   ROUND(AVG(dp.price), 1) as avg_price,
                   ROUND(AVG(dp.avg_rating), 2) as avg_rating,
                   SUM(dp.review_count) as total_reviews
            FROM fact_order_items foi
            JOIN dim_product dp ON foi.product_id = dp.product_id
            JOIN dim_date dd ON foi.order_date_id = dd.date_id
            WHERE dd.date_str >= ? AND dd.date_str <= ? {cat_sql}
            GROUP BY dp.main_category, dp.sub_category
            ORDER BY revenue DESC
        """, sql_params)
    
    def api_time_analysis(self):
        """时段分析（行为口径）

        修正说明：原实现按 fact_orders.order_hour 分组，但公开真实数据集的订单
        仅有日期粒度（order_hour 恒为 0），聚合结果会退化为"全部 169,708 笔订单
        集中在 0 时"的失真结论。此处与 /api/heatmap 统一改为行为事实表口径
        （淘宝 UserBehavior 含秒级时间戳），并在返回体中显式标注来源。
        """
        rows = hourly_activity_rows()
        return {"source": "fact_user_behavior(淘宝 UserBehavior)",
                "note": "真实订单无小时粒度，时段分析统一采用行为口径；"
                        "读 ADS 预聚合表 ads_behavior_hourly（全周期 24 行）",
                "data": [{"order_hour": r["behavior_hour"], "order_count": r["cnt"],
                         "uv": r["uv"]} for r in rows]}
    
    def api_user_segments(self):
        """用户分层分析"""
        conn = sqlite3.connect(DB_PATH)
        try:
            # 按宠物类型
            pet = query_json("""
                SELECT pet_type, COUNT(*) as cnt,
                       ROUND(AVG(lifetime_amount), 0) as avg_spend,
                       SUM(CASE WHEN lifetime_orders >= 2 THEN 1 ELSE 0 END) as repurchase
                FROM dim_user WHERE lifetime_orders > 0
                GROUP BY pet_type ORDER BY cnt DESC
            """)
            # 按城市等级
            city = query_json("""
                SELECT city_tier, COUNT(*) as cnt,
                       ROUND(AVG(lifetime_amount), 0) as avg_spend
                FROM dim_user WHERE lifetime_orders > 0
                GROUP BY city_tier ORDER BY cnt DESC
            """)
            # 按消费层级
            consume = query_json("""
                SELECT consume_level, COUNT(*) as cnt,
                       ROUND(AVG(lifetime_amount), 0) as avg_spend,
                       ROUND(AVG(lifetime_orders), 1) as avg_orders
                FROM dim_user WHERE lifetime_orders > 0
                GROUP BY consume_level ORDER BY avg_spend DESC
            """)
            # 按活跃度
            activity = query_json("""
                SELECT activity_level, COUNT(*) as cnt,
                       ROUND(AVG(lifetime_amount), 0) as avg_spend
                FROM dim_user WHERE lifetime_orders > 0
                GROUP BY activity_level ORDER BY avg_spend DESC
            """)
            return {"pet_type": pet, "city_tier": city, "consume_level": consume, "activity": activity}
        finally:
            conn.close()
    
    def _get_date_range(self, params):
        """解析时间范围参数，返回 (start_date, end_date)。

        start/end 经 clean_date 白名单校验（YYYY-MM-DD 格式），非法值回退
        到数据周期默认值，杜绝通过引号闭合篡改 SQL。
        """
        start = clean_date(params.get("start", [None])[0], DEFAULT_START)
        end = clean_date(params.get("end", [None])[0], DEFAULT_END)
        return start, end

    @staticmethod
    def _date_to_id(date_str):
        """将 'YYYY-MM-DD' 转换为 YYYYMMDD 整数（用于直接过滤 date_id）"""
        return int(date_str.replace("-", ""))

    def api_sales_trend_range(self, params):
        """支持时间范围筛选的销售趋势"""
        start, end = self._get_date_range(params)
        return query_json(f"""
            SELECT dd.date_str, dd.day_name, dd.is_weekend, dd.year_month,
                   COALESCE(ads.order_count, 0) as order_count,
                   COALESCE(ads.actual_amount, 0) as revenue,
                   COALESCE(ads.user_count, 0) as user_count,
                   COALESCE(ads.avg_order_amount, 0) as avg_order,
                   COALESCE(ads.new_user_count, 0) as new_users,
                   COALESCE(ads.promo_order_count, 0) as promo_orders
            FROM dim_date dd
            LEFT JOIN ads_daily_sales ads ON dd.date_id = ads.date_id
            WHERE dd.date_str >= '{start}' AND dd.date_str <= '{end}'
            ORDER BY dd.date_id
        """)

    def api_product_sales_trend(self, params):
        """商品/类目销售趋势（按日期）"""
        start, end = self._get_date_range(params)
        category = clean_category(params.get("category", [None])[0])
        cat_filter = f"AND dp.main_category = '{category}'" if category else ""
        return query_json(f"""
            SELECT dd.date_str, dd.year_month,
                   COUNT(DISTINCT foi.order_id) as order_count,
                   SUM(foi.quantity) as sales_quantity,
                   SUM(foi.item_amount) as revenue,
                   COUNT(DISTINCT foi.user_id) as user_count,
                   COUNT(DISTINCT foi.product_id) as product_count
            FROM dim_date dd
            JOIN fact_order_items foi ON dd.date_id = foi.order_date_id
            JOIN dim_product dp ON foi.product_id = dp.product_id
            WHERE dd.date_str >= '{start}' AND dd.date_str <= '{end}' {cat_filter}
            GROUP BY dd.date_str ORDER BY dd.date_id
        """)

    def api_product_sales_detail(self, params):
        """商品销售明细（支持时间范围和类目筛选）"""
        start, end = self._get_date_range(params)
        category = clean_category(params.get("category", [None])[0])
        limit = clean_limit(params.get("limit", [30])[0], 30)
        cat_filter = f"AND dp.main_category = '{category}'" if category else ""
        return query_json(f"""
            SELECT dp.product_id, dp.product_name, dp.main_category, dp.sub_category,
                   dp.brand, dp.price, dp.avg_rating, dp.review_count,
                   COALESCE(dp.image_url, '') as image_url,
                   COUNT(DISTINCT foi.order_id) as order_count,
                   SUM(foi.quantity) as sales_quantity,
                   ROUND(SUM(foi.item_amount), 0) as revenue,
                   COUNT(DISTINCT foi.user_id) as user_count
            FROM fact_order_items foi
            JOIN dim_product dp ON foi.product_id = dp.product_id
            JOIN dim_date dd ON foi.order_date_id = dd.date_id
            WHERE dd.date_str >= '{start}' AND dd.date_str <= '{end}' {cat_filter}
              AND dp.total_sales > 0
            GROUP BY dp.product_id
            ORDER BY revenue DESC
            LIMIT {limit}
        """)

    def api_product_summary(self, params):
        """商品分析页 KPI 汇总（动销商品数 / 品牌数量 / 平均评分）

        修复要点：原实现用 LIMIT 30 的明细列表去重统计，导致 KPI 被截断
        （如"动销商品数 30"），口径失真。此处按时间范围/类目对全量订单明细
        去重统计，保证 KPI 真实可信。
        """
        start, end = self._get_date_range(params)
        category = clean_category(params.get("category", [None])[0])
        cat_filter = f"AND dp.main_category = '{category}'" if category else ""
        rows = query_json(f"""
            SELECT COUNT(*) as unique_products,
                   COUNT(DISTINCT brand) as unique_brands,
                   ROUND(AVG(avg_rating), 2) as avg_rating
            FROM (
                SELECT DISTINCT dp.product_id, dp.brand, dp.avg_rating
                FROM fact_order_items foi
                JOIN dim_product dp ON foi.product_id = dp.product_id
                JOIN dim_date dd ON foi.order_date_id = dd.date_id
                WHERE dd.date_str >= '{start}' AND dd.date_str <= '{end}' {cat_filter}
            )
        """)
        return rows[0] if rows else {"unique_products": 0, "unique_brands": 0, "avg_rating": 0}

    def api_behavior_heatmap_detail(self, params):
        """用户行为热力图数据（小时×星期）- 优化版：用 date_id 直接过滤"""
        start, end = self._get_date_range(params)
        start_id = self._date_to_id(start)
        end_id = self._date_to_id(end)
        btype = clean_behavior_type(params.get("type", [None])[0])
        type_filter = f"AND behavior_type = '{btype}'" if btype else ""
        # 使用 substr(CAST(date_id AS TEXT),...) 提取星期信息
        # date_id 格式 YYYYMMDD，用 date() 函数获取星期
        return query_json(f"""
            SELECT behavior_hour as hour,
                   CAST(strftime('%w', substr(CAST(date_id AS TEXT),1,4)||'-'||substr(CAST(date_id AS TEXT),5,2)||'-'||substr(CAST(date_id AS TEXT),7,2)) AS INTEGER) as weekday,
                   CASE CAST(strftime('%w', substr(CAST(date_id AS TEXT),1,4)||'-'||substr(CAST(date_id AS TEXT),5,2)||'-'||substr(CAST(date_id AS TEXT),7,2)) AS INTEGER)
                     WHEN 0 THEN '周日' WHEN 1 THEN '周一' WHEN 2 THEN '周二'
                     WHEN 3 THEN '周三' WHEN 4 THEN '周四' WHEN 5 THEN '周五' WHEN 6 THEN '周六'
                   END as day_name,
                   COUNT(*) as cnt
            FROM fact_user_behavior
            WHERE date_id >= {start_id} AND date_id <= {end_id} {type_filter}
            GROUP BY behavior_hour, weekday
            ORDER BY weekday, behavior_hour
        """)

    def api_conversion_funnel(self, params):
        """转化漏斗（支持时间范围）

        重要：多源融合场景下，行为事实表与订单事实表来自不同数据源（用户空间不同）。
        为保证漏斗各环节口径一致，全部环节均取自行为事实表 fact_user_behavior
        （淘宝 UserBehavior 的 pv/fav/cart/buy 真实行为序列）。
        """
        start, end = self._get_date_range(params)
        start_id = self._date_to_id(start)
        end_id = self._date_to_id(end)
        conn = sqlite3.connect(DB_PATH)
        try:
            stage_defs = [("浏览", "浏览"), ("收藏", "收藏"), ("加购", "加购"), ("下单", "下单")]
            # 原实现按阶段循环 4 次 COUNT(DISTINCT)，等于全表扫 4 遍（冷缓存约 5s）。
            # 改为一次 GROUP BY 取回四类，并从 ADS 预聚合表读取，符合漏斗
            # 「同源同窗口」口径的前提下把耗时降到百毫秒级。
            tbl, _pv = behavior_source()
            placeholders = ",".join(["?"] * len(stage_defs))
            agg = dict(conn.execute(
                f"SELECT behavior_type, COUNT(DISTINCT user_id) FROM {tbl} "
                f"WHERE date_id>=? AND date_id<=? AND behavior_type IN ({placeholders}) "
                f"GROUP BY behavior_type",
                (start_id, end_id) + tuple(b for _, b in stage_defs)).fetchall())
            stages, counts = [], []
            for label, btype in stage_defs:
                stages.append(label)
                counts.append(agg.get(btype, 0))
            base = counts[0] if counts and counts[0] else 0
            rates = [100.0]
            for i in range(1, len(counts)):
                rates.append(round(counts[i] / counts[i - 1] * 100, 1) if counts[i - 1] else 0)
            overall = [round(c / base * 100, 2) if base else 0 for c in counts]
            return {"stages": stages, "counts": counts, "rates": rates,
                    "overall_rates": overall,
                    "source": "fact_user_behavior(淘宝 UserBehavior 真实行为序列)"}
        finally:
            conn.close()

    def api_user_path_analysis(self, params):
        """用户行为路径分析（桑基图数据）- 优化版：直接用 date_id 过滤"""
        start, end = self._get_date_range(params)
        start_id = self._date_to_id(start)
        end_id = self._date_to_id(end)
        tbl, _pv = behavior_source()
        conn = sqlite3.connect(DB_PATH)
        try:
            # 只用 (user_id, behavior_type) 组合做流转统计，因此 ADS 预聚合表
            # （同粒度去重）可直接替代 264 万行明细；product_cnt 在计算中未被使用，
            # 保留列仅为兼容下游取值，避免改动判断逻辑。
            rows = conn.execute(f"""
                SELECT user_id, behavior_type, 0 as product_cnt
                FROM {tbl}
                WHERE date_id >= ? AND date_id <= ?
                GROUP BY user_id, behavior_type
            """, (start_id, end_id)).fetchall()

            from collections import defaultdict
            user_behaviors = defaultdict(set)
            for uid, btype, _ in rows:
                user_behaviors[uid].add(btype)

            # 统计行为类型间的流转
            transitions = [
                ("浏览", "搜索"), ("浏览", "收藏"), ("浏览", "加购"),
                ("搜索", "收藏"), ("搜索", "加购"), ("收藏", "加购")
            ]
            link_counts = defaultdict(int)
            for bt in user_behaviors.values():
                for src, tgt in transitions:
                    if src in bt and tgt in bt:
                        link_counts[(src, tgt)] += 1

            # 加购→下单、下单→支付：用单次批量查询
            cart_users = [uid for uid, bt in user_behaviors.items() if "加购" in bt]
            if cart_users:
                # 一次性查询所有有加购行为且在时间范围内有订单的用户
                ph = ",".join(["?"] * len(cart_users))
                order_rows = conn.execute(f"""
                    SELECT DISTINCT user_id FROM fact_orders
                    WHERE user_id IN ({ph}) AND order_date_id >= ? AND order_date_id <= ?
                """, cart_users + [start_id, end_id]).fetchall()
                order_users = set(r[0] for r in order_rows)
                link_counts[("加购", "下单")] = sum(1 for uid in cart_users if uid in order_users)

                # 下单→支付
                if order_users:
                    order_list = list(order_users)
                    ph2 = ",".join(["?"] * len(order_list))
                    paid_rows = conn.execute(f"""
                        SELECT DISTINCT user_id FROM fact_orders
                        WHERE user_id IN ({ph2}) AND order_status='已完成'
                          AND order_date_id >= ? AND order_date_id <= ?
                    """, order_list + [start_id, end_id]).fetchall()
                    paid_users = set(r[0] for r in paid_rows)
                    link_counts[("下单", "支付")] = len(paid_users)

            path_order = {"浏览": 0, "搜索": 1, "收藏": 2, "加购": 3, "下单": 4, "支付": 5, "评价": 6}
            node_colors = {
                "浏览": "#d4763a", "搜索": "#e8a04a", "收藏": "#c8534e",
                "加购": "#7ba05b", "下单": "#6ba89a", "支付": "#a0688a", "评价": "#d4a045"
            }
            nodes_set = set()
            links = []
            for (src, tgt), cnt in sorted(link_counts.items(), key=lambda x: -x[1]):
                if cnt > 5:
                    nodes_set.add(src); nodes_set.add(tgt)
                    links.append({"source": src, "target": tgt, "value": cnt})

            nodes = [{"name": n, "itemStyle": {"color": node_colors.get(n, "#b8a395")}}
                     for n in sorted(nodes_set, key=lambda x: path_order.get(x, 99))]

            return {"nodes": nodes, "links": links}
        finally:
            conn.close()

    def api_user_retention(self, params):
        """用户留存分析（月度队列）

        修复要点：
        1. first_order_date / order_date_id 均为 YYYYMMDD，需 substr 取前 6 位得到 YYYYMM；
        2. 队列起点不再硬编码（原为 '20250701'），改为全量纳入，适配真实数据周期；
        3. 一次性取回 (user, cohort_month, order_month) 后在内存聚合，
           避免"队列 × 期数"的嵌套循环查询（真实数据下会超时）。
        """
        conn = sqlite3.connect(DB_PATH)
        try:
            rows = conn.execute("""
                SELECT du.user_id,
                       substr(du.first_order_date, 1, 6)              AS cohort_month,
                       substr(CAST(fo.order_date_id AS TEXT), 1, 6)   AS order_month
                FROM dim_user du
                JOIN fact_orders fo ON du.user_id = fo.user_id
                WHERE du.first_order_date IS NOT NULL AND du.first_order_date <> ''
                  AND fo.order_status = '已完成'
            """).fetchall()

            if not rows:
                return {"weeks": [], "cohorts": []}

            def mdiff(a, b):
                ay, am = int(a[:4]), int(a[4:6])
                by, bm = int(b[:4]), int(b[4:6])
                return (by - ay) * 12 + (bm - am)

            cohort_users = {}
            active = {}
            for uid, cohort, omonth in rows:
                if not cohort or not omonth:
                    continue
                cohort_users.setdefault(cohort, set()).add(uid)
                p = mdiff(cohort, omonth)
                if p < 0:
                    continue
                active.setdefault(cohort, {}).setdefault(p, set()).add(uid)

            months = sorted(cohort_users.keys())
            max_cohorts = min(len(months), 8)
            retention_data = []
            for i in range(max_cohorts):
                cm = months[i]
                size = len(cohort_users[cm])
                series = []
                for offset in range(0, 12):
                    cnt = len(active.get(cm, {}).get(offset, set()))
                    series.append(round(cnt / size * 100, 1) if size else 0.0)
                retention_data.append({"cohort": cm, "users": size, "retention": series})

            return {"weeks": months[:max_cohorts], "cohorts": retention_data}
        finally:
            conn.close()

    def api_recommendation_daily(self, params):
        """逐期行为转化率（bucket=day|month，默认 month）

        数据源：bucket=month 读 ADS 预聚合表 ads_behavior_monthly（15 行，毫秒级）；
                bucket=day  直查 fact_user_behavior，跨期>92 天自动降级为 month。

        为什么改成预聚合（v1.7.0）：
            月度口径原先每次请求都在 264.8 万行明细上做 COUNT(DISTINCT user_id)，
            单次 12s、5 并发最坏 15s。实测加覆盖索引 (date_id,behavior_type,user_id)
            反而更慢（6.96s→8.79s）——聚合代价无法靠索引消除，只能靠分层物化。

        口径说明（务必完整理解，否则会得出荒谬结论）：
        1. 该面板是**站点级行为指标**，并非推荐实验指标；
        2. fact_user_behavior 自身也是两源拼接：淘宝 UserBehavior 提供
           浏览/收藏/加购/下单，**只存在于 2017-11 与 2017-12 两个月**；
           2018-01 起的 169,708 行全部是 Amazon 评价搬运来的「评价」类型。
           因此**加购率/收藏率在数学上只有 2 个可用月度点**，任何"月度趋势"
           都只有两个点 —— 这是数据源的客观边界，不是实现缺陷；
        3. 行为域与订单域是两个**互不相交的用户空间**，严禁跨源求比率：
           原实现用 paid/browse 算转化率得到过 52700% 的失真值。
           故 convertion 类比率仅在同域分母非空时计算，否则返回 null 断线。
        """
        start, end = self._get_date_range(params)
        start_id = self._date_to_id(start)
        end_id = self._date_to_id(end)
        bucket = params.get("bucket", ["month"])[0]
        if bucket not in ("day", "month"):
            bucket = "month"

        conn = sqlite3.connect(DB_PATH)
        try:
            NOTE = ("fact_user_behavior 为两源拼接：淘宝行为（浏览/收藏/加购）仅覆盖 "
                    "2017-11 与 2017-12，2018 年起仅有 Amazon 搬运的「评价」行；"
                    "行为域与订单域用户空间互不相交，禁止跨源求比率，"
                    "分母为空的期返回 null 断线。")

            def _rate(num, den):
                return round(num / den * 100, 2) if den else None

            # ---------- 月度口径：直接读 ADS 预聚合表 ----------
            if bucket == "month":
                ym_start, ym_end = start.replace("-", "")[:6], end.replace("-", "")[:6]
                rows = conn.execute("""
                    SELECT ym, total_users, browse_users, collect_users, cart_users,
                           behavior_pv, order_users, paid_users, order_amount
                    FROM ads_behavior_monthly
                    WHERE ym >= ? AND ym <= ? ORDER BY ym
                """, (ym_start, ym_end)).fetchall()

                # 表缺失时降级为明细直算，保证在跑完 ETL 之前也不会白屏
                if not rows:
                    return self._recommendation_monthly_fallback(
                        conn, ym_start, ym_end, NOTE)

                # 行为域实际有浏览量的月份（用于前端诚实标注稀疏覆盖）
                # 阈值 1000 UV：低于此的月份（如 2017-07 仅 1 UV、2017-10 仅 5 UV）
                # 是 ETL 边界残留的噪声行，若计入会让读者误以为有 5 期数据。
                beh_ym = conn.execute(
                    "SELECT ym, browse_users FROM ads_behavior_monthly "
                    "WHERE browse_users >= ? ORDER BY ym",
                    (MIN_VALID_BROWSE_UV,)).fetchall()
                noise_ym = conn.execute(
                    "SELECT ym, browse_users FROM ads_behavior_monthly "
                    "WHERE browse_users > 0 AND browse_users < ? ORDER BY ym",
                    (MIN_VALID_BROWSE_UV,)).fetchall()

                result = []
                for (ym, total, browse, collect, cart, pv, ou, pu, amt) in rows:
                    # 样本量不足的月份不参与比率计算，返回 null 断线，
                    # 避免画出 browse=1、cart=0 → "加购率 0%" 这种误导性柱子。
                    sample_ok = (browse or 0) >= MIN_VALID_BROWSE_UV
                    result.append({
                        "period": f"{ym[:4]}-{ym[4:6]}",
                        "total_users": total or 0,
                        "browse_users": browse or 0,
                        "collect_users": collect or 0,
                        "cart_users": cart or 0,
                        "behavior_pv": pv or 0,
                        "order_users": ou or 0,
                        "paid_users": pu or 0,
                        "order_amount": round(amt or 0, 2),
                        "ctr": _rate(cart or 0, browse or 0) if sample_ok else None,
                        "collect_rate": _rate(collect or 0, browse or 0) if sample_ok else None,
                        # 以下两个跨源比率**不再提供**：订单域与行为域用户不相交，
                        # paid/browse 在本数据集上没有业务含义（历史值曾达 52700%）。
                        "conversion_rate": None,
                        "order_rate": None,
                    })
                return {
                    "bucket": "month",
                    "source": "ads_behavior_monthly（ADS 预聚合，血缘 fact_user_behavior + fact_orders）",
                    "note": NOTE,
                    "behavior_months": [f"{r[0][:4]}-{r[0][4:6]}" for r in beh_ym],
                    # 有浏览行为但样本量过小、不足以计算比率（浏览 UV < 1000）的月份。
                    # 单独返回而非直接丢弃：这些行是 ETL 边界残留，属已知事实，
                    # 隐匿比展示更危险；但也不应参与"趋势"解读。
                    "noise_months": [{"period": f"{r[0][:4]}-{r[0][4:6]}",
                                      "browse_users": r[1]} for r in noise_ym],
                    "min_valid_browse_uv": MIN_VALID_BROWSE_UV,
                    "cross_source_ratio_disabled": True,
                    "data": result,
                }

            # ---------- 逐日口径：明细直算 ----------
            day_rows = conn.execute("""
                SELECT COUNT(*) FROM dim_date WHERE date_id >= ? AND date_id <= ?
            """, (start_id, end_id)).fetchone()[0]
            if day_rows > 92:
                # 跨期过长时逐日直算会全表扫描（12s+），且绝大多数日期为空，
                # 强行动 month 并如实告知降级原因。
                out = self.api_recommendation_daily(
                    {k: v for k, v in params.items() if k != "bucket"} | {"bucket": ["month"]})
                out["downgraded"] = f"日期跨度 {day_rows} 天 > 92 天，已自动降级为月度口径"
                return out

            behav_map = {r[0]: r for r in conn.execute("""
                SELECT date_id,
                       COUNT(DISTINCT user_id),
                       COUNT(DISTINCT CASE WHEN behavior_type IN ('浏览','pv') THEN user_id END),
                       COUNT(DISTINCT CASE WHEN behavior_type IN ('收藏','fav') THEN user_id END),
                       COUNT(DISTINCT CASE WHEN behavior_type IN ('加购','cart') THEN user_id END)
                FROM fact_user_behavior
                WHERE date_id >= ? AND date_id <= ? GROUP BY date_id
            """, (start_id, end_id)).fetchall()}
            order_map = {r[0]: (r[1], r[2]) for r in conn.execute("""
                SELECT order_date_id, COUNT(DISTINCT user_id),
                       COUNT(DISTINCT CASE WHEN order_status='已完成' THEN user_id END)
                FROM fact_orders
                WHERE order_date_id >= ? AND order_date_id <= ? GROUP BY order_date_id
            """, (start_id, end_id)).fetchall()}
            calendar = conn.execute("""
                SELECT date_id, date_str FROM dim_date
                WHERE date_id >= ? AND date_id <= ? ORDER BY date_id
            """, (start_id, end_id)).fetchall()

            result = []
            for did, dstr in calendar:
                b = behav_map.get(did)
                browse = (b[2] if b else 0) or 0
                collect = (b[3] if b else 0) or 0
                cart = (b[4] if b else 0) or 0
                ou, pu = order_map.get(did, (0, 0))
                result.append({
                    "period": dstr,
                    "total_users": (b[1] if b else 0) or 0,
                    "browse_users": browse,
                    "collect_users": collect,
                    "cart_users": cart,
                    "behavior_pv": 0,
                    "order_users": ou or 0,
                    "paid_users": pu or 0,
                    "order_amount": 0,
                    "ctr": _rate(cart, browse),
                    "collect_rate": _rate(collect, browse),
                    "conversion_rate": None,
                    "order_rate": None,
                })
            return {
                "bucket": "day",
                "source": "fact_user_behavior + fact_orders（明细直算，跨源）",
                "note": NOTE,
                "cross_source_ratio_disabled": True,
                "data": result,
            }
        finally:
            conn.close()

    @staticmethod
    def _recommendation_monthly_fallback(conn, ym_start, ym_end, note):
        """ads_behavior_monthly 尚未构建时的降级：明细直算（慢，但结果等价）。"""
        rows = conn.execute("""
            SELECT substr(CAST(date_id AS TEXT),1,6) AS ym,
                   COUNT(DISTINCT user_id),
                   COUNT(DISTINCT CASE WHEN behavior_type IN ('浏览','pv') THEN user_id END),
                   COUNT(DISTINCT CASE WHEN behavior_type IN ('收藏','fav') THEN user_id END),
                   COUNT(DISTINCT CASE WHEN behavior_type IN ('加购','cart') THEN user_id END)
            FROM fact_user_behavior
            WHERE substr(CAST(date_id AS TEXT),1,6) >= ?
              AND substr(CAST(date_id AS TEXT),1,6) <= ?
            GROUP BY ym ORDER BY ym
        """, (ym_start, ym_end)).fetchall()
        o_rows = conn.execute("""
            SELECT substr(CAST(order_date_id AS TEXT),1,6) AS ym,
                   COUNT(DISTINCT user_id),
                   COUNT(DISTINCT CASE WHEN order_status='已完成' THEN user_id END),
                   ROUND(SUM(actual_amount),2)
            FROM fact_orders
            WHERE substr(CAST(order_date_id AS TEXT),1,6) >= ?
              AND substr(CAST(order_date_id AS TEXT),1,6) <= ?
            GROUP BY ym
        """, (ym_start, ym_end)).fetchall()
        o_map = {r[0]: (r[1], r[2], r[3]) for r in o_rows}

        def _rate(num, den):
            return round(num / den * 100, 2) if den else None

        data = []
        for ym, total, browse, collect, cart in rows:
            ou, pu, amt = o_map.get(ym, (0, 0, 0))
            data.append({
                "period": f"{ym[:4]}-{ym[4:6]}", "total_users": total or 0,
                "browse_users": browse or 0, "collect_users": collect or 0,
                "cart_users": cart or 0, "behavior_pv": 0,
                "order_users": ou or 0, "paid_users": pu or 0,
                "order_amount": round(amt or 0, 2),
                "ctr": _rate(cart or 0, browse or 0),
                "collect_rate": _rate(collect or 0, browse or 0),
                "conversion_rate": None, "order_rate": None,
            })
        return {
            "bucket": "month", "source": "fact_user_behavior（明细直算降级路径，建议先跑 build_ads_behavior_monthly.py）",
            "note": note, "cross_source_ratio_disabled": True, "data": data,
        }

    def api_user_search_terms(self, params):
        """热门关注商品 TOP20

        说明：本研究所用的公开真实数据集（Amazon 评价—购买记录、淘宝 pv/fav/cart/buy）
        均**不含搜索词日志**，无法统计真实搜索词。为避免展示空图，此处改以
        "被购买次数最多的商品"作为**热度代理指标**呈现，字段名沿用 keyword
        以兼容前端，并在 note 中标注口径（论文 §8.2 同口径表述）。

        修复说明（Q6）：该接口此前虽存在，前端从未挂载，导致论文声称的
        "热门关注商品 TOP20"在系统内无承载页面——论文与系统不一致是答辩硬伤。
        本次同时补上筛选期聚合与"去重买家数/销售额"两个佐证字段，使榜单可被
        交叉验证，而非孤立的计数榜。
        """
        start, end = self._get_date_range(params)
        start_id, end_id = self._date_to_id(start), self._date_to_id(end)
        rows = query_json("""
            SELECT dp.product_id,
                   dp.product_name as keyword,
                   dp.main_category,
                   dp.image_url as image_url,
                   COUNT(*) as search_cnt,
                   COUNT(DISTINCT foi.user_id) as user_cnt,
                   SUM(foi.quantity) as qty_cnt,
                   ROUND(SUM(foi.item_amount), 0) as revenue
            FROM fact_order_items foi
            JOIN dim_product dp ON foi.product_id = dp.product_id
            WHERE foi.order_date_id >= ? AND foi.order_date_id <= ?
            GROUP BY foi.product_id
            ORDER BY search_cnt DESC, qty_cnt DESC LIMIT 20
        """, (start_id, end_id))
        return {"rows": rows,
                "source": "fact_order_items（Amazon 域真实购买记录）",
                "metric": "购买次数（热度代理指标）",
                "note": ("公开数据集不含搜索词日志，无法统计真实搜索量；"
                         "此处以「被购买次数」作为关注热度代理指标，口径与论文 §8.2 一致")}

    # —— 跨域用户空间：行为域（淘宝）与交易/评价域（Amazon）用户 ID 完全不相交 ——
    #    实测：行为域 24,673 人、评价域 48,473 人、交集 0 人。
    #    因此"去重用户数"决不能跨 behavior_type 直接 COUNT(DISTINCT user_id)，
    #    否则得到 73,146 = 24,673 + 48,473 的拼接假数（审计 Q7）。
    BEHAVIOR_DOMAIN_TYPES = ("浏览", "收藏", "加购", "下单")
    _BEH_DOMAIN_SQL = ",".join("'%s'" % t for t in BEHAVIOR_DOMAIN_TYPES)

    def api_user_summary(self, params):
        """用户行为页 KPI 汇总（**分域口径**）

        演进说明：
        1) v1.x 原实现按日累加 distinct 用户导致重复计数（"活跃用户 263,955"），
           "新用户"为虚构值（总用户×15%）；
        2) v1.7 改为时间窗内真实聚合，但仍未区分来源，得到 73,146 —— 这是把
           淘宝行为域 24,673 人与 Amazon 评价域 48,473 人相加的结果，而二者
           **交集为 0**，属于标准的"跨源用户空间混算"错误（与 C7 同源）；
        3) v1.8.1 起主口径收敛到行为域，评价域作为独立指标并列返回，
           前端同时展示两者以避免信息丢失。
        """
        start, end = self._get_date_range(params)
        start_id = self._date_to_id(start)
        end_id = self._date_to_id(end)

        tbl, pv = behavior_source()
        rows = query_json(f"""
            SELECT COUNT(DISTINCT CASE WHEN behavior_type IN ({self._BEH_DOMAIN_SQL})
                                       THEN user_id END) as active_users,
                   COUNT(DISTINCT CASE WHEN behavior_type = '评价'
                                       THEN user_id END) as review_users,
                   COUNT(DISTINCT user_id) as naive_distinct_users,
                   COALESCE(SUM(CASE WHEN behavior_type='浏览' THEN {pv} ELSE 0 END), 0) as pv,
                   COALESCE(SUM(CASE WHEN behavior_type='收藏' THEN {pv} ELSE 0 END), 0) as fav_cnt,
                   COUNT(DISTINCT CASE WHEN behavior_type='浏览' THEN user_id END) as browse_users,
                   COUNT(DISTINCT CASE WHEN behavior_type='加购' THEN user_id END) as cart_users,
                   COUNT(DISTINCT CASE WHEN behavior_type='下单' THEN user_id END) as order_users
            FROM {tbl}
            WHERE date_id >= ? AND date_id <= ?
        """, (start_id, end_id))
        r = rows[0] if rows else {"active_users": 0, "pv": 0, "fav_cnt": 0,
                                  "browse_users": 0, "cart_users": 0,
                                  "order_users": 0, "review_users": 0,
                                  "naive_distinct_users": 0}
        r["domain"] = {
            "behavior_label": "行为域活跃用户（淘宝 UserBehavior）",
            "review_label": "评价域活跃用户（Amazon 评价）",
            "intersection": 0,
            "note": ("两个数据域的 user_id 空间互不相交（交集 0），"
                     "直接跨源 COUNT(DISTINCT user_id) 会把 24,673 与 48,473 "
                     "相加成 73,146 的假值，故分域呈现"),
        }
        return r

    # ==================== 宠物热销榜（黏土风新页面） ====================
    def api_pet_top(self, params):
        """热销榜 Top 商品：支持类目筛选、时间范围与四种排序（销量/销售额/评分/价格）"""
        start, end = self._get_date_range(params)
        category = clean_category(params.get("category", [None])[0])
        limit = clean_limit(params.get("limit", [12])[0], 12)
        sort = params.get("sort", ["sales"])[0]
        order_map = {
            "sales": "sales_quantity DESC",
            "revenue": "revenue DESC",
            "rating": "avg_rating DESC, sales_quantity DESC",
            "price": "price DESC, sales_quantity DESC",
        }
        order_by = order_map.get(sort, "sales_quantity DESC")
        cat_filter = f"AND dp.main_category = '{category}'" if category else ""
        return query_json(f"""
            SELECT dp.product_id, dp.product_name, dp.main_category, dp.sub_category,
                   dp.brand, dp.price, dp.avg_rating, dp.review_count,
                   COALESCE(dp.image_url, '') as image_url,
                   SUM(foi.quantity) as sales_quantity,
                   ROUND(SUM(foi.item_amount), 0) as revenue,
                   COUNT(DISTINCT foi.user_id) as buyer_count
            FROM fact_order_items foi
            JOIN dim_product dp ON foi.product_id = dp.product_id
            JOIN dim_date dd ON foi.order_date_id = dd.date_id
            WHERE dd.date_str >= '{start}' AND dd.date_str <= '{end}' {cat_filter}
            GROUP BY dp.product_id
            ORDER BY {order_by}
            LIMIT {limit}
        """)

    def api_pet_category_share(self, params):
        """各品类销售占比（可按类目单选高亮，前端始终展示全量结构）"""
        start, end = self._get_date_range(params)
        return query_json(f"""
            SELECT dp.main_category,
                   COUNT(DISTINCT dp.product_id) as product_count,
                   SUM(foi.quantity) as sales_quantity,
                   ROUND(SUM(foi.item_amount), 0) as revenue
            FROM fact_order_items foi
            JOIN dim_product dp ON foi.product_id = dp.product_id
            JOIN dim_date dd ON foi.order_date_id = dd.date_id
            WHERE dd.date_str >= '{start}' AND dd.date_str <= '{end}'
            GROUP BY dp.main_category
            ORDER BY revenue DESC
        """)

    def api_pet_product_trend(self, params):
        """单个商品的按日销售趋势（详情弹窗图表用）"""
        pid = params.get("product_id", [None])[0]
        start, end = self._get_date_range(params)
        if not pid:
            return []
        return query_json(f"""
            SELECT dd.date_str,
                   SUM(foi.quantity) as qty,
                   ROUND(SUM(foi.item_amount), 0) as revenue
            FROM fact_order_items foi
            JOIN dim_date dd ON foi.order_date_id = dd.date_id
            WHERE foi.product_id = ? AND dd.date_str >= ? AND dd.date_str <= ?
            GROUP BY dd.date_str ORDER BY dd.date_id
        """, (pid, start, end))

    def api_export_data(self, params):
        """通用数据导出（JSON 视图，供前端直接消费）

        CSV 流式下载走 do_GET 里的 _export_csv 分支：
            /api/export/data?api=pet_top&format=csv
        不再把 CSV 内容包进 JSON（原实现导致大结果集内存翻倍，
        且中文逗号替换会破坏数值字段）。
        """
        endpoint = params.get("api", [None])[0]
        if not endpoint or not endpoint.startswith("/api/"):
            return {"error": "Missing or invalid api parameter"}
        return self.handle_api(endpoint, params)

    def _export_csv(self, params):
        """CSV 流式导出：逐行 write，避免全量驻留内存。

        浏览器/curl 直接下载：
            /api/export/data?api=pet_top&format=csv&limit=100
        """
        endpoint = params.get("api", [None])[0]
        if not endpoint or not endpoint.startswith("/api/"):
            self.send_json({"error": "Missing or invalid api parameter"})
            return
        data = self.handle_api(endpoint, params)
        if isinstance(data, dict) and "error" in data:
            self.send_json(data)
            return

        rows = data if isinstance(data, list) else []
        if not rows:
            for v in (data.values() if isinstance(data, dict) else []):
                if isinstance(v, list) and v:
                    rows = v
                    break
        if not rows:
            self.send_json({"error": "No tabular data to export"})
            return

        self.send_response(200)
        self.send_header("Content-Type", "text/csv; charset=utf-8")
        self.send_header("Content-Disposition",
                         'attachment; filename="export_%s.csv"'
                         % datetime.now().strftime("%Y%m%d_%H%M%S"))
        self.end_headers()
        # RFC 4180：字段含逗号/引号/换行时加双引号转义；数值字段保持原样
        def _cell(v):
            s = "" if v is None else str(v)
            if any(ch in s for ch in (",", '"', "\n", "\r")):
                return '"' + s.replace('"', '""') + '"'
            return s
        keys = list(rows[0].keys())
        self.wfile.write((",".join(keys) + "\r\n").encode("utf-8"))
        for row in rows:
            self.wfile.write((",".join(_cell(row.get(k)) for k in keys)
                              + "\r\n").encode("utf-8"))

    # ==================== 购物篮洞察（v2 时序购物篮页面） ====================
    def api_temporal_rules(self, params):
        """时序关联规则：买 A → T 天 → B（支持最低支持度/TopN 筛选与图数据视图）"""
        data = load_temporal() or {}
        rules = list(data.get("rules", []))
        min_users = int(params.get("min_users", [0])[0])
        limit = clean_limit(params.get("limit", [100])[0], 100)
        view = params.get("view", ["flat"])[0]
        rules = [r for r in rules if r.get("users_ab", 0) >= min_users]
        if view == "graph":
            top_n = int(params.get("top_n", [40])[0])
            top = rules[:top_n]
            nodes, seen = [], set()
            for r in top:
                for c in (r["antecedent"], r["consequent"]):
                    if c not in seen:
                        seen.add(c)
                        nodes.append({"name": c})
            return {
                "nodes": nodes,
                "links": [{"source": r["antecedent"], "target": r["consequent"],
                           "value": r["confidence"], "users": r["users_ab"],
                           "gap_median": r["gap_median"], "lift": r["lift"]} for r in top],
                "categories": sorted(seen),
            }
        return rules[:limit]

    def api_repurchase_cycle(self, params):
        """二级类目消耗周期画像（周期间隔分布）"""
        data = load_temporal() or {}
        cycles = list(data.get("repurchase_cycles", []))
        min_interval = int(params.get("min_interval", [0])[0])
        cycles = [c for c in cycles if c.get("interval_count", 0) >= min_interval]
        return cycles

    def api_replenishment_candidates(self, params):
        """补货候选清单（进入补货窗口的用户-类目对象）"""
        data = load_temporal() or {}
        cands = list(data.get("replenishment_candidates", []))
        category = clean_category(params.get("category", [None])[0])
        if category:
            cands = [c for c in cands if c.get("sub_category") == category or
                     c.get("main_category") == category]
        limit = clean_limit(params.get("limit", [100])[0], 100)
        # 扁平化suggested_products，便于前端表格与CSV导出
        flat = params.get("flat", ["1"])[0] == "1"
        out = []
        for c in cands[:limit]:
            row = {k: c[k] for k in c if k != "suggested_products"}
            if flat:
                ps = c.get("suggested_products", [])
                row["suggested_product_1"] = ps[0]["product_name"] if len(ps) > 0 else ""
                row["suggested_product_2"] = ps[1]["product_name"] if len(ps) > 1 else ""
                row["suggested_product_3"] = ps[2]["product_name"] if len(ps) > 2 else ""
            else:
                row["suggested_products"] = c.get("suggested_products", [])
            out.append(row)
        return out

    # ==================== 宠物生命周期（v2 双维画像 · 派生推断） ====================
    def api_pet_lifecycle(self, params):
        """宠物生命阶段推断结果（派生数据，全程标注可追溯与未知）
        view=summary 汇总 / dist 阶段分布 / switch 切换明细 / profile 宠物维度画像
        """
        data = load_lifecycle()
        if not data:
            return {"error": "pet_lifecycle.json 不存在，请先运行 analytics/pet_lifecycle.py"}
        view = params.get("view", ["summary"])[0]
        if view == "dist":
            return {
                "user_stage_distribution": data.get("user_stage_distribution", {}),
                "confidence_distribution": data.get("confidence_distribution", {}),
                "product_tag_stats": data.get("product_tag_stats", {}),
            }
        if view == "switch":
            limit = clean_limit(params.get("limit", [50])[0], 50)
            return {
                "stats": data.get("switch_stats", {}),
                "cases": data.get("switch_cases", [])[:limit],
            }
        if view == "profile":
            return data.get("pet_profiles", [])
        return {
            "data_stats": data.get("data_stats", {}),
            "switch_stats": data.get("switch_stats", {}),
            "user_stage_distribution": data.get("user_stage_distribution", {}),
            "confidence_distribution": data.get("confidence_distribution", {}),
            "pet_profiles": data.get("pet_profiles", []),
            "method": data.get("method", {}),
            "limitations": data.get("limitations", []),
            "generated_at": data.get("generated_at", ""),
        }

    # ==================== 在线模拟 A/B 实验平台 ====================
    def api_ab_presets(self, params):
        """A/B 模拟：预设策略组、可调参数与响应模型说明"""
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "ab_sim", os.path.join(ANALYTICS_DIR, "ab_simulation.py"))
        ab = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(ab)
        except Exception as e:
            return {"error": f"加载模拟器失败: {e}"}
        last = None
        try:
            with open(AB_SIM_FILE, "r", encoding="utf-8") as f:
                last = json.load(f)
        except Exception:
            pass
        return {
            "preset_strategies": ab.PRESET_STRATEGIES,
            "param_schema": [
                {"key": "w_pop", "label": "类目热度权重", "min": 0, "max": 1, "step": 0.05, "default": 0.3},
                {"key": "w_affinity", "label": "用户类目亲和权重", "min": 0, "max": 1, "step": 0.05, "default": 0.7},
                {"key": "w_co", "label": "时序补货信号权重", "min": 0, "max": 1, "step": 0.05, "default": 0.0},
                {"key": "epsilon", "label": "随机探索率 ε", "min": 0, "max": 0.3, "step": 0.01, "default": 0.0},
            ],
            "experiment_params": ab.DEFAULT_PARAMS,
            "last_run": {"generated_at": last.get("generated_at"), "seed": last.get("seed"),
                         "n_users": last.get("n_users")} if last else None,
            "method_note": (
                "模拟环境双层口径：真实锚点层（real_hit_rate，测试期真实购买，无假设）；"
                "模拟响应层（CTR/CVR/GMV，显式响应模型，simulated=true 标记）。"
                "同 (seed, 策略组) 配置重跑结果逐位一致。"),
        }

    def api_ab_simulate(self, params):
        """A/B 模拟：运行实验（同步返回，约 7~20s），支持自定义策略参数与种子"""
        try:
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                "ab_sim", os.path.join(ANALYTICS_DIR, "ab_simulation.py"))
            ab = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(ab)
            seed = int(params.get("seed", [42])[0])
            sample_ratio = float(params.get("sample_ratio", [1.0])[0])
            strategies = ab.PRESET_STRATEGIES
            if params.get("strategies"):
                custom = json.loads(params.get("strategies")[0])
                if isinstance(custom, list) and custom:
                    strategies = []
                    for i, s in enumerate(custom):
                        strategies.append({
                            "key": str(s.get("key", chr(65 + i))),
                            "name": str(s.get("name", f"策略 {s.get('key', i)}")),
                            "params": {**ab.PRESET_STRATEGIES[0]["params"], **s.get("params", {})},
                        })
            out = ab.ABSimulator().run(strategies=strategies, seed=seed,
                                       sample_ratio=sample_ratio, save=True)
            return out
        except Exception as e:
            return {"error": f"模拟运行失败: {e}"}

    def serve_html(self, filename):
        path = os.path.join(DASHBOARD_DIR, "templates", filename)
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
            content = content.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", len(content))
            self.end_headers()
            self.wfile.write(content)
        else:
            self.send_error(404, "File not found")
    
    def serve_static(self, filename):
        # Handle ECharts CDN redirect
        # 前端以 /static/js/... 或 /js/... 形式引用，统一归一化到 dashboard/static/ 下，
        # 避免出现 "static/static/..." 双重前缀导致 404
        filename = filename.lstrip("/")
        if filename.startswith("static/"):
            filename = filename[len("static/"):]
        path = os.path.join(DASHBOARD_DIR, "static", filename)
        if os.path.exists(path):
            ext = filename.rsplit(".", 1)[-1]
            mime = {"css": "text/css", "js": "application/javascript", "png": "image/png",
                    "jpg": "image/jpeg", "jpeg": "image/jpeg", "gif": "image/gif",
                    "svg": "image/svg+xml", "webp": "image/webp", "ico": "image/x-icon"
                    }.get(ext, "application/octet-stream")
            with open(path, "rb") as f:
                content = f.read()
            self.send_response(200)
            # 二进制资源不附加 charset，避免部分浏览器按文本解码导致图片失效
            if ext in ("css", "js", "svg"):
                self.send_header("Content-Type", f"{mime}; charset=utf-8")
            else:
                self.send_header("Content-Type", mime)
            self.send_header("Content-Length", len(content))
            self.end_headers()
            self.wfile.write(content)
        else:
            self.send_error(404)
    
    def send_json(self, data):
        content = json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", len(content))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(content)
    
    def log_message(self, format, *args):
        """结构化访问日志：记录 方法/路径/状态码/耗时。

        原实现为 `pass`（静默），导致线上排障无据可查。这里改为写入
        dashboard/server.log，并同时输出到 stdout；日志量按 DAYS 滚动不必，
        单文件已足够演示与答辩场景。慢请求（>1s）追加 SLOW 标记，便于定位
        需要加索引或缓存的接口。
        """
        try:
            t0 = getattr(self, "t0", None) or time.time()
            elapsed_ms = (time.time() - t0) * 1000
            client = self.client_address[0] if self.client_address else "-"
            # BaseHTTPRequestHandler 的 args 形如 ('GET /api/overview HTTP/1.1',)
            req = format % args if args else str(format)
            line = (f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {client} "
                    f"{req} -> {elapsed_ms:.0f}ms"
                    f"{'  [SLOW]' if elapsed_ms > 1000 else ''}")
            print(line, flush=True)
            try:
                # 独立文件，避免与启动时的 stdout 重定向目标（> server.log）互相追加
                with open(os.path.join(DASHBOARD_DIR, "access.log"), "a", encoding="utf-8") as f:
                    f.write(line + "\n")
            except OSError:
                pass  # 日志写入失败不应影响请求本身
        except Exception:
            pass  # 兜底：任何日志异常都不得中断响应

def main():
    # 部署到公网/容器时通过环境变量覆盖监听地址与端口，
    # 避免在服务器里直接改源码（同一份代码可同时用于本地演示与云上部署）。
    host = os.environ.get("DASHBOARD_HOST", "0.0.0.0")
    port = int(os.environ.get("DASHBOARD_PORT", "9888"))
    
    print("=" * 60)
    print("🐾 宠物用品电商 BI 数据可视化平台")
    print(f"   版本: v1.8.1")
    print(f"   数仓: {DB_PATH}")
    print(f"   监听: http://{host}:{port}")
    print(f"   按 Ctrl+C 停止服务器")
    print("=" * 60)
    
    # 多线程而非单线程：sqlite 连接为每请求独立创建（query_json / conn.close），
    # 线程间不共享；单线程下 recommendation_daily 月度聚合（数秒级）会阻塞
    # 其余图表请求，演示时表现为"整页卡住"。
    from http.server import ThreadingHTTPServer
    server = ThreadingHTTPServer((host, port), APIHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n服务器已停止")
        server.server_close()

if __name__ == "__main__":
    main()
