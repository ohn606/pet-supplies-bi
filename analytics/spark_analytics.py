"""
宠物用品电商 - PySpark 大数据分析模块
基于数据仓库进行用户核心指标计算与购物篮关联分析

技术栈: PySpark SQL / MLlib / pandas (本地模拟)
"""
import os
import sys
import json
import sqlite3
from datetime import datetime, timedelta
from itertools import combinations
from collections import Counter, defaultdict
import math

import numpy as np
import pandas as pd

# 尝试导入PySpark
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col, count, countDistinct, sum as spark_sum, avg, max as spark_max,
        min as spark_min, when, lit, round as spark_round, row_number,
        collect_list, struct, array_contains, size, explode, split, 
        datediff, to_date, date_format, month, year, quarter, weekofyear,
        rank, dense_rank, percent_rank, ntile, lag, lead, 
        stddev, variance, corr, first, last
    )
    from pyspark.sql.window import Window
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, FloatType, DoubleType, 
        DateType, TimestampType
    )
    from pyspark.ml.fpm import FPGrowth
    from pyspark.ml.stat import Correlation
    from pyspark.ml.feature import VectorAssembler
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    print("[WARN] PySpark未安装, 使用pandas本地模拟模式")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WAREHOUSE_DIR = os.path.join(BASE_DIR, "warehouse")
ANALYTICS_DIR = os.path.join(BASE_DIR, "analytics")

# ============================================================
# 1. 用户核心指标分析 (RFM / 留存 / 转化 / 复购)
# ============================================================

class UserMetricsAnalyzer:
    """用户核心指标分析器"""
    
    def __init__(self, db_path=None):
        if db_path is None:
            db_path = os.path.join(WAREHOUSE_DIR, "pet_supplies_dw.db")
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        
    def __del__(self):
        if self.conn:
            self.conn.close()
    
    def query_to_df(self, sql):
        """执行SQL并返回DataFrame"""
        return pd.read_sql_query(sql, self.conn)
    
    def analyze_rfm(self):
        """
        RFM用户价值分析
        """
        print("\n" + "=" * 60)
        print("1. RFM用户价值分析")
        print("=" * 60)
        
        # 查询用户RFM明细
        df = self.query_to_df("""
            SELECT user_id, rfm_r, rfm_f, rfm_m, rfm_score, rfm_segment,
                   lifetime_orders, lifetime_amount, avg_order_amount,
                   activity_level, consume_level, pet_type, city_tier
            FROM dim_user
            WHERE lifetime_orders > 0
        """)
        
        # RFM分层统计
        print("\n--- RFM用户分层分布 ---")
        segment_stats = df.groupby("rfm_segment").agg(
            用户数=("user_id", "count"),
            平均消费金额=("lifetime_amount", "mean"),
            平均订单数=("lifetime_orders", "mean"),
            平均客单价=("avg_order_amount", "mean"),
        ).round(1).sort_values("用户数", ascending=False)
        print(segment_stats)
        
        # 宠物类型与RFM交叉分析
        print("\n--- 宠物类型 × RFM分层交叉表 ---")
        cross = pd.crosstab(df["pet_type"], df["rfm_segment"], margins=True)
        print(cross)
        
        return segment_stats, df
    
    def analyze_retention(self):
        """
        用户留存率分析 (Cohort Analysis)
        """
        print("\n" + "=" * 60)
        print("2. 用户留存率分析 (队列分析)")
        print("=" * 60)
        
        # 获取每个用户的首次购买月份和后续购买月份
        # 注：order_date_id 与 first_order_date 均为 YYYYMMDD 整数/字符串，
        #     需先按 substr 转为 YYYY-MM-DD 才能使用 strftime 取月份
        df = self.query_to_df("""
            SELECT
                fo.user_id,
                fo.order_date_id,
                du.first_order_date,
                substr(CAST(fo.order_date_id AS TEXT), 1, 4) || '-' ||
                    substr(CAST(fo.order_date_id AS TEXT), 5, 2) as order_month,
                substr(du.first_order_date, 1, 4) || '-' ||
                    substr(du.first_order_date, 5, 2) as cohort_month
            FROM fact_orders fo
            JOIN dim_user du ON fo.user_id = du.user_id
            WHERE fo.order_status = '已完成'
        """)
        
        if len(df) == 0:
            print("无数据")
            return
        
        # 计算月份差异
        df["order_month"] = pd.to_datetime(df["order_month"])
        df["cohort_month"] = pd.to_datetime(df["cohort_month"])
        df["period"] = (df["order_month"].dt.year - df["cohort_month"].dt.year) * 12 + \
                       (df["order_month"].dt.month - df["cohort_month"].dt.month)
        
        # 队列分析
        cohort_counts = df.groupby("cohort_month")["user_id"].nunique()
        
        # 构建留存矩阵
        cohort_data = df.groupby(["cohort_month", "period"])["user_id"].nunique().unstack()
        cohort_pct = cohort_data.div(cohort_counts, axis=0) * 100
        
        print("\n--- 月度留存率矩阵 (% of cohort) ---")
        # 只显示前6个月和前几个队列
        display = cohort_pct.iloc[:6, :7].round(1)
        print(display.to_string())
        
        return cohort_pct
    
    def analyze_conversion(self):
        """
        转化漏斗分析: 浏览 → 加购 → 下单 → 支付
        """
        print("\n" + "=" * 60)
        print("3. 用户转化漏斗分析")
        print("=" * 60)
        
        # 重要（多源融合）：行为事实表与订单事实表来自不同数据源（用户实体空间不同），
        # 漏斗各环节必须同源统计，否则会出现"下单用户数 > 浏览用户数"的失真结论。
        # 因此全部环节取自行为事实表（淘宝真实 pv/fav/cart/buy 序列）。
        df_behavior = self.query_to_df("""
            SELECT behavior_type, COUNT(DISTINCT user_id) as user_count
            FROM fact_user_behavior
            GROUP BY behavior_type
        """)

        def _cnt(bt):
            m = df_behavior[df_behavior["behavior_type"] == bt]["user_count"]
            return int(m.values[0]) if len(m) else 0

        browse = _cnt("浏览")
        collect = _cnt("收藏")
        cart = _cnt("加购")
        order = _cnt("下单")

        stages = ["浏览", "收藏", "加购", "下单"]
        counts = [browse, collect, cart, order]
        
        print("\n--- 转化漏斗 ---")
        for i, (stage, count) in enumerate(zip(stages, counts)):
            rate = f"{count/browse*100:.1f}%" if browse > 0 else "N/A"
            prev_rate = f"{count/counts[i-1]*100:.1f}%" if i > 0 and counts[i-1] > 0 else ""
            print(f"  {stage:8s}: {count:8d} 用户  (总体转化率: {rate}, 环比转化率: {prev_rate})")
        
        return {"stages": stages, "counts": counts}
    
    def analyze_repurchase(self):
        """
        复购率分析
        """
        print("\n" + "=" * 60)
        print("4. 用户复购率分析")
        print("=" * 60)
        
        # 按宠物类型分析复购
        df = self.query_to_df("""
            SELECT 
                du.pet_type,
                du.consume_level,
                COUNT(DISTINCT du.user_id) as user_count,
                SUM(du.lifetime_orders) as total_orders,
                CAST(SUM(du.lifetime_orders) AS REAL) / COUNT(DISTINCT du.user_id) as avg_orders_per_user,
                SUM(CASE WHEN du.lifetime_orders >= 2 THEN 1 ELSE 0 END) as repurchase_users,
                CAST(SUM(CASE WHEN du.lifetime_orders >= 2 THEN 1 ELSE 0 END) AS REAL) / COUNT(DISTINCT du.user_id) * 100 as repurchase_rate,
                AVG(du.lifetime_amount) as avg_lifetime_value
            FROM dim_user du
            WHERE du.lifetime_orders > 0
            GROUP BY du.pet_type, du.consume_level
            ORDER BY du.pet_type, du.consume_level
        """)
        
        print("\n--- 宠物类型 × 消费层级 复购分析 ---")
        print(df.round(1).to_string())
        
        # 类目级别复购
        df_cat = self.query_to_df("""
            SELECT 
                dp.main_category,
                dp.sub_category,
                COUNT(DISTINCT foi.user_id) as buyer_count,
                SUM(foi.quantity) as total_quantity,
                CAST(SUM(foi.quantity) AS REAL) / COUNT(DISTINCT foi.user_id) as avg_per_user,
                COUNT(DISTINCT CASE WHEN user_rebuy.cnt >= 2 THEN foi.user_id END) as rebuy_users
            FROM fact_order_items foi
            JOIN dim_product dp ON foi.product_id = dp.product_id
            LEFT JOIN (
                SELECT user_id, product_id, COUNT(DISTINCT order_id) as cnt
                FROM fact_order_items
                GROUP BY user_id, product_id
            ) user_rebuy ON foi.user_id = user_rebuy.user_id AND foi.product_id = user_rebuy.product_id
            GROUP BY dp.main_category, dp.sub_category
            ORDER BY buyer_count DESC
            LIMIT 15
        """)
        df_cat["repurchase_rate"] = (df_cat["rebuy_users"] / df_cat["buyer_count"] * 100).round(1)
        print("\n--- 类目复购率 TOP15 ---")
        print(df_cat[["main_category", "sub_category", "buyer_count", "avg_per_user", "repurchase_rate"]].to_string())
        
        return df


# ============================================================
# 2. 购物篮关联分析 (Market Basket Analysis)
# ============================================================

class MarketBasketAnalyzer:
    """购物篮关联分析 - Apriori / FP-Growth"""
    
    def __init__(self, db_path=None):
        if db_path is None:
            db_path = os.path.join(WAREHOUSE_DIR, "pet_supplies_dw.db")
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        
    def __del__(self):
        if self.conn:
            self.conn.close()
    
    def get_transactions(self, min_items=2):
        """获取购物篮事务数据"""
        df = pd.read_sql_query("""
            SELECT order_id, product_id
            FROM fact_order_items
            ORDER BY order_id
        """, self.conn)
        
        transactions = df.groupby("order_id")["product_id"].apply(list).tolist()
        # 过滤单项购物篮
        transactions = [t for t in transactions if len(t) >= min_items]
        
        print(f"购物篮总数: {len(transactions)}, 平均商品数: {np.mean([len(t) for t in transactions]):.1f}")
        return transactions
    
    def apriori(self, transactions, min_support=0.01, min_confidence=0.3, max_k=3):
        """
        Apriori算法实现
        """
        print("\n" + "=" * 60)
        print("购物篮关联分析 - Apriori算法")
        print("=" * 60)
        
        n_transactions = len(transactions)
        
        # Step 1: 生成频繁1项集
        item_counts = Counter()
        for t in transactions:
            item_counts.update(set(t))
        
        freq_items = {}
        k = 1
        candidates = {frozenset([item]): count for item, count in item_counts.items() 
                      if count / n_transactions >= min_support}
        freq_items[1] = candidates.copy()
        
        print(f"\n  L1: {len(candidates)} 个频繁单项, min_support={min_support}")
        for itemset, count in sorted(candidates.items(), key=lambda x: -x[1])[:10]:
            print(f"    {list(itemset)}: support={count/n_transactions:.4f}")
        
        # Step 2: 生成更高阶频繁项集
        for k in range(2, max_k + 1):
            # 从L_{k-1}生成C_k候选集
            prev_items = list(freq_items[k-1].keys())
            new_candidates = set()
            
            for i in range(len(prev_items)):
                for j in range(i + 1, len(prev_items)):
                    union = prev_items[i] | prev_items[j]
                    if len(union) == k:
                        # 剪枝：检查所有k-1子集都是频繁的
                        all_freq = all(
                            frozenset(union - {item}) in freq_items[k-1]
                            for item in union
                        )
                        if all_freq:
                            new_candidates.add(union)
            
            # 扫描事务数据库
            candidate_counts = Counter()
            for t in transactions:
                t_set = frozenset(t)
                for cand in new_candidates:
                    if cand.issubset(t_set):
                        candidate_counts[cand] += 1
            
            # 过滤
            freq_items[k] = {cand: count for cand, count in candidate_counts.items()
                             if count / n_transactions >= min_support}
            
            print(f"\n  L{k}: {len(freq_items[k])} 个频繁{k}项集")
            for itemset, count in sorted(freq_items[k].items(), key=lambda x: -x[1])[:10]:
                print(f"    {list(itemset)}: support={count/n_transactions:.4f}")
        
        # Step 3: 生成关联规则
        print(f"\n--- 关联规则 (min_confidence={min_confidence}) ---")
        rules = []
        
        for k in range(2, max_k + 1):
            for itemset, itemset_count in freq_items.get(k, {}).items():
                support = itemset_count / n_transactions
                
                # 生成规则：antecedent → consequent
                for i in range(1, len(itemset)):
                    for antecedent in combinations(itemset, i):
                        antecedent = frozenset(antecedent)
                        consequent = itemset - antecedent
                        
                        antecedent_support = freq_items[len(antecedent)].get(antecedent, 0) / n_transactions
                        if antecedent_support <= 0:
                            continue
                        
                        confidence = support / antecedent_support
                        
                        if confidence >= min_confidence:
                            # 计算提升度
                            consequent_count = freq_items[len(consequent)].get(consequent, 0)
                            consequent_support = consequent_count / n_transactions if consequent_count > 0 else 0
                            lift = confidence / consequent_support if consequent_support > 0 else 0
                            
                            rules.append({
                                "antecedent": list(antecedent),
                                "consequent": list(consequent),
                                "support": round(support, 4),
                                "confidence": round(confidence, 4),
                                "lift": round(lift, 2)
                            })
        
        # 按提升度排序输出
        rules.sort(key=lambda r: -r["lift"])
        for i, r in enumerate(rules[:20]):
            print(f"  {i+1:2d}. {r['antecedent']} → {r['consequent']} "
                  f"(support={r['support']:.4f}, confidence={r['confidence']:.4f}, lift={r['lift']:.2f})")
        
        return rules
    
    def category_cross_sell(self):
        """类目级别的交叉销售分析"""
        print("\n" + "=" * 60)
        print("类目交叉销售分析")
        print("=" * 60)
        
        # 获取每个订单中购买的类目
        df = pd.read_sql_query("""
            SELECT foi.order_id, dp.main_category, dp.sub_category
            FROM fact_order_items foi
            JOIN dim_product dp ON foi.product_id = dp.product_id
            ORDER BY foi.order_id
        """, self.conn)
        
        # 聚合每个订单的类目
        order_cats = df.groupby("order_id")["sub_category"].apply(lambda x: list(set(x))).tolist()
        order_cats = [cats for cats in order_cats if len(cats) >= 2]
        
        # 计算类目共现矩阵
        cat_pairs = Counter()
        cat_counts = Counter()
        
        for cats in order_cats:
            for cat in set(cats):
                cat_counts[cat] += 1
            for c1, c2 in combinations(set(cats), 2):
                cat_pairs[(min(c1, c2), max(c1, c2))] += 1
        
        n_orders = len(order_cats)
        
        # 计算关联度
        print("\n--- 类目共现强度 TOP20 (按提升度排序) ---")
        results = []
        for (c1, c2), co_count in cat_pairs.items():
            supp_ab = co_count / n_orders
            supp_a = cat_counts[c1] / n_orders
            supp_b = cat_counts[c2] / n_orders
            conf_a_to_b = supp_ab / supp_a if supp_a > 0 else 0
            lift = supp_ab / (supp_a * supp_b) if supp_a * supp_b > 0 else 0
            
            results.append({
                "category_a": c1, "category_b": c2,
                "co_occurrence": co_count,
                "confidence_a2b": round(conf_a_to_b, 3),
                "lift": round(lift, 2)
            })
        
        results.sort(key=lambda x: -x["lift"])
        for i, r in enumerate(results[:20]):
            print(f"  {i+1:2d}. {r['category_a']} + {r['category_b']}: "
                  f"共现{r['co_occurrence']}次, 置信度={r['confidence_a2b']:.3f}, 提升度={r['lift']:.2f}")
        
        return results
    
    def time_period_analysis(self):
        """不同时段的购物篮特征分析"""
        print("\n" + "=" * 60)
        print("时段购物篮特征分析")
        print("=" * 60)
        
        df = pd.read_sql_query("""
            SELECT 
                fo.order_id,
                CASE 
                    WHEN fo.order_hour BETWEEN 0 AND 5 THEN '深夜(0-6)'
                    WHEN fo.order_hour BETWEEN 6 AND 11 THEN '上午(6-12)'
                    WHEN fo.order_hour BETWEEN 12 AND 17 THEN '下午(12-18)'
                    ELSE '晚上(18-24)'
                END as time_period,
                fo.total_amount,
                fo.item_count,
                fo.total_quantity,
                fo.actual_amount,
                fo.is_promo,
                dp.main_category
            FROM fact_orders fo
            JOIN fact_order_items foi ON fo.order_id = foi.order_id
            JOIN dim_product dp ON foi.product_id = dp.product_id
        """, self.conn)
        
        period_stats = df.groupby("time_period").agg(
            订单数=("order_id", "nunique"),
            平均客单价=("actual_amount", "mean"),
            平均商品数=("item_count", "mean"),
            促销订单占比=("is_promo", "mean"),
        ).round(1)
        
        print("\n--- 不同时段订单特征 ---")
        print(period_stats.to_string())
        
        return period_stats


# ============================================================
# 3. 商品分析
# ============================================================

class ProductAnalyzer:
    """商品分析"""
    
    def __init__(self, db_path=None):
        if db_path is None:
            db_path = os.path.join(WAREHOUSE_DIR, "pet_supplies_dw.db")
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        
    def __del__(self):
        if self.conn:
            self.conn.close()
    
    def hot_products(self, top_n=20):
        """热门商品排行"""
        print("\n" + "=" * 60)
        print("热门商品分析")
        print("=" * 60)
        
        df = self.query("""
            SELECT 
                dp.product_id,
                dp.product_name,
                dp.main_category,
                dp.sub_category,
                dp.price,
                dp.total_sales,
                dp.total_revenue,
                dp.avg_rating,
                dp.review_count,
                CAST(dp.total_sales AS REAL) / 
                    (SELECT SUM(total_sales) FROM dim_product WHERE total_sales > 0) * 100 as sales_share
            FROM dim_product dp
            WHERE dp.total_sales > 0
            ORDER BY dp.total_sales DESC
            LIMIT ?
        """, [top_n])
        
        print(f"\n--- 销量TOP{top_n}商品 ---")
        for i, row in df.iterrows():
            print(f"  {i+1:2d}. {row['product_name'][:30]:30s} | "
                  f"{row['main_category'][:6]:6s} | "
                  f"¥{row['price']:8.1f} | "
                  f"销量:{row['total_sales']:5d} | "
                  f"评分:{row['avg_rating']:.1f}")
        
        return df
    
    def category_performance(self):
        """类目绩效分析"""
        print("\n--- 类目销售绩效 ---")
        
        df = self.query("""
            SELECT 
                main_category,
                sub_category,
                COUNT(DISTINCT product_id) as product_count,
                SUM(total_sales) as total_sales,
                SUM(total_revenue) as total_revenue,
                AVG(price) as avg_price,
                AVG(avg_rating) as avg_rating,
                AVG(review_count) as avg_review_count
            FROM dim_product
            GROUP BY main_category, sub_category
            ORDER BY total_revenue DESC
        """)
        
        print(df.round(1).to_string())
        return df
    
    def query(self, sql, params=None):
        if params:
            return pd.read_sql_query(sql, self.conn, params=params)
        return pd.read_sql_query(sql, self.conn)


# ============================================================
# 主程序
# ============================================================

def main():
    print("=" * 60)
    print("宠物用品电商 - 大数据分析系统")
    print(f"运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    db_path = os.path.join(WAREHOUSE_DIR, "pet_supplies_dw.db")
    
    # 1. 用户指标分析
    user_analyzer = UserMetricsAnalyzer(db_path)
    user_analyzer.analyze_rfm()
    user_analyzer.analyze_retention()
    user_analyzer.analyze_conversion()
    user_analyzer.analyze_repurchase()
    
    # 2. 购物篮分析
    basket_analyzer = MarketBasketAnalyzer(db_path)
    transactions = basket_analyzer.get_transactions()
    rules = basket_analyzer.apriori(transactions, min_support=0.005, min_confidence=0.2)
    basket_analyzer.category_cross_sell()
    basket_analyzer.time_period_analysis()
    
    # 3. 商品分析
    product_analyzer = ProductAnalyzer(db_path)
    product_analyzer.hot_products(20)
    product_analyzer.category_performance()
    
    # 保存分析结果
    results = {
        "analysis_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "association_rules": rules,
    }
    
    output_path = os.path.join(ANALYTICS_DIR, "analysis_results.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n分析结果已保存至: {output_path}")

if __name__ == "__main__":
    main()
