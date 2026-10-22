"""
宠物用品电商数据仓库 - ETL增量加载流程
支持全量加载和增量加载两种模式
"""
import os
import sys
import json
import sqlite3
from datetime import datetime, timedelta
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data", "raw")
WAREHOUSE_DIR = os.path.join(BASE_DIR, "warehouse")
DB_PATH = os.path.join(WAREHOUSE_DIR, "pet_supplies_dw.db")


class DataWarehouseETL:
    """数据仓库ETL处理器"""
    
    def __init__(self, db_path=DB_PATH, mode="full"):
        self.db_path = db_path
        self.mode = mode  # full 或 incremental
        self.conn = None
        self.etl_batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
        self.etl_stats = {}
        
    def connect(self):
        """连接数据库"""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        return self.conn
    
    def close(self):
        if self.conn:
            self.conn.commit()
            self.conn.close()
    
    def init_schema(self):
        """初始化数据仓库表结构"""
        print("[ETL] 初始化数据仓库表结构...")
        
        # 全量模式下先删除所有表
        if self.mode == "full":
            self.conn.execute("PRAGMA foreign_keys=OFF")
            tables = ["ads_category_sales", "ads_daily_behavior", "ads_daily_sales",
                       "fact_reviews", "fact_user_behavior", "fact_order_items",
                       "fact_orders", "dim_product", "dim_user", "dim_date"]
            for tbl in tables:
                self.conn.execute(f"DROP TABLE IF EXISTS {tbl}")
            self.conn.commit()
            self.conn.execute("PRAGMA foreign_keys=ON")
        
        schema_path = os.path.join(WAREHOUSE_DIR, "sql", "01_schema_ddl.sql")
        with open(schema_path, "r", encoding="utf-8") as f:
            ddl = f.read()
        self.conn.executescript(ddl)
        self.conn.commit()
        print("[ETL] 表结构初始化完成")
    
    def load_dim_date(self):
        """加载时间维度表"""
        print("[ETL] 加载时间维度表...")
        cursor = self.conn.cursor()
        
        # 时间范围由真实数据集构建阶段写入的 data_period.json 决定（模拟数据则回退默认值）
        period_file = os.path.join(DATA_DIR, "data_period.json")
        start_s, end_s = "2025-07-01", "2026-06-30"
        if os.path.exists(period_file):
            try:
                with open(period_file, "r", encoding="utf-8") as pf:
                    _p = json.load(pf)
                start_s = _p.get("start_date", start_s)
                end_s = _p.get("end_date", end_s)
            except Exception:
                pass
        start = datetime.strptime(start_s, "%Y-%m-%d") - timedelta(days=30)
        end = datetime.strptime(end_s, "%Y-%m-%d") + timedelta(days=30)
        print(f"[ETL] 时间维度范围(数据驱动): {start.date()} ~ {end.date()}")
        
        day_names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
        
        # 简化的节假日数据
        holidays = {
            "2025-01-01": "元旦", "2025-01-28": "春节", "2025-01-29": "春节",
            "2025-01-30": "春节", "2025-01-31": "春节", "2025-02-01": "春节",
            "2025-02-02": "春节", "2025-02-03": "春节", "2025-04-04": "清明",
            "2025-05-01": "劳动节", "2025-05-02": "劳动节", "2025-05-03": "劳动节",
            "2025-05-04": "劳动节", "2025-05-05": "劳动节", "2025-06-22": "端午",
            "2025-09-15": "中秋", "2025-10-01": "国庆", "2025-10-02": "国庆",
            "2025-10-03": "国庆", "2025-10-04": "国庆", "2025-10-05": "国庆",
            "2025-10-06": "国庆", "2025-10-07": "国庆", "2025-10-08": "国庆",
            "2026-01-01": "元旦", "2026-02-17": "春节",
        }
        
        current = start
        count = 0
        while current <= end:
            date_id = int(current.strftime("%Y%m%d"))
            date_str = current.strftime("%Y-%m-%d")
            is_holiday = 1 if date_str in holidays else 0
            holiday_name = holidays.get(date_str, None)
            
            cursor.execute(
                """INSERT OR IGNORE INTO dim_date VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    date_id, date_str,
                    current.year, (current.month-1)//3+1, current.month,
                    f"{current.month}月", current.isocalendar()[1],
                    current.day, current.weekday(),
                    day_names[current.weekday()],
                    1 if current.weekday() >= 5 else 0,
                    is_holiday, holiday_name,
                    current.strftime("%Y-%m"),
                    f"{current.year}Q{(current.month-1)//3+1}"
                )
            )
            current += timedelta(days=1)
            count += 1
        
        self.conn.commit()
        print(f"[ETL] 时间维度加载完成, 共 {count} 条")
    
    def load_dim_user(self):
        """加载用户维度表"""
        print("[ETL] 加载用户维度表...")
        
        users_file = os.path.join(DATA_DIR, "users.jsonl")
        if not os.path.exists(users_file):
            print(f"[ETL] 用户数据文件不存在: {users_file}")
            return
        
        cursor = self.conn.cursor()
        count = 0
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        with open(users_file, "r", encoding="utf-8") as f:
            for line in f:
                user = json.loads(line)
                cursor.execute(
                    """INSERT OR REPLACE INTO dim_user 
                       (user_id, nickname, age, gender, city, city_tier, pet_type,
                        pet_age_group, income_level, register_date, register_source,
                        is_member, activity_level, consume_level, etl_update_time)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        user["user_id"], user["nickname"], user["age"],
                        user["gender"], user["city"], user["city_tier"],
                        user["pet_type"], user["pet_age_group"], user["income_level"],
                        user["register_date"], user["register_source"],
                        1 if user["is_member"] else 0,
                        user["activity_level"], user["consume_level"], now
                    )
                )
                count += 1
        
        self.conn.commit()
        print(f"[ETL] 用户维度加载完成, 共 {count} 条")
        self.etl_stats["dim_user"] = count
    
    def load_dim_product(self):
        """加载商品维度表"""
        print("[ETL] 加载商品维度表...")
        
        products_file = os.path.join(DATA_DIR, "products.jsonl")
        if not os.path.exists(products_file):
            return
        
        cursor = self.conn.cursor()
        count = 0
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        with open(products_file, "r", encoding="utf-8") as f:
            for line in f:
                prod = json.loads(line)
                cursor.execute(
                    """INSERT OR REPLACE INTO dim_product
                       (product_id, product_name, main_category, sub_category,
                        leaf_category, brand, price, quality_score, stock,
                        list_date, shelf_status, description, image_count,
                        has_video, etl_update_time)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        prod["product_id"], prod["product_name"],
                        prod["main_category"], prod["sub_category"],
                        prod["leaf_category"], prod["brand"],
                        prod["price"], prod["quality_score"],
                        prod["stock"], prod["list_date"],
                        prod["shelf_status"], prod["description"],
                        prod.get("image_count", 0),
                        1 if prod.get("has_video") else 0, now
                    )
                )
                count += 1
        
        self.conn.commit()
        print(f"[ETL] 商品维度加载完成, 共 {count} 条")
        self.etl_stats["dim_product"] = count
    
    def load_fact_orders(self):
        """加载订单事实表"""
        print("[ETL] 加载订单事实表...")
        
        orders_file = os.path.join(DATA_DIR, "orders.jsonl")
        order_items_file = os.path.join(DATA_DIR, "order_items.jsonl")
        
        if not os.path.exists(orders_file):
            return
        
        cursor = self.conn.cursor()
        
        # 先加载订单明细，统计每个订单的商品数和库存单位数
        item_stats = {}
        if os.path.exists(order_items_file):
            with open(order_items_file, "r", encoding="utf-8") as f:
                for line in f:
                    item = json.loads(line)
                    oid = item["order_id"]
                    if oid not in item_stats:
                        item_stats[oid] = {"item_count": 0, "total_quantity": 0}
                    item_stats[oid]["item_count"] += 1
                    item_stats[oid]["total_quantity"] += item["quantity"]
        
        # 加载订单
        count = 0
        with open(orders_file, "r", encoding="utf-8") as f:
            for line in f:
                order = json.loads(line)
                oid = order["order_id"]
                stats = item_stats.get(oid, {"item_count": 1, "total_quantity": 1})
                date_id = int(order["order_date"].replace("-", ""))
                
                # 提取小时
                try:
                    order_hour = int(order.get("order_time", "00:00:00").split(":")[0])
                except:
                    order_hour = 0
                
                cursor.execute(
                    """INSERT OR REPLACE INTO fact_orders
                       (order_id, order_date_id, user_id, total_amount,
                        discount_amount, actual_amount, item_count, total_quantity,
                        is_promo, promo_name, pay_method, order_status,
                        order_source, shipping_city, order_hour)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        oid, date_id, order["user_id"],
                        order["total_amount"], order["discount_amount"],
                        order["actual_amount"],
                        stats["item_count"], stats["total_quantity"],
                        order.get("is_promo", 0), order.get("promo_name", ""),
                        order.get("pay_method", ""), order.get("order_status", ""),
                        order.get("order_source", ""), order.get("shipping_city", ""),
                        order_hour
                    )
                )
                count += 1
        
        self.conn.commit()
        print(f"[ETL] 订单事实表加载完成, 共 {count} 条")
        self.etl_stats["fact_orders"] = count
    
    def load_fact_order_items(self):
        """加载订单明细事实表"""
        print("[ETL] 加载订单明细事实表...")
        
        items_file = os.path.join(DATA_DIR, "order_items.jsonl")
        if not os.path.exists(items_file):
            return
        
        # 构建订单到日期和用户的映射
        cursor = self.conn.cursor()
        cursor.execute("SELECT order_id, order_date_id, user_id FROM fact_orders")
        order_map = {row[0]: (row[1], row[2]) for row in cursor.fetchall()}
        
        count = 0
        with open(items_file, "r", encoding="utf-8") as f:
            for line in f:
                item = json.loads(line)
                oid = item["order_id"]
                date_id, user_id = order_map.get(oid, (0, ""))
                
                cursor.execute(
                    """INSERT OR REPLACE INTO fact_order_items
                       (item_id, order_id, order_date_id, user_id, product_id,
                        quantity, unit_price, item_amount, is_gift)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                    (
                        item["item_id"], oid, date_id, user_id,
                        item["product_id"], item["quantity"],
                        item["unit_price"], item["item_amount"],
                        item.get("is_gift", 0)
                    )
                )
                count += 1
        
        self.conn.commit()
        print(f"[ETL] 订单明细加载完成, 共 {count} 条")
        self.etl_stats["fact_order_items"] = count
    
    def load_fact_user_behavior(self):
        """加载用户行为事实表"""
        print("[ETL] 加载用户行为事实表（分批次加载）...")
        
        behavior_file = os.path.join(DATA_DIR, "behavior_logs.jsonl")
        if not os.path.exists(behavior_file):
            return
        
        cursor = self.conn.cursor()
        count = 0
        batch = []
        batch_size = 5000
        
        with open(behavior_file, "r", encoding="utf-8") as f:
            for line in f:
                log = json.loads(line)
                date_str = log["behavior_time"][:10]
                date_id = int(date_str.replace("-", ""))
                
                try:
                    behavior_hour = int(log["behavior_time"][11:13])
                except:
                    behavior_hour = 0
                
                batch.append((
                    log["log_id"], date_id, log["user_id"], log["product_id"],
                    log["behavior_type"], behavior_hour, log["device_type"],
                    log.get("duration_seconds", 0), log.get("session_id", ""),
                    log.get("ip_region", "")
                ))
                
                if len(batch) >= batch_size:
                    cursor.executemany(
                        """INSERT OR REPLACE INTO fact_user_behavior
                           (log_id, date_id, user_id, product_id, behavior_type,
                            behavior_hour, device_type, duration_seconds, session_id, ip_region)
                           VALUES (?,?,?,?,?,?,?,?,?,?)""",
                        batch
                    )
                    count += len(batch)
                    batch = []
        
        # 处理剩余批次
        if batch:
            cursor.executemany(
                """INSERT OR REPLACE INTO fact_user_behavior
                   (log_id, date_id, user_id, product_id, behavior_type,
                    behavior_hour, device_type, duration_seconds, session_id, ip_region)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                batch
            )
            count += len(batch)
        
        self.conn.commit()
        print(f"[ETL] 行为事实表加载完成, 共 {count} 条")
        self.etl_stats["fact_user_behavior"] = count
    
    def load_fact_reviews(self):
        """加载评价事实表"""
        print("[ETL] 加载评价事实表...")
        
        reviews_file = os.path.join(DATA_DIR, "reviews.jsonl")
        if not os.path.exists(reviews_file):
            return
        
        cursor = self.conn.cursor()
        count = 0
        
        with open(reviews_file, "r", encoding="utf-8") as f:
            for line in f:
                review = json.loads(line)
                date_id = int(review["review_date"].replace("-", ""))
                
                cursor.execute(
                    """INSERT OR REPLACE INTO fact_reviews
                       (review_id, date_id, user_id, product_id, order_id,
                        rating, review_content, has_image, has_video,
                        is_anonymous, helpful_count)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        review["review_id"], date_id,
                        review["user_id"], review["product_id"],
                        review.get("order_id", ""), review["rating"],
                        review["review_content"],
                        review.get("has_image", 0), review.get("has_video", 0),
                        review.get("is_anonymous", 0),
                        review.get("helpful_count", 0)
                    )
                )
                count += 1
        
        self.conn.commit()
        print(f"[ETL] 评价事实表加载完成, 共 {count} 条")
        self.etl_stats["fact_reviews"] = count
    
    def compute_aggregations(self):
        """计算聚合层数据"""
        print("[ETL] 计算聚合层数据...")
        cursor = self.conn.cursor()
        
        # 1. 每日销售汇总
        print("  [ADS] 计算每日销售汇总...")
        cursor.execute("""
            INSERT INTO ads_daily_sales
            SELECT 
                fo.order_date_id as date_id,
                COUNT(DISTINCT fo.order_id),
                COUNT(DISTINCT fo.user_id),
                SUM(fo.total_amount),
                SUM(fo.actual_amount),
                SUM(fo.discount_amount),
                CASE WHEN COUNT(fo.order_id) > 0 
                     THEN SUM(fo.actual_amount) / COUNT(fo.order_id) ELSE 0 END,
                0,  -- new_user_count 后续更新
                SUM(fo.is_promo)
            FROM fact_orders fo
            WHERE fo.order_status NOT IN ('已退款', '已取消')
            GROUP BY fo.order_date_id
        """)
        
        # 2. 每日行为汇总
        print("  [ADS] 计算每日行为汇总...")
        cursor.execute("""
            INSERT INTO ads_daily_behavior
            SELECT 
                date_id,
                COUNT(CASE WHEN behavior_type='浏览' THEN 1 END) as pv,
                COUNT(DISTINCT CASE WHEN behavior_type='浏览' THEN user_id END) as uv,
                COUNT(CASE WHEN behavior_type='收藏' THEN 1 END),
                COUNT(CASE WHEN behavior_type='加购' THEN 1 END),
                COUNT(CASE WHEN behavior_type='搜索' THEN 1 END),
                COUNT(CASE WHEN behavior_type='分享' THEN 1 END),
                AVG(CASE WHEN behavior_type='浏览' THEN duration_seconds END),
                -- 跳出: 同一天只浏览了一个商品
                SUM(CASE WHEN behavior_type='浏览' THEN 
                    (SELECT CASE WHEN COUNT(DISTINCT product_id)=1 THEN 1 ELSE 0 END 
                     FROM fact_user_behavior f2 
                     WHERE f2.user_id=fact_user_behavior.user_id 
                     AND f2.date_id=fact_user_behavior.date_id
                     AND f2.behavior_type='浏览')
                ELSE 0 END),
                0.0
            FROM fact_user_behavior
            GROUP BY date_id
        """)
        
        # 3. 类目销售汇总
        # 注意：原实现 LEFT JOIN fact_reviews 按 product_id 关联，会因每个商品有多条
        # 评价而产生笛卡尔积，把 item_amount 放大约 66 倍（SUM 从 2,174 万虚增到 14.4 亿）。
        # 修复：评分直接用 dim_product.avg_rating，不再 JOIN fact_reviews。
        print("  [ADS] 计算类目销售汇总...")
        cursor.execute("""
            INSERT INTO ads_category_sales
            SELECT 
                foi.order_date_id,
                dp.main_category,
                dp.sub_category,
                COUNT(DISTINCT foi.order_id),
                SUM(foi.quantity),
                SUM(foi.item_amount),
                COUNT(DISTINCT foi.user_id),
                AVG(dp.avg_rating)
            FROM fact_order_items foi
            JOIN dim_product dp ON foi.product_id = dp.product_id
            GROUP BY foi.order_date_id, dp.main_category, dp.sub_category
        """)
        
        # 4. 更新用户RFM值
        print("  [ADS] 计算用户RFM值...")
        # 计算最大日期
        cursor.execute("SELECT MAX(order_date_id) FROM fact_orders")
        max_date_id = cursor.fetchone()[0]
        if max_date_id:
            max_date = datetime.strptime(str(max_date_id), "%Y%m%d")
            
            # 更新用户维度表
            cursor.execute("""
                UPDATE dim_user SET
                    first_order_date = (SELECT MIN(order_date_id) FROM fact_orders WHERE user_id=dim_user.user_id),
                    last_order_date = (SELECT MAX(order_date_id) FROM fact_orders WHERE user_id=dim_user.user_id),
                    lifetime_orders = (SELECT COUNT(*) FROM fact_orders WHERE user_id=dim_user.user_id AND order_status='已完成'),
                    lifetime_amount = (SELECT COALESCE(SUM(actual_amount),0) FROM fact_orders WHERE user_id=dim_user.user_id AND order_status='已完成')
            """)
            
            # 计算RFM
            cursor.execute("""
                UPDATE dim_user SET
                    avg_order_amount = CASE WHEN lifetime_orders > 0 
                        THEN lifetime_amount / lifetime_orders ELSE 0 END,
                    rfm_r = CASE 
                        WHEN last_order_date IS NULL THEN 1
                        WHEN last_order_date >= ? THEN 5
                        WHEN last_order_date >= ? THEN 4
                        WHEN last_order_date >= ? THEN 3
                        WHEN last_order_date >= ? THEN 2
                        ELSE 1 END,
                    rfm_f = CASE 
                        WHEN lifetime_orders >= 20 THEN 5
                        WHEN lifetime_orders >= 10 THEN 4
                        WHEN lifetime_orders >= 5 THEN 3
                        WHEN lifetime_orders >= 2 THEN 2
                        ELSE 1 END,
                    rfm_m = CASE 
                        WHEN lifetime_amount >= 5000 THEN 5
                        WHEN lifetime_amount >= 2000 THEN 4
                        WHEN lifetime_amount >= 500 THEN 3
                        WHEN lifetime_amount >= 100 THEN 2
                        ELSE 1 END,
                    etl_update_time = ?
            """, (
                int((max_date - timedelta(days=30)).strftime("%Y%m%d")),
                int((max_date - timedelta(days=60)).strftime("%Y%m%d")),
                int((max_date - timedelta(days=90)).strftime("%Y%m%d")),
                int((max_date - timedelta(days=180)).strftime("%Y%m%d")),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ))
            
            # 更新RFM总分和分层
            cursor.execute("""
                UPDATE dim_user SET
                    rfm_score = rfm_r + rfm_f + rfm_m,
                    rfm_segment = CASE 
                        WHEN rfm_r + rfm_f + rfm_m >= 13 THEN '高价值客户'
                        WHEN rfm_r + rfm_f + rfm_m >= 10 THEN '重要发展客户'
                        WHEN rfm_r + rfm_f + rfm_m >= 7 THEN '一般客户'
                        WHEN rfm_r + rfm_f + rfm_m >= 5 THEN '低价值客户'
                        ELSE '流失客户' END
            """)
        
        # 5. 更新商品维度表
        print("  [ADS] 更新商品销售统计...")
        cursor.execute("""
            UPDATE dim_product SET
                total_sales = COALESCE((SELECT SUM(quantity) FROM fact_order_items WHERE product_id=dim_product.product_id), 0),
                total_revenue = COALESCE((SELECT SUM(item_amount) FROM fact_order_items WHERE product_id=dim_product.product_id), 0),
                avg_rating = COALESCE((SELECT AVG(rating) FROM fact_reviews WHERE product_id=dim_product.product_id), 0),
                review_count = COALESCE((SELECT COUNT(*) FROM fact_reviews WHERE product_id=dim_product.product_id), 0)
        """)
        
        self.conn.commit()
        print("[ETL] 聚合计算完成")
    
    def run(self):
        """执行完整的ETL流程"""
        print("=" * 60)
        print("宠物用品电商数据仓库 ETL 流程")
        print(f"批次: {self.etl_batch_id}")
        print(f"模式: {self.mode}")
        print("=" * 60)
        
        try:
            self.connect()
            
            # Step 1: 初始化表结构
            self.init_schema()
            
            # Step 2: 加载维度表
            self.load_dim_date()
            self.load_dim_user()
            self.load_dim_product()
            
            # Step 3: 加载事实表
            self.load_fact_orders()
            self.load_fact_order_items()
            self.load_fact_user_behavior()
            self.load_fact_reviews()
            
            # Step 4: 计算聚合层
            self.compute_aggregations()
            
            # 输出统计
            print("\n" + "=" * 60)
            print("ETL 完成统计:")
            for table, count in self.etl_stats.items():
                print(f"  {table}: {count}")
            
            # 验证数据
            cursor = self.conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM fact_orders")
            print(f"  总订单数: {cursor.fetchone()[0]}")
            cursor.execute("SELECT COUNT(*) FROM fact_user_behavior")
            print(f"  总行为记录: {cursor.fetchone()[0]}")
            cursor.execute("SELECT COUNT(*) FROM dim_user WHERE lifetime_orders > 0")
            print(f"  有购买行为的用户: {cursor.fetchone()[0]}")
            
        finally:
            self.close()


if __name__ == "__main__":
    etl = DataWarehouseETL(mode="full")
    etl.run()
