# -*- coding: utf-8 -*-
"""
DWS 汇总服务层构建脚本（v1.5.0 P2-1）
====================================
定位：在 DWD（fact_*）与 ADS（ads_*）之间显式落地"主题宽表"层，
为主题口径提供单一事实来源，保证表/字段命名规范、口径定义与依赖血缘可追溯。

血缘（DWD → DWS → 下游）：
    fact_user_behavior ──→ dws_user_behavior_1d ──→ ads_daily_behavior / 留存 / 漏斗 / 热力图
    fact_order_items ⋈ fact_orders ──→ dws_user_purchase_1d ──→ RFM / 推荐评分矩阵
                                   └──→ dws_product_sales_1d ──→ ads_daily_sales / ads_category_sales / 热销榜
    fact_order_items ⋈ fact_orders ⋈ dim_product ──→ dws_user_category_cum ──→ Co-Score / 生命周期证据 / 补货候选

口径：
    有效订单 = order_status NOT IN ('已退款','已取消')（与 ads_daily_sales 一致）
    金额 = item_amount（实付）；类目冗余自 dim_product 构建日快照

运行：python etl/build_dws.py（全量幂等重建，ETL 之后执行；结束时输出与 DWD 的对账校验）
"""
import os
import sys
import sqlite3
import time

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "warehouse", "pet_supplies_dw.db")
DDL_PATH = os.path.join(BASE_DIR, "warehouse", "sql", "02_dws_schema.sql")

VALID_ORDER = "fo.order_status NOT IN ('已退款', '已取消')"


def build():
    t0 = time.time()
    if not os.path.exists(DB_PATH):
        print(f"[DWS] 数据仓库不存在：{DB_PATH}，请先运行 etl/run_etl.py")
        sys.exit(1)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 1. 执行 DDL（幂等重建）
    print("[DWS] 执行 DDL：warehouse/sql/02_dws_schema.sql")
    with open(DDL_PATH, encoding="utf-8") as f:
        cursor.executescript(f.read())
    conn.commit()

    # 2. 用户×日 行为宽表
    print("[DWS] 构建 dws_user_behavior_1d ...")
    cursor.execute("""
        INSERT INTO dws_user_behavior_1d
        SELECT date_id, user_id,
               SUM(CASE WHEN behavior_type='浏览' THEN 1 ELSE 0 END),
               SUM(CASE WHEN behavior_type='收藏' THEN 1 ELSE 0 END),
               SUM(CASE WHEN behavior_type='加购' THEN 1 ELSE 0 END),
               SUM(CASE WHEN behavior_type='搜索' THEN 1 ELSE 0 END),
               SUM(CASE WHEN behavior_type='分享' THEN 1 ELSE 0 END),
               COUNT(*),
               COALESCE(AVG(CASE WHEN behavior_type='浏览' THEN duration_seconds END), 0)
        FROM fact_user_behavior
        GROUP BY date_id, user_id
    """)

    # 3. 用户×日 购买宽表（有效订单口径）
    print("[DWS] 构建 dws_user_purchase_1d ...")
    cursor.execute(f"""
        INSERT INTO dws_user_purchase_1d
        SELECT foi.order_date_id, foi.user_id,
               COUNT(DISTINCT foi.order_id),
               COUNT(DISTINCT foi.product_id),
               SUM(foi.quantity),
               SUM(foi.item_amount)
        FROM fact_order_items foi
        JOIN fact_orders fo ON fo.order_id = foi.order_id
        WHERE {VALID_ORDER}
        GROUP BY foi.order_date_id, foi.user_id
    """)

    # 4. 商品×日 销售宽表（类目冗余快照）
    print("[DWS] 构建 dws_product_sales_1d ...")
    cursor.execute(f"""
        INSERT INTO dws_product_sales_1d
        SELECT foi.order_date_id, foi.product_id,
               COALESCE(dp.main_category, '未知'), COALESCE(dp.sub_category, '未知'),
               COUNT(DISTINCT foi.order_id),
               COUNT(DISTINCT foi.user_id),
               SUM(foi.quantity),
               SUM(foi.item_amount)
        FROM fact_order_items foi
        JOIN fact_orders fo ON fo.order_id = foi.order_id
        LEFT JOIN dim_product dp ON dp.product_id = foi.product_id
        WHERE {VALID_ORDER}
        GROUP BY foi.order_date_id, foi.product_id,
                 COALESCE(dp.main_category, '未知'), COALESCE(dp.sub_category, '未知')
    """)

    # 5. 用户×类目 累计亲和快照（类目维度需明细粒度，直接源于 DWD，类目冗余自 dim_product 快照）
    print("[DWS] 构建 dws_user_category_cum ...")
    cursor.execute(f"""
        INSERT INTO dws_user_category_cum
        SELECT foi.user_id,
               COALESCE(dp.sub_category, '未知'),
               COUNT(DISTINCT foi.order_id),
               SUM(foi.quantity),
               SUM(foi.item_amount),
               MIN(foi.order_date_id),
               MAX(foi.order_date_id)
        FROM fact_order_items foi
        JOIN fact_orders fo ON fo.order_id = foi.order_id
        LEFT JOIN dim_product dp ON dp.product_id = foi.product_id
        WHERE {VALID_ORDER}
        GROUP BY foi.user_id, COALESCE(dp.sub_category, '未知')
    """)
    conn.commit()

    # 6. 对账校验：DWS 汇总必须与 DWD 明细完全一致（有效订单口径）
    print("[DWS] 对账校验 ...")
    checks = [
        ("dws_user_behavior_1d.pv = DWD 浏览数",
         "SELECT (SELECT SUM(pv) FROM dws_user_behavior_1d)",
         "SELECT COUNT(*) FROM fact_user_behavior WHERE behavior_type='浏览'"),
        ("dws_user_purchase_1d.amount = DWD 有效订单销售额",
         f"SELECT (SELECT ROUND(SUM(amount),2) FROM dws_user_purchase_1d)",
         f"SELECT ROUND(SUM(foi.item_amount),2) FROM fact_order_items foi JOIN fact_orders fo ON fo.order_id=foi.order_id WHERE {VALID_ORDER}"),
        ("dws_product_sales_1d.amount = DWD 有效订单销售额",
         "SELECT (SELECT ROUND(SUM(amount),2) FROM dws_product_sales_1d)",
         f"SELECT ROUND(SUM(foi.item_amount),2) FROM fact_order_items foi JOIN fact_orders fo ON fo.order_id=foi.order_id WHERE {VALID_ORDER}"),
        ("dws_user_category_cum.amount = DWD 有效订单销售额",
         "SELECT (SELECT ROUND(SUM(amount),2) FROM dws_user_category_cum)",
         f"SELECT ROUND(SUM(foi.item_amount),2) FROM fact_order_items foi JOIN fact_orders fo ON fo.order_id=foi.order_id WHERE {VALID_ORDER}"),
    ]
    ok = True
    for name, sql_dws, sql_dwd in checks:
        v1 = cursor.execute(sql_dws).fetchone()[0]
        v2 = cursor.execute(sql_dwd).fetchone()[0]
        match = (v1 == v2)
        ok = ok and match
        print(f"    {'PASS' if match else 'FAIL'}  {name}: DWS={v1} vs DWD={v2}")
    for t in ["dws_user_behavior_1d", "dws_user_purchase_1d", "dws_product_sales_1d", "dws_user_category_cum"]:
        n = cursor.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"    {t}: {n} 行")
    conn.commit()
    conn.close()
    print(f"[DWS] 构建完成，耗时 {time.time()-t0:.1f}s，对账{'全部通过' if ok else '存在失败项！'}")
    return ok


if __name__ == "__main__":
    build()
