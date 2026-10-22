# -*- coding: utf-8 -*-
"""
ADS 层月度行为/转化主题聚合表构建（v1.7.0）
============================================
定位：把 recommendation_daily 月度口径从"请求时聚合 264 万行明细"改为
      "请求时读取 15 行预聚合"，单次耗时 12s → 毫秒级。

血缘（DWD → ADS）：
    fact_user_behavior ──┐
                         ├──→ ads_behavior_monthly ──→ 推荐效果追踪面板
    fact_orders ─────────┘

口径：
    ym           = substr(date_id, 1, 6)
    各类 UV      = COUNT(DISTINCT user_id)
    有效订单     = order_status NOT IN ('已退款','已取消')（与 ads_daily_sales 一致）

⚠ 跨源：行为域与订单域用户空间不相交、时间窗几乎不重叠，本表**不**做交叉去重，
  也不产出跨源转化率；比率口径由下游按同域分母计算。

运行：python etl/build_ads_behavior_monthly.py（全量幂等重建；结尾输出对账校验）
"""
import os
import sys
import time
import sqlite3
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "warehouse", "pet_supplies_dw.db")
DDL_PATH = os.path.join(BASE_DIR, "warehouse", "sql", "04_ads_behavior_monthly.sql")

VALID_ORDER = "order_status NOT IN ('已退款', '已取消')"


def build(conn):
    cur = conn.cursor()
    t0 = time.time()

    # 1) 建表（幂等）
    with open(DDL_PATH, "r", encoding="utf-8") as f:
        cur.executescript(f.read())

    # 2) 清空重建（幂等：全量重跑结果一致）
    cur.execute("DELETE FROM ads_behavior_monthly")
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 3) 行为域按月聚合（单次扫描，GROUP BY 覆盖全部年月）
    behavior_sql = f"""
        INSERT INTO ads_behavior_monthly
            (ym, total_users, browse_users, collect_users, cart_users,
             behavior_pv, order_users, paid_users, order_amount, etl_update_time)
        SELECT substr(date_id, 1, 6),
               COUNT(DISTINCT user_id),
               COUNT(DISTINCT CASE WHEN behavior_type='浏览' THEN user_id END),
               COUNT(DISTINCT CASE WHEN behavior_type='收藏' THEN user_id END),
               COUNT(DISTINCT CASE WHEN behavior_type='加购' THEN user_id END),
               COUNT(*),
               0, 0, 0.0, '{ts}'
        FROM fact_user_behavior
        GROUP BY substr(date_id, 1, 6)
    """
    print("[1/3] 聚合 fact_user_behavior ...")
    t1 = time.time()
    cur.execute(behavior_sql)
    print(f"      耗时 {time.time() - t1:.2f}s")

    # 4) 订单域按月写回（单独更新，避免与行为域 JOIN 造成笛卡尔积）
    order_sql = f"""
        SELECT substr(order_date_id, 1, 6) AS ym,
               COUNT(DISTINCT user_id),
               COUNT(DISTINCT CASE WHEN order_status='已完成' THEN user_id END),
               ROUND(SUM(CASE WHEN {VALID_ORDER} THEN actual_amount ELSE 0 END), 2)
        FROM fact_orders
        GROUP BY substr(order_date_id, 1, 6)
    """
    print("[2/3] 聚合 fact_orders ...")
    t1 = time.time()
    orders = cur.execute(order_sql).fetchall()
    print(f"      耗时 {time.time() - t1:.2f}s")

    cur.executemany("""
        INSERT INTO ads_behavior_monthly
            (ym, total_users, browse_users, collect_users, cart_users,
             behavior_pv, order_users, paid_users, order_amount, etl_update_time)
        VALUES (?, 0, 0, 0, 0, 0, ?, ?, ?, ?)
        ON CONFLICT(ym) DO UPDATE SET
            order_users      = excluded.order_users,
            paid_users       = excluded.paid_users,
            order_amount     = excluded.order_amount,
            etl_update_time  = excluded.etl_update_time
    """, [(ym, ou, pu, amt, ts) for ym, ou, pu, amt in orders])

    conn.commit()

    # 5) 对账：与明细表直接聚合的结果逐项比对
    print("[3/3] 对账校验 ...")
    checks = []

    def same(actual, expected, name):
        ok = actual == expected
        checks.append((name, ok, actual, expected))

    same(cur.execute("SELECT COUNT(*) FROM ads_behavior_monthly").fetchone()[0],
         cur.execute("SELECT COUNT(DISTINCT substr(date_id,1,6)) FROM fact_user_behavior").fetchone()[0],
         "年月期数")
    same(cur.execute("SELECT SUM(total_users) FROM ads_behavior_monthly").fetchone()[0],
         cur.execute("SELECT SUM(u) FROM (SELECT COUNT(DISTINCT user_id) u "
                     "FROM fact_user_behavior GROUP BY substr(date_id,1,6))").fetchone()[0],
         "分月活跃UV合计（去重后按人，月内不可跨月累加，仅校验月内口径一致）")
    same(cur.execute("SELECT IFNULL(SUM(order_amount),0) FROM ads_behavior_monthly").fetchone()[0],
         cur.execute(f"SELECT ROUND(SUM(actual_amount),2) FROM fact_orders WHERE {VALID_ORDER}"
                     ).fetchone()[0] or 0,
         "成交额合计")

    print()
    print("=" * 58)
    all_ok = True
    for name, ok, a, e in checks:
        all_ok &= ok
        print(f"  {'PASS' if ok else 'FAIL'}  {name:26s} 表={a}  明细={e}")
    print("=" * 58)
    print(f"总耗时 {time.time() - t0:.2f}s ｜ 结论：{'全部通过' if all_ok else '存在不一致'}")
    return all_ok


def main():
    if not os.path.exists(DB_PATH):
        print(f"[错误] 数据仓库不存在：{DB_PATH}\n请先执行 python etl/run_etl.py")
        return 1
    conn = sqlite3.connect(DB_PATH)
    try:
        ok = build(conn)
        rows = conn.execute(
            "SELECT ym, browse_users, cart_users, order_users, paid_users "
            "FROM ads_behavior_monthly ORDER BY ym").fetchall()
        print()
        print("月度明细（行为域 UV / 订单域 UV 分列，禁止跨源求比率）：")
        for ym, b, c, ou, pu in rows:
            rate = f"{c / b * 100:.2f}%" if b else "—"
            print(f"  {ym}  浏览UV={b:>6}  加购UV={c:>5}  加购率={rate:>8}  "
                  f"下单UV={ou:>6}  支付UV={pu:>6}")
        return 0 if ok else 2
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
