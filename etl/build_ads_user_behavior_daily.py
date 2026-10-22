# -*- coding: utf-8 -*-
"""
ADS 层「用户-日-行为」粒度汇总表构建（v1.8.1）
==============================================
定位：把用户分析页的 COUNT(DISTINCT user_id) 类查询从"每次扫 264 万行明细"
     改为"扫 37.9 万行预聚合"，单次 10.0s → 0.57s。

血缘（DWD → ADS）：
    fact_user_behavior ──→ ads_user_behavior_daily ──→ 用户分析页 KPI / 漏斗 / 热力图

口径等价性（关键，必须对账）：
    SUM(cnt)                 == COUNT(*)                    （事件总量）
    COUNT(DISTINCT user_id)  == COUNT(DISTINCT user_id)     （去重用户数）
    即：本表只做「同键合并计数」，不改变任何去重语义。

运行：python etl/build_ads_user_behavior_daily.py（全量幂等重建；结尾输出对账校验）
"""
import os
import sys
import time
import sqlite3

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "warehouse", "pet_supplies_dw.db")
DDL_PATH = os.path.join(BASE_DIR, "warehouse", "sql", "05_ads_user_behavior_daily.sql")


def build(conn):
    cur = conn.cursor()
    t0 = time.time()

    with open(DDL_PATH, "r", encoding="utf-8") as f:
        cur.executescript(f.read())

    cur.execute("DELETE FROM ads_user_behavior_daily")

    print("[1/2] 汇总 fact_user_behavior → ads_user_behavior_daily ...")
    t1 = time.time()
    cur.execute("""
        INSERT INTO ads_user_behavior_daily (date_id, user_id, behavior_type, cnt)
        SELECT date_id, user_id, behavior_type, COUNT(*)
        FROM fact_user_behavior
        GROUP BY date_id, user_id, behavior_type
    """)
    conn.commit()
    print(f"      写入 {cur.rowcount:,} 行，耗时 {time.time() - t1:.2f}s")

    # 第二张物化表：24 小时活跃分布（全周期口径）
    print("[2/3] 汇总 fact_user_behavior → ads_behavior_hourly ...")
    t1 = time.time()
    cur.execute("DELETE FROM ads_behavior_hourly")
    cur.execute("""
        INSERT INTO ads_behavior_hourly (behavior_hour, cnt, uv)
        SELECT behavior_hour, COUNT(*), COUNT(DISTINCT user_id)
        FROM fact_user_behavior
        GROUP BY behavior_hour
    """)
    conn.commit()
    print(f"      写入 {cur.rowcount:,} 行，耗时 {time.time() - t1:.2f}s")

    print("[3/3] 对账校验 ...")
    checks = []

    def same(name, actual, expected):
        checks.append((name, actual == expected, actual, expected))

    # 1) 事件总量守恒
    same("事件总量（SUM(cnt) = COUNT(*)）",
         cur.execute("SELECT SUM(cnt) FROM ads_user_behavior_daily").fetchone()[0],
         cur.execute("SELECT COUNT(*) FROM fact_user_behavior").fetchone()[0])

    # 2) 各行为类型事件量守恒
    #    注意：必须先 fetchall 固化为列表——在遍历同一个 cursor 的循环体里
    #    再次执行 SQL 会把外层迭代的结果集顶掉（只会生效第一次迭代）。
    for bt, n in cur.execute(
            "SELECT behavior_type, COUNT(*) FROM fact_user_behavior GROUP BY 1").fetchall():
        same(f"事件量[{bt}]",
             cur.execute("SELECT SUM(cnt) FROM ads_user_behavior_daily "
                         "WHERE behavior_type=?", (bt,)).fetchone()[0], n)

    # 3) 去重用户数等价（行为域四类型）
    same("行为域去重用户数",
         cur.execute("SELECT COUNT(DISTINCT user_id) FROM ads_user_behavior_daily "
                     "WHERE behavior_type IN ('浏览','收藏','加购','下单')").fetchone()[0],
         cur.execute("SELECT COUNT(DISTINCT user_id) FROM fact_user_behavior "
                     "WHERE behavior_type IN ('浏览','收藏','加购','下单')").fetchone()[0])

    # 4) 去重用户数等价（评价域）
    same("评价域去重用户数",
         cur.execute("SELECT COUNT(DISTINCT user_id) FROM ads_user_behavior_daily "
                     "WHERE behavior_type='评价'").fetchone()[0],
         cur.execute("SELECT COUNT(DISTINCT user_id) FROM fact_user_behavior "
                     "WHERE behavior_type='评价'").fetchone()[0])

    # 5) 主键唯一性
    same("主键组合唯一",
         cur.execute("SELECT COUNT(*) FROM (SELECT 1 FROM ads_user_behavior_daily "
                     "GROUP BY date_id, user_id, behavior_type)").fetchone()[0],
         cur.execute("SELECT COUNT(*) FROM ads_user_behavior_daily").fetchone()[0])

    # 6) 小时分布表守恒
    same("小时分布 事件量",
         cur.execute("SELECT SUM(cnt) FROM ads_behavior_hourly").fetchone()[0],
         cur.execute("SELECT COUNT(*) FROM fact_user_behavior").fetchone()[0])
    same("小时分布 时段数",
         cur.execute("SELECT COUNT(*) FROM ads_behavior_hourly").fetchone()[0],
         cur.execute("SELECT COUNT(DISTINCT behavior_hour) FROM fact_user_behavior").fetchone()[0])

    print()
    print("=" * 66)
    all_ok = True
    for name, ok, a, e in checks:
        all_ok &= ok
        print(f"  {'PASS' if ok else 'FAIL'}  {name:26s}  ADS={a}  明细={e}")
    print("=" * 66)
    rows = cur.execute("SELECT COUNT(*) FROM ads_user_behavior_daily").fetchone()[0]
    raw = cur.execute("SELECT COUNT(*) FROM fact_user_behavior").fetchone()[0]
    print(f"压缩比 {raw:,} → {rows:,} 行（{raw / max(rows, 1):.1f}×）"
          f" ｜ 总耗时 {time.time() - t0:.2f}s ｜ {'全部通过' if all_ok else '存在不一致'}")
    return all_ok


def main():
    if not os.path.exists(DB_PATH):
        print(f"[错误] 数据仓库不存在：{DB_PATH}")
        return 1
    conn = sqlite3.connect(DB_PATH)
    try:
        return 0 if build(conn) else 2
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
