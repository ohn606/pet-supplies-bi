"""
宠物用品电商 - 时序购物篮分析引擎 (Temporal Market Basket Analysis)

突破传统单订单静态关联：基于用户跨订单购买序列，挖掘类目级
「买 A → 间隔 T 天 → 买 B」时序关联规则与消耗品复购周期，
并生成补货候选清单（C1 创新点：动态生命周期补货预测）。

产出: analytics/temporal_rules.json
下游: recommendation/hybrid_recommender.py (Co-Score 特征注入)
      dashboard/server.py (/api/temporal_rules 等三个接口)

技术栈: pandas / NumPy（与 spark_analytics.py 同构，可直接迁移 PySpark）
"""
import os
import json
import sqlite3
from datetime import datetime
from collections import defaultdict

import numpy as np
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WAREHOUSE_DIR = os.path.join(BASE_DIR, "warehouse")
ANALYTICS_DIR = os.path.join(BASE_DIR, "analytics")
DB_PATH = os.path.join(WAREHOUSE_DIR, "pet_supplies_dw.db")
OUTPUT_PATH = os.path.join(ANALYTICS_DIR, "temporal_rules.json")

# ---------------- 可调参数（口径唯一来源，改前先跑分布标定） ----------------
PARAMS = {
    "max_gap_days": 120,        # 时序规则最大间隔窗口（天）
    "min_rule_users": 30,       # 规则最少覆盖用户数（时序支持度下限）
    "min_interval_count": 10,   # 复购周期统计最少间隔样本数
    "replenish_ratio": 0.8,     # 补货候选阈值：距上次购买 ≥ 周期中位数 × 0.8
    "max_candidates": 300,      # 补货候选清单上限（按用户价值排序）
}


class TemporalBasketAnalyzer:
    """跨订单时序购物篮分析器"""

    def __init__(self, db_path=None):
        if db_path is None:
            db_path = DB_PATH
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)

    def __del__(self):
        if self.conn:
            self.conn.close()

    def query_to_df(self, sql):
        return pd.read_sql_query(sql, self.conn)

    # ---------------- 数据加载：用户-类目跨订单购买序列 ----------------

    def load_sequences(self):
        """构建用户跨订单购买序列（类目粒度，同日同类目去重）"""
        df = self.query_to_df("""
            SELECT foi.user_id, foi.order_date_id,
                   dp.main_category, dp.sub_category
            FROM fact_order_items foi
            JOIN dim_product dp ON foi.product_id = dp.product_id
            ORDER BY foi.user_id, foi.order_date_id
        """)
        df["date"] = pd.to_datetime(df["order_date_id"].astype(str), format="%Y%m%d")
        # 同日同类目视为一次购买事件（避免同单多件干扰间隔分布）
        df = df.drop_duplicates(subset=["user_id", "order_date_id", "sub_category"])

        self.sequences = defaultdict(list)   # user -> [(date, main, sub)]
        self.users_by_sub = defaultdict(set)  # sub -> set(user)
        for row in df.itertuples(index=False):
            self.sequences[row.user_id].append((row.date, row.main_category, row.sub_category))
            self.users_by_sub[row.sub_category].add(row.user_id)

        self.n_users = len(self.sequences)
        self.n_events = int(len(df))
        self.date_min, self.date_max = df["date"].min(), df["date"].max()
        self.sub_categories = sorted(self.users_by_sub.keys())
        print(f"  购买序列: {self.n_users} 用户 / {self.n_events} 事件 / "
              f"{len(self.sub_categories)} 个二级类目")
        print(f"  数据周期: {self.date_min.date()} ~ {self.date_max.date()}")
        return self

    # ---------------- 1. 时序关联规则：买 A --T±分布--> B ----------------

    def mine_temporal_rules(self):
        """
        挖掘跨订单时序关联规则 (A → B, A≠B, 间隔 ≤ max_gap_days)。

        指标口径（用户级，一人一票，避免重度用户偏置）:
        - support:          users_ab / 全体用户（时序支持度）
        - confidence:       users_ab / users_a（买 A 后 T 窗口内买 B 的概率）
        - lift:             confidence / P(B)，P(B)=买过 B 用户占比（经典提升度）
        - temporal_share:   users_ab / 双向共购用户数（共购者中按 A→B 时序发生的比例，
                            衡量方向性；接近 1 表示强定向时序）
        - gap 分布:         首次命中间隔的 P25 / 中位 / P75（天）
        """
        max_gap = PARAMS["max_gap_days"]
        min_users = PARAMS["min_rule_users"]

        pair_users = defaultdict(set)    # (A, B) -> 命中用户
        pair_gaps = defaultdict(list)    # (A, B) -> 每用户首次命中间隔
        both_users = defaultdict(set)    # (A, B) -> 双向共现用户（静态基线）

        for uid, seq in self.sequences.items():
            subs = {(s) for _, _, s in seq}
            # 静态共现（不分先后/时间）
            sub_list = sorted(subs)
            for i in range(len(sub_list)):
                for j in range(i + 1, len(sub_list)):
                    a, b = sub_list[i], sub_list[j]
                    both_users[(a, b)].add(uid)
                    both_users[(b, a)].add(uid)
            # 时序命中：A 先买，max_gap 天内后买 B（仅 A≠B 跨类目规则）
            n = len(seq)
            for i in range(n):
                d_i, _, a = seq[i]
                for j in range(i + 1, n):
                    d_j, _, b = seq[j]
                    gap = (d_j - d_i).days
                    if gap <= 0:
                        continue
                    if gap > max_gap:
                        break  # 序列按时间排序，后续间隔只会更大
                    if a == b:
                        continue  # 同类目复购由 repurchase_cycles 承担
                    key = (a, b)
                    if uid not in pair_users[key]:
                        pair_users[key].add(uid)
                        pair_gaps[key].append(gap)

        rules = []
        for (a, b), users in pair_users.items():
            if len(users) < min_users:
                continue
            users_a = len(self.users_by_sub.get(a, set()))
            users_b = len(self.users_by_sub.get(b, set()))
            if users_a == 0 or users_b == 0:
                continue
            confidence = len(users) / users_a
            p_b = users_b / self.n_users
            lift = confidence / p_b if p_b > 0 else 0.0
            static_both = len(both_users.get((a, b), set()))
            temporal_share = (len(users) / static_both) if static_both > 0 else 0.0
            gaps = pair_gaps[(a, b)]
            rules.append({
                "antecedent": a,
                "consequent": b,
                "users_a": users_a,
                "users_b": users_b,
                "users_ab": len(users),
                "support": round(len(users) / self.n_users, 4),
                "confidence": round(confidence, 4),
                "lift": round(lift, 3),
                "temporal_share": round(temporal_share, 4),
                "gap_count": len(gaps),
                "gap_mean": round(float(np.mean(gaps)), 1),
                "gap_p25": round(float(np.percentile(gaps, 25)), 1),
                "gap_median": round(float(np.median(gaps)), 1),
                "gap_p75": round(float(np.percentile(gaps, 75)), 1),
            })

        # 排序：时序支持度优先，其次置信度
        rules.sort(key=lambda r: (-r["users_ab"], -r["confidence"]))
        self.rules = rules
        print(f"  时序规则: {len(rules)} 条（min_users={min_users}, 窗口≤{max_gap}天）")
        for r in rules[:10]:
            print(f"    {r['antecedent']} → {r['consequent']}: "
                  f"users={r['users_ab']}, conf={r['confidence']:.3f}, "
                  f"lift={r['lift']:.2f}, share={r['temporal_share']:.2f}, "
                  f"中位间隔={r['gap_median']}天")
        return self

    # ---------------- 2. 消耗品复购周期画像（同二级类目） ----------------

    def repurchase_cycles(self):
        """同二级类目相邻两次购买的间隔分布 → 典型补货周期"""
        min_int = PARAMS["min_interval_count"]
        sub_intervals = defaultdict(list)
        for uid, seq in self.sequences.items():
            last_date = {}
            for d, _, s in seq:
                if s in last_date:
                    sub_intervals[s].append((d - last_date[s]).days)
                last_date[s] = d

        cycles = []
        sub_main = {}
        for seq in self.sequences.values():
            for _, m, s in seq:
                sub_main.setdefault(s, m)

        for s, gaps in sub_intervals.items():
            buyers = len(self.users_by_sub.get(s, set()))
            if len(gaps) < min_int or buyers == 0:
                continue
            rebuy_users = sum(1 for u in self.users_by_sub[s]
                              if sum(1 for _, _, ss in self.sequences[u] if ss == s) >= 2)
            cycles.append({
                "sub_category": s,
                "main_category": sub_main.get(s, ""),
                "buyers": buyers,
                "rebuy_users": rebuy_users,
                "rebuy_rate": round(rebuy_users / buyers, 4),
                "interval_count": len(gaps),
                "cycle_mean": round(float(np.mean(gaps)), 1),
                "cycle_p25": round(float(np.percentile(gaps, 25)), 1),
                "cycle_median": round(float(np.median(gaps)), 1),
                "cycle_p75": round(float(np.percentile(gaps, 75)), 1),
            })
        cycles.sort(key=lambda c: -c["interval_count"])
        self.cycles = cycles
        print(f"  复购周期画像: {len(cycles)} 个类目（间隔样本≥{min_int}）")
        for c in cycles[:8]:
            print(f"    {c['sub_category']}: 周期中位={c['cycle_median']}天 "
                  f"(P25-P75: {c['cycle_p25']}-{c['cycle_p75']}), 复购率={c['rebuy_rate']:.2%}")
        return self

    # ---------------- 3. 补货候选清单（动态生命周期预测应用） ----------------

    def replenishment_candidates(self):
        """
        补货候选：用户买过类目 C，且距上次购买 ≥ 周期中位数 × ratio，
        视为进入补货窗口的高转化触达对象。按用户累计消费排序，附推荐商品。
        """
        ratio = PARAMS["replenish_ratio"]
        cycle_map = {c["sub_category"]: c for c in self.cycles}
        # 类目下热销商品（用于候选触达推荐）
        hot_products = self.query_to_df("""
            SELECT dp.sub_category, dp.product_id, dp.product_name, dp.price,
                   dp.total_sales
            FROM dim_product dp
            WHERE dp.total_sales > 0
        """).sort_values(["sub_category", "total_sales"], ascending=[True, False])
        sub_top = defaultdict(list)
        for row in hot_products.itertuples(index=False):
            if len(sub_top[row.sub_category]) < 3:
                sub_top[row.sub_category].append({
                    "product_id": row.product_id,
                    "product_name": row.product_name,
                    "price": round(float(row.price), 2),
                    "total_sales": int(row.total_sales),
                })

        users_info = self.query_to_df("""
            SELECT user_id, rfm_segment, lifetime_amount, lifetime_orders
            FROM dim_user WHERE lifetime_orders > 0
        """).set_index("user_id")

        end_days = (self.date_max - pd.Timestamp("1970-01-01")).days
        candidates = []
        for uid, seq in self.sequences.items():
            last_date = {}
            for d, m, s in seq:
                last_date[s] = (d, m)
            for s, (d, m) in last_date.items():
                cyc = cycle_map.get(s)
                if not cyc or cyc["cycle_median"] <= 0:
                    continue
                elapsed = end_days - (d - pd.Timestamp("1970-01-01")).days
                if elapsed < cyc["cycle_median"] * ratio:
                    continue
                info = users_info.loc[uid] if uid in users_info.index else None
                candidates.append({
                    "user_id": uid,
                    "rfm_segment": str(info["rfm_segment"]) if info is not None else "未知",
                    "lifetime_amount": round(float(info["lifetime_amount"]) if info is not None else 0, 2),
                    "main_category": m,
                    "sub_category": s,
                    "last_purchase_date": d.strftime("%Y-%m-%d"),
                    "cycle_median": cyc["cycle_median"],
                    "days_elapsed": int(elapsed),
                    "cycle_ratio": round(elapsed / cyc["cycle_median"], 2),
                    "suggested_products": sub_top.get(s, []),
                })

        candidates.sort(key=lambda c: -c["lifetime_amount"])
        self.candidates = candidates[:PARAMS["max_candidates"]]
        print(f"  补货候选: {len(candidates)} 条（阈值 ratio≥{ratio}），"
              f"输出 TOP {len(self.candidates)}（按用户累计消费）")
        return self

    # ---------------- 汇总输出 ----------------

    def save(self):
        output = {
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "params": PARAMS,
            "data_stats": {
                "users": self.n_users,
                "purchase_events": self.n_events,
                "sub_categories": len(self.sub_categories),
                "date_start": self.date_min.strftime("%Y-%m-%d"),
                "date_end": self.date_max.strftime("%Y-%m-%d"),
                "rule_count": len(self.rules),
                "cycle_category_count": len(self.cycles),
                "candidate_count": len(self.candidates),
            },
            "rules": self.rules,
            "repurchase_cycles": self.cycles,
            "replenishment_candidates": self.candidates,
        }
        with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        print(f"  结果已保存: {OUTPUT_PATH}")
        return output


def main():
    print("=" * 60)
    print("宠物用品电商 - 时序购物篮分析引擎")
    print(f"启动: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    analyzer = TemporalBasketAnalyzer()
    print("\n[1/4] 构建跨订单购买序列...")
    analyzer.load_sequences()
    print("\n[2/4] 挖掘时序关联规则 (买 A → T 天 → B)...")
    analyzer.mine_temporal_rules()
    print("\n[3/4] 消耗品复购周期画像...")
    analyzer.repurchase_cycles()
    print("\n[4/4] 生成补货候选清单...")
    analyzer.replenishment_candidates()
    analyzer.save()
    print("\n完成。")


if __name__ == "__main__":
    main()
