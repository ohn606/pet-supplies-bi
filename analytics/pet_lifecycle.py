"""
宠物用品电商 - 宠物生命周期推断与双维画像补救 (Pet Lifecycle Inference)

背景：4 个真实数据集中**不存在宠物品种/年龄/体重等宠物显性属性字段**
（dim_user.pet_age_group 全部为「未知」），研究创新点 C2 提出的
「主人 + 宠物」双维画像在宠物侧缺少直接数据支撑。

本模块**不虚构任何不存在的字段**，而是从真实商品文本（product_name /
description）与真实购买序列中**显式派生**可追溯的生命阶段证据，
推断宠物所处生命阶段，并检测「幼年粮 → 成年粮」的切换行为。

数据铁律遵循：
1. 阶段证据全部来自真实商品文本正则命中，evidence 字段保留命中的
   商品 ID 与原文片段，可逐条回溯核验；
2. 无法判定的用户一律标注为「未知」，不做默认值填充、不做概率臆测；
3. 多阶段冲突（同一商品同时命中幼年与成年）标记为 multi，不强行归类；
4. 切换间隔为**观测窗口内的购买行为间隔（天）**，不等于宠物实际月龄，
   输出中显式声明该语义边界（数据无宠物出生日期，无法推算真实年龄）。

产出: analytics/pet_lifecycle.json
      warehouse 派生表 ads_pet_lifecycle（可 SQL 回溯，表名带 ads_ 前缀标识派生层）
下游: dashboard/server.py (/api/pet_lifecycle)

技术栈: pandas / NumPy（与 spark_analytics.py 同构，可直接迁移 PySpark）
"""
import os
import re
import json
import sqlite3
from datetime import datetime
from collections import defaultdict, Counter

import numpy as np
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WAREHOUSE_DIR = os.path.join(BASE_DIR, "warehouse")
ANALYTICS_DIR = os.path.join(BASE_DIR, "analytics")
SQL_DIR = os.path.join(WAREHOUSE_DIR, "sql")
DB_PATH = os.path.join(WAREHOUSE_DIR, "pet_supplies_dw.db")
OUTPUT_PATH = os.path.join(ANALYTICS_DIR, "pet_lifecycle.json")
DDL_PATH = os.path.join(SQL_DIR, "03_ads_pet_lifecycle.sql")

# ---------------- 可调参数（口径唯一来源） ----------------
PARAMS = {
    "conflict_as_multi": True,   # 同商品命中多个互斥阶段 → 记为 multi（不强行归类）
    "senior_over_adult": True,   # 同时命中 senior+adult 时 senior 优先（更具体）
    "min_evidence_high": 3,      # 有效证据 ≥3 且纯度 ≥purity_high → 高置信
    "purity_high": 0.70,
    "min_evidence_medium": 2,    # 有效证据 ≥2 → 中置信
    "max_evidence_products": 5,  # 每个用户保留的可追溯证据商品数上限
    "top_profile_categories": 5, # 画像中保留的类目偏好数量
    "min_switch_gap_days": 30,   # 切换判定最小间隔（< 该值无法区分「成长切换」与「并行囤货」）
}

# 生命阶段关键词（仅对消耗品类目生效；命中即产生一条可追溯证据）
# 说明：未采用 "maintenance" —— 该词在保健品语境（如关节维持期）中不含明确生命阶段语义，
#       纳入会引入系统误判，故剔除（铁律：宁可少判，不可臆断）。
STAGE_PATTERNS = {
    "幼年": r"\b(puppy|puppies|kitten|kittens|junior)\b|\b(growth|growing|weaning)\b",
    "成年": r"\badult\b|\bmature\b",
    "老年": r"\b(senior|geriatric|aging)\b|\baged\b|\b7\+\b|\b11\+\b",
}
ALL_STAGE_PATTERN = r"all life stage[s]?|all ages|any age|all breed[s]? and age"

# 仅在这些消耗品类目中判定生命阶段（避免玩具/窝笼等无关品误命中）
CONSUMABLE_SUBS = {
    "主粮", "零食", "Food", "Treats",
    "健康护理", "Health Supplies", "Vitamins & Supplements",
    "Healthcare", "Health & Wellness",
}

STAGE_ORDER = ["幼年", "成年", "老年"]
UNKNOWN = "未知"


class PetLifecycleAnalyzer:
    """宠物生命阶段推断器（证据驱动，全程可追溯）"""

    def __init__(self, db_path=None):
        self.db_path = db_path or DB_PATH
        self.conn = sqlite3.connect(self.db_path)

    def __del__(self):
        if self.conn:
            self.conn.close()

    def query_to_df(self, sql):
        return pd.read_sql_query(sql, self.conn)

    # ---------------- 1. 商品侧：生命阶段打标 ----------------

    def tag_products(self):
        """对消耗品商品做生命阶段打标（返回打标 DataFrame + 统计）"""
        df = self.query_to_df("""
            SELECT product_id, product_name, main_category, sub_category,
                   brand, price, description
            FROM dim_product
        """)
        df = df[df["sub_category"].isin(CONSUMABLE_SUBS)].copy()

        tags, hits = [], []
        for row in df.itertuples(index=False):
            text = f"{row.product_name or ''} {row.description or ''}"
            hits_found = {s: bool(re.search(p, text, re.I)) for s, p in STAGE_PATTERNS.items()}
            n_hit = sum(hits_found.values())
            all_stage = bool(re.search(ALL_STAGE_PATTERN, text, re.I))

            if n_hit == 0:
                tag = "全阶段" if all_stage else "无标记"
            elif n_hit >= 2:
                # 互斥冲突处理
                if PARAMS["senior_over_adult"] and hits_found["老年"] and hits_found["成年"] \
                        and not hits_found["幼年"]:
                    tag = "老年"        # senior 更具体，非冲突
                elif PARAMS["conflict_as_multi"]:
                    tag = "多阶段"      # 真冲突：幼年与成年/老年同现，不强行归类
                else:
                    tag = "多阶段"
            else:
                tag = [s for s, v in hits_found.items() if v][0]

            tags.append(tag)
            hits.append({s: hits_found[s] for s in STAGE_ORDER})

        df["life_stage_tag"] = tags
        df["all_stage_flag"] = [bool(re.search(ALL_STAGE_PATTERN,
                                               f"{n or ''} {d or ''}", re.I))
                                for n, d in zip(df["product_name"], df["description"])]
        self.product_tags = df
        tag_stats = Counter(tags)
        print(f"  消耗品商品: {len(df)} 件，打标分布: {dict(tag_stats)}")
        return df, tag_stats

    # ---------------- 2. 用户侧：证据聚合与阶段推断 ----------------

    def build_user_evidence(self):
        """按用户聚合生命阶段购买证据（保留首次/末次日期与可追溯商品）"""
        tagged_ids = self.product_tags.loc[
            self.product_tags["life_stage_tag"].isin(STAGE_ORDER), "product_id"
        ].tolist()
        id_set = set(tagged_ids)
        if not id_set:
            raise RuntimeError("未命中任何生命阶段商品，请检查关键词或类目过滤")

        ph = ",".join(f"'{i}'" for i in id_set)
        items = self.query_to_df(f"""
            SELECT foi.user_id, foi.product_id, foi.order_date_id, foi.item_amount,
                   foi.quantity, dp.sub_category, dp.main_category, dp.product_name
            FROM fact_order_items foi
            JOIN dim_product dp ON foi.product_id = dp.product_id
            WHERE foi.product_id IN ({ph})
        """)
        tag_map = dict(zip(self.product_tags["product_id"], self.product_tags["life_stage_tag"]))
        items["stage"] = items["product_id"].map(tag_map)
        items["date"] = pd.to_datetime(items["order_date_id"].astype(str), format="%Y%m%d")

        users = self.query_to_df("""
            SELECT user_id, pet_type, lifetime_orders, lifetime_amount, avg_order_amount
            FROM dim_user
        """).set_index("user_id")

        evidence = {}
        for uid, grp in items.groupby("user_id"):
            rec = {
                "n": defaultdict(int),
                "first": {}, "last": {},
                "products": defaultdict(list),
                "amount": defaultdict(float),
            }
            for row in grp.itertuples(index=False):
                s = row.stage
                rec["n"][s] += 1
                d = row.date
                rec["first"][s] = min(rec["first"].get(s, d), d)
                rec["last"][s] = max(rec["last"].get(s, d), d)
                rec["amount"][s] += float(row.item_amount or 0)
                if len(rec["products"][s]) < PARAMS["max_evidence_products"]:
                    rec["products"][s].append({"product_id": row.product_id,
                                               "name": (row.product_name or "")[:80],
                                               "date": d.strftime("%Y-%m-%d")})
            evidence[uid] = rec

        self.user_evidence = evidence
        self.items = items
        self.users = users
        print(f"  产生阶段证据的购买记录: {len(items)} 条，覆盖用户: {len(evidence)}")
        return evidence

    @staticmethod
    def _confidence(n_effective, purity):
        if n_effective >= PARAMS["min_evidence_high"] and purity >= PARAMS["purity_high"]:
            return "高"
        if n_effective >= PARAMS["min_evidence_medium"] or purity >= 0.6:
            return "中"
        return "低"

    @staticmethod
    def _days(a, b):
        return int((b - a).days)

    def infer_user_stage(self, rec):
        """单用户阶段推断（规则透明，返回 stage / confidence / switch）"""
        n = {s: rec["n"].get(s, 0) for s in STAGE_ORDER}
        n_eff = sum(n.values())
        if n_eff == 0:
            return {"stage": UNKNOWN, "confidence": "—", "reason": "无生命阶段证据",
                    "switch_status": "insufficient_evidence"}

        dominant = max(n, key=lambda s: n[s])
        purity = n[dominant] / n_eff
        conf = self._confidence(n_eff, purity)

        # 切换检测的四种状态（均只描述观测行为，不推断宠物真实年龄）
        switch = {"switch_status": "not_applicable", "switch_date": None, "switch_gap_days": None}
        has_y, has_a = n["幼年"] > 0, n["成年"] > 0

        if has_y and has_a:
            fy, fa = rec["first"]["幼年"], rec["first"]["成年"]
            ly, la = rec["last"]["幼年"], rec["last"]["成年"]
            gap = self._days(fy, fa)
            if gap == 0:
                # 同一天同时下单幼年粮与成年粮：属混合购买，不构成切换证据
                switch["switch_status"] = "same_day_mixed"
                switch["switch_gap_days"] = 0
            elif 0 < gap < PARAMS["min_switch_gap_days"]:
                # 间隔短于一个复购周期：无法区分「成长切换」与「并行囤货/多宠」，不判切换
                switch["switch_status"] = "short_interval_undetermined"
                switch["switch_gap_days"] = gap
            elif fa >= fy:
                switch["switch_status"] = "confirmed" if la > ly else "ongoing"
                switch["switch_date"] = fa.strftime("%Y-%m-%d")
                switch["switch_gap_days"] = gap
            else:
                # 先买成年粮后买幼年粮：不臆断（可能是新养幼宠/为他人代买）
                switch["switch_status"] = "reverse_order_undetermined"
                switch["switch_date"] = fy.strftime("%Y-%m-%d")
                switch["switch_gap_days"] = self._days(fa, fy)
        elif has_y:
            switch["switch_status"] = "young_only_no_switch_observed"
        elif has_a:
            switch["switch_status"] = "adult_only_no_baseline"

        # 阶段判定
        if n_eff == 1 or purity == 1.0:
            stage = dominant
            reason = f"唯一阶段证据（{dominant}×{n[dominant]}）"
        elif has_y and has_a and switch["switch_status"] == "confirmed":
            stage = "成年"
            reason = f"幼年→成年切换已观测（幼年最后 {rec['last']['幼年'].date()}，成年最后 {rec['last']['成年'].date()}）"
            conf = self._confidence(n["成年"], n["成年"] / max(1, n["成年"] + n["幼年"]))
        elif has_y and has_a:
            stage = "过渡中"
            reason = "幼年与成年证据并存且幼年粮仍在购买，判定为过渡期"
            conf = "低"
        elif n["老年"] > 0 and rec["last"]["老年"] >= max(
                [rec["last"].get(s, rec["last"]["老年"]) for s in STAGE_ORDER if n[s] > 0]):
            stage = "老年"
            reason = f"老年粮为最近购买阶段（老年×{n['老年']}）"
        elif switch["switch_status"] in ("same_day_mixed", "short_interval_undetermined"):
            stage = dominant
            reason = (f"幼年/成年证据并存但无法判定切换（{switch['switch_status']}，"
                      f"间隔 {switch['switch_gap_days']} 天 < {PARAMS['min_switch_gap_days']} 天阈值），"
                      f"以占比最高者 {dominant} 作为弱推断")
            conf = "低"
        else:
            stage = dominant
            reason = f"多阶段并存，以占比最高者为准（{dominant} 纯度 {purity:.2f}）"

        out = {"stage": stage, "confidence": conf, "reason": reason}
        out.update(switch)
        return out

    def run_inference(self):
        rows = []
        for uid, rec in self.user_evidence.items():
            r = self.infer_user_stage(rec)
            u = self.users.loc[uid] if uid in self.users.index else None
            n = {s: rec["n"].get(s, 0) for s in STAGE_ORDER}
            rows.append({
                "user_id": uid,
                "pet_type": (u["pet_type"] if u is not None else UNKNOWN),
                "inferred_stage": r["stage"],
                "stage_confidence": r["confidence"],
                "n_young": n["幼年"], "n_adult": n["成年"], "n_senior": n["老年"],
                "first_young_date": rec["first"].get("幼年").strftime("%Y-%m-%d") if n["幼年"] else None,
                "last_young_date": rec["last"].get("幼年").strftime("%Y-%m-%d") if n["幼年"] else None,
                "first_adult_date": rec["first"].get("成年").strftime("%Y-%m-%d") if n["成年"] else None,
                "last_adult_date": rec["last"].get("成年").strftime("%Y-%m-%d") if n["成年"] else None,
                "switch_status": r["switch_status"],
                "switch_date": r["switch_date"],
                "switch_gap_days": r["switch_gap_days"],
                "reason": r["reason"],
                "evidence": json.dumps(
                    {s: rec["products"].get(s, []) for s in STAGE_ORDER if n[s] > 0},
                    ensure_ascii=False),
                "lifetime_orders": int(u["lifetime_orders"]) if u is not None else None,
                "lifetime_amount": float(u["lifetime_amount"]) if u is not None else None,
            })
        df = pd.DataFrame(rows)
        self.user_stage_df = df
        print(f"  阶段推断: {len(df)} 用户")
        print(f"  分布: {dict(Counter(df['inferred_stage']))}")
        print(f"  置信度: {dict(Counter(df['stage_confidence']))}")
        return df

    # ---------------- 3. 切换间隔分布 ----------------

    def switch_stats(self, df):
        sw = df[(df["switch_status"].isin(["confirmed", "ongoing"]))
                & df["switch_gap_days"].notna()]
        win = self.query_to_df("""
            SELECT MIN(order_date_id) a, MAX(order_date_id) b FROM fact_order_items
        """)
        d0 = pd.to_datetime(str(int(win["a"][0])), format="%Y%m%d")
        d1 = pd.to_datetime(str(int(win["b"][0])), format="%Y%m%d")
        window_days = (d1 - d0).days
        gaps = sw["switch_gap_days"].astype(float).values
        confirmed = df[df["switch_status"] == "confirmed"]
        cg = confirmed["switch_gap_days"].dropna().astype(float).values
        res = {
            "switch_observed_users": int(len(sw)),
            "confirmed_switch_users": int(len(confirmed)),
            "ongoing_switch_users": int((df["switch_status"] == "ongoing").sum()),
            "reverse_order_users": int((df["switch_status"] == "reverse_order_undetermined").sum()),
            "young_only_users": int((df["switch_status"] == "young_only_no_switch_observed").sum()),
            "adult_only_users": int((df["switch_status"] == "adult_only_no_baseline").sum()),
            "same_day_mixed_users": int((df["switch_status"] == "same_day_mixed").sum()),
            "short_interval_users": int((df["switch_status"] == "short_interval_undetermined").sum()),
            "observation_window_days": window_days,
            "observation_window": f"{d0.date()} ~ {d1.date()}",
            "min_switch_gap_days": PARAMS["min_switch_gap_days"],
            "gap_p25": round(float(np.percentile(gaps, 25)), 1) if len(gaps) else None,
            "gap_median": round(float(np.median(gaps)), 1) if len(gaps) else None,
            "gap_p75": round(float(np.percentile(gaps, 75)), 1) if len(gaps) else None,
            "gap_mean": round(float(np.mean(gaps)), 1) if len(gaps) else None,
            "confirmed_gap_median": round(float(np.median(cg)), 1) if len(cg) else None,
            "confirmed_gap_p25": round(float(np.percentile(cg, 25)), 1) if len(cg) else None,
            "confirmed_gap_p75": round(float(np.percentile(cg, 75)), 1) if len(cg) else None,
            "semantic_note": (
                "switch_gap_days 为同一用户首次购买幼年粮与首次购买成年粮之间的观测间隔（天）。"
                f"数据集观测窗口仅 {window_days} 天（{d0.date()}~{d1.date()}），"
                "远小于犬/猫约 12 个月的成长周期，因此本数据集**在原理上无法观测完整的"
                "幼年→成年成长切换**；此处结果只能作为「粮型偏好迁移」的行为证据，"
                "不等于宠物实际月龄（数据无出生日期字段，无法推算真实年龄）。"),
        }
        print(f"  切换观测: 共 {res['switch_observed_users']} 人（确认 {res['confirmed_switch_users']}，"
              f"过渡中 {res['ongoing_switch_users']}），中位间隔 {res['gap_median']} 天")
        return res

    # ---------------- 4. 宠物维度画像（补救 C2） ----------------

    def pet_profiles(self, df):
        """按 (推断阶段 × 宠物类型) 构建宠物维度画像"""
        # 用户-类目偏好（真实购买）
        pref = self.query_to_df("""
            SELECT foi.user_id, dp.sub_category, SUM(foi.item_amount) AS amt,
                   COUNT(*) AS cnt
            FROM fact_order_items foi
            JOIN dim_product dp ON foi.product_id = dp.product_id
            GROUP BY foi.user_id, dp.sub_category
        """)
        top_cat = {}
        for uid, g in pref.groupby("user_id"):
            s = g.groupby("sub_category")["cnt"].sum().sort_values(ascending=False)
            top_cat[uid] = list(s.index[:PARAMS["top_profile_categories"]])

        profiles = []
        for (stage, ptype), g in df.groupby(["inferred_stage", "pet_type"]):
            if len(g) < 30:      # 样本过小的分组不输出结论（避免噪声解读）
                continue
            cats = Counter()
            for uid in g["user_id"]:
                for c in top_cat.get(uid, []):
                    cats[c] += 1
            profiles.append({
                "inferred_stage": stage,
                "pet_type": ptype,
                "user_count": int(len(g)),
                "avg_orders": round(float(g["lifetime_orders"].mean()), 2),
                "avg_lifetime_amount": round(float(g["lifetime_amount"].mean()), 2),
                "confidence_dist": dict(Counter(g["stage_confidence"])),
                "top_sub_categories": [{"sub_category": c, "users": n}
                                       for c, n in cats.most_common(5)],
                "derived_flag": True,
                "note": "inferred_stage 为派生推断字段（非原始字段），pet_type 为原始字段",
            })
        profiles.sort(key=lambda x: -x["user_count"])
        print(f"  宠物维度画像分组: {len(profiles)} 个")
        return profiles

    # ---------------- 5. 落库（派生层，可 SQL 回溯） ----------------

    def save_table(self, df):
        ddl = """
-- 派生层：宠物生命周期推断结果（ads_ 前缀标识为派生/推断数据，非原始数据）
-- 由 analytics/pet_lifecycle.py 生成，可整体重建，不参与 ETL 增量链路
DROP TABLE IF EXISTS ads_pet_lifecycle;
CREATE TABLE ads_pet_lifecycle (
    user_id           TEXT PRIMARY KEY,
    pet_type          TEXT,      -- 原始字段（dim_user）
    inferred_stage    TEXT,      -- 派生：幼年/成年/老年/过渡中/未知
    stage_confidence  TEXT,      -- 高/中/低（证据条数与纯度决定）
    n_young           INTEGER,
    n_adult           INTEGER,
    n_senior          INTEGER,
    first_young_date  TEXT,
    last_young_date   TEXT,
    first_adult_date  TEXT,
    last_adult_date   TEXT,
    switch_status     TEXT,      -- confirmed/ongoing/reverse_order_undetermined/...
    switch_date       TEXT,
    switch_gap_days   INTEGER,   -- 观测间隔（天），非宠物月龄
    reason            TEXT,      -- 判定依据（可追溯）
    evidence          TEXT,      -- JSON：命中的商品 ID / 名称 / 日期
    lifetime_orders   INTEGER,
    lifetime_amount   REAL,
    etl_update_time   TEXT
);
"""
        os.makedirs(SQL_DIR, exist_ok=True)
        with open(DDL_PATH, "w", encoding="utf-8") as f:
            f.write(ddl.strip() + "\n")
        cur = self.conn.cursor()
        cur.executescript(ddl)
        out = df.copy()
        out["etl_update_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cols = ["user_id", "pet_type", "inferred_stage", "stage_confidence",
                "n_young", "n_adult", "n_senior", "first_young_date", "last_young_date",
                "first_adult_date", "last_adult_date", "switch_status", "switch_date",
                "switch_gap_days", "reason", "evidence", "lifetime_orders",
                "lifetime_amount", "etl_update_time"]
        out[cols].to_sql("ads_pet_lifecycle", self.conn, if_exists="append", index=False)
        self.conn.commit()
        n = cur.execute("SELECT COUNT(*) FROM ads_pet_lifecycle").fetchone()[0]
        print(f"  派生表 ads_pet_lifecycle 已写入: {n} 行（DDL 归档: warehouse/sql/03_ads_pet_lifecycle.sql）")

    # ---------------- 主流程 ----------------

    def run(self):
        print("=" * 70)
        print("宠物生命周期推断（证据驱动 · 全程可追溯 · 未知显式标注）")
        print("=" * 70)
        t0 = datetime.now()

        print("\n[1/5] 商品侧生命阶段打标 ...")
        _, tag_stats = self.tag_products()

        print("\n[2/5] 用户侧证据聚合 ...")
        self.build_user_evidence()

        print("\n[3/5] 阶段推断与切换检测 ...")
        df = self.run_inference()

        print("\n[4/5] 切换间隔分布 + 宠物维度画像 ...")
        sw_stats = self.switch_stats(df)
        profiles = self.pet_profiles(df)

        print("\n[5/5] 写入派生表与 JSON ...")
        self.save_table(df)

        stage_dist = Counter(df["inferred_stage"])
        conf_dist = Counter(df["stage_confidence"])
        total_users = int(self.query_to_df("SELECT COUNT(*) c FROM dim_user")["c"][0])

        result = {
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "params": PARAMS,
            "method": {
                "evidence_source": "dim_product.product_name + description 正则命中（消耗品类目）",
                "stage_patterns": STAGE_PATTERNS,
                "all_stage_pattern": ALL_STAGE_PATTERN,
                "consumable_sub_categories": sorted(CONSUMABLE_SUBS),
                "conflict_policy": "同商品命中互斥阶段 → 记为「多阶段」，不参与单阶段计数；senior+adult 时 senior 优先",
                "confidence_rule": "有效证据≥3 且纯度≥0.70 → 高；≥2 或纯度≥0.6 → 中；否则低",
                "unknown_policy": "无任何阶段证据 → 显式标注「未知」，置信度「—」",
            },
            "data_stats": {
                "total_users": total_users,
                "users_with_evidence": int(len(df)),
                "users_without_evidence": total_users - int(len(df)),
                "users_unknown_stage": total_users - int(len(df)) + int(stage_dist.get(UNKNOWN, 0)),
                "evidence_coverage": round(len(df) / total_users, 4),
                "evidence_records": int(len(self.items)),
                "tagged_products": int((self.product_tags["life_stage_tag"].isin(STAGE_ORDER)).sum()),
            },
            "product_tag_stats": dict(tag_stats),
            "user_stage_distribution": dict(stage_dist),
            "confidence_distribution": dict(conf_dist),
            "switch_stats": sw_stats,
            "pet_profiles": profiles,
            "switch_cases": df[df["switch_status"] == "confirmed"]
                .sort_values("switch_gap_days")
                .head(50)[["user_id", "pet_type", "first_young_date", "first_adult_date",
                           "switch_gap_days", "n_young", "n_adult", "stage_confidence"]]
                .to_dict(orient="records"),
            "limitations": [
                "4 个原始数据集均无宠物品种/年龄/体重字段，dim_user.pet_age_group 100% 为「未知」",
                "生命阶段为基于商品文本的派生推断，非平台标注的真实标签，无 ground truth 可校验准确率",
                f"数据集观测窗口仅 {sw_stats['observation_window_days']} 天，远小于犬/猫约 12 个月成长周期，"
                "原理上无法观测完整的幼年→成年成长切换，结果仅可作为粮型偏好迁移的行为证据",
                "购买记录为快照式历史行为（大量同日多品下单），非连续纵向追踪，间隔语义受限",
                "用户购买可能包含为他人/多宠代买，无法从数据区分",
                "关键词规则基于英文商品标题，未覆盖的中文/小众表述会被判为「无标记」",
            ],
        }

        with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"\n✓ 产出: {OUTPUT_PATH}")
        print(f"  耗时: {(datetime.now() - t0).total_seconds():.1f}s")
        return result


def main():
    PetLifecycleAnalyzer().run()


if __name__ == "__main__":
    main()
