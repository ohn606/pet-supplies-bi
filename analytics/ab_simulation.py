"""
宠物用品电商 - 在线模拟 A/B 实验平台 (Simulated A/B Testing)

在**模拟环境**中对比多组推荐策略/参数配置，输出可复现的对比结果与结论。
设计原则（数据铁律）：

1. **双层口径，显式分离**：
   - 真实锚点层：策略推荐列表与用户**测试期真实购买**的命中率（real_hit_rate），
     完全由真实数据计算，无任何模拟假设；
   - 模拟响应层：CTR / CVR / GMV 由**显式声明的响应模型**生成
     （点击概率 = f(类目亲和度, 时序周期接近度, 位置折扣)），
     该层是建模假设而非数据事实，所有输出均携带 `simulated: true` 标记。
2. **可复现**：固定随机种子（默认 42）；同一 (种子, 策略组, 样本) 配置重跑结果逐位一致。
3. **公平对比**：所有策略共享同一响应模型、同一用户样本、同一曝光位次结构，
   唯一差异是策略参数；显著性检验采用用户级配对 bootstrap。
4. **防泄漏**：策略可见信息仅限训练期（< split_date）；测试期数据仅用于评估。

产出: analytics/ab_simulation_results.json（最近一次运行快照）
下游: dashboard/server.py (/api/ab_presets, /api/ab_simulate)

技术栈: pandas / NumPy（与 spark_analytics.py 同构）
"""
import os
import re
import json
import math
import sqlite3
import hashlib
from datetime import datetime

import numpy as np
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WAREHOUSE_DIR = os.path.join(BASE_DIR, "warehouse")
ANALYTICS_DIR = os.path.join(BASE_DIR, "analytics")
DB_PATH = os.path.join(WAREHOUSE_DIR, "pet_supplies_dw.db")
TEMPORAL_FILE = os.path.join(ANALYTICS_DIR, "temporal_rules.json")
OUTPUT_PATH = os.path.join(ANALYTICS_DIR, "ab_simulation_results.json")

# ---------------- 默认参数（口径唯一来源） ----------------
DEFAULT_PARAMS = {
    "split_date": 20180801,     # 训练/测试时间切分（YYYYMMDD）
    "top_k": 10,                # 每用户曝光位数量
    "slots_per_category": 3,    # 每个入选类目占用的曝光位（类目内按热度取 top N）
    "base_ctr": 0.04,           # 响应模型：基准点击率（模拟假设）
    "base_cvr": 0.25,           # 响应模型：点击后基准转化率（模拟假设）
    "affinity_gain": 0.9,       # 响应模型：类目亲和度增益上限
    "co_gain": 0.6,             # 响应模型：时序周期接近度增益上限
    "position_decay": 0.85,     # 响应模型：曝光位次折扣（rank i → decay^i）
    "price_fit_gain": 0.3,      # 响应模型：价格带匹配增益上限
    "n_bootstrap": 1000,        # 配对 bootstrap 重抽样次数
    "min_test_orders": 1,       # 用户进入实验的最低测试期订单数
}

# 预设策略组（可被 API 参数覆盖；参数语义见 score_frame 注释）
PRESET_STRATEGIES = [
    {"key": "A", "name": "A · 纯热度基线", "params": {"w_pop": 1.0, "w_affinity": 0.0, "w_co": 0.0, "epsilon": 0.0}},
    {"key": "B", "name": "B · 个性化类目亲和", "params": {"w_pop": 0.3, "w_affinity": 0.7, "w_co": 0.0, "epsilon": 0.0}},
    {"key": "C", "name": "C · 亲和 + 时序补货信号", "params": {"w_pop": 0.2, "w_affinity": 0.4, "w_co": 0.4, "epsilon": 0.0}},
]


class ABSimulator:
    """离线回放 + 显式响应模型的 A/B 模拟器"""

    def __init__(self, db_path=None):
        self.db_path = db_path or DB_PATH
        self.conn = sqlite3.connect(self.db_path)

    def __del__(self):
        if self.conn:
            self.conn.close()

    # ---------------- 数据准备 ----------------

    def load_data(self, split_date):
        """订单明细（含商品价格/类目）+ 训练期类目热度"""
        items = pd.read_sql_query(f"""
            SELECT foi.user_id, foi.order_date_id, foi.product_id,
                   foi.quantity, foi.item_amount, dp.sub_category, dp.price
            FROM fact_order_items foi
            JOIN dim_product dp ON foi.product_id = dp.product_id
            WHERE foi.order_date_id BETWEEN 20180101 AND 20181231
        """, self.conn)
        items["date"] = pd.to_datetime(items["order_date_id"].astype(str), format="%Y%m%d")

        self.items = items
        self.train = items[items["order_date_id"] < split_date]
        self.test = items[items["order_date_id"] >= split_date]

        # 训练期商品热度（购买次数）与类目热度
        self.item_pop = self.train.groupby("product_id").size()
        self.sub_pop = self.train.groupby("sub_category").size()
        # 类目内热度排行（曝光位候选）
        self.sub_top_items = {}
        for sub, g in self.train.groupby("sub_category"):
            cnt = g.groupby("product_id").size().sort_values(ascending=False)
            self.sub_top_items[sub] = list(cnt.index[:50])
        print(f"  训练期: {len(self.train)} 条（{self.train['user_id'].nunique()} 用户），"
              f"测试期: {len(self.test)} 条（{self.test['user_id'].nunique()} 用户）")

    def load_temporal_signals(self):
        """复用时序购物篮结果：类目复购周期 + 类目级时序规则（C1 信号复用）"""
        try:
            with open(TEMPORAL_FILE, "r", encoding="utf-8") as f:
                t = json.load(f)
        except Exception:
            t = {}
        self.cycles = {c["sub_category"]: c for c in t.get("repurchase_cycles", [])}
        # 用户历史类目 → (后果类目, confidence) 聚合
        self.rules_out = {}
        for r in t.get("rules", []):
            self.rules_out.setdefault(r["antecedent"], []).append(
                (r["consequent"], r["confidence"]))

    # ---------------- 用户特征（仅训练期信息） ----------------

    def build_user_features(self, users):
        """用户级特征：类目亲和分布 + 各类目最近一次购买距切分日的天数"""
        hist = self.train[self.train["user_id"].isin(users)]
        # 类目亲和（购买次数占比）
        aff = hist.groupby(["user_id", "sub_category"]).size().reset_index(name="n")
        aff["affinity"] = aff.groupby("user_id")["n"].transform(lambda x: x / x.sum())

        # 类目最近购买距切分日的天数（用于时序周期接近度）
        last = hist.groupby(["user_id", "sub_category"])["date"].max().reset_index()
        split_dt = pd.Timestamp(str(self.split_date))
        last["days_since"] = (split_dt - last["date"]).dt.days.clip(lower=0)

        self.user_affinity = {u: dict(zip(g["sub_category"], g["affinity"]))
                              for u, g in aff.groupby("user_id")}
        self.user_last_days = {u: dict(zip(g["sub_category"], g["days_since"]))
                               for u, g in last.groupby("user_id")}

    @staticmethod
    def _cycle_closeness(days, cycle):
        """距上次购买天数与复购周期中位数的接近度 ∈ [0,1]（三角核，宽=P25~P75）"""
        if days is None:
            return 0.0
        mu = cycle.get("interval_median")
        if not mu or mu <= 0:
            return 0.0
        lo = cycle.get("interval_p25") or mu * 0.6
        hi = cycle.get("interval_p75") or mu * 1.4
        half = max(3.0, (hi - lo) / 2.0)
        return max(0.0, 1.0 - abs(days - mu) / (2.0 * half))

    # ---------------- 策略打分与曝光生成 ----------------

    def score_categories(self, uid, p):
        """类目级打分：w_pop×热度 + w_affinity×亲和 + w_co×时序接近度（+ ε 探索）"""
        aff = self.user_affinity.get(uid, {})
        days_map = self.user_last_days.get(uid, {})
        subs = list(self.sub_pop.index)
        pop = np.array([self.sub_pop[s] for s in subs], dtype=np.float64)
        pop = pop / (pop.max() if pop.max() > 0 else 1.0)
        aff_v = np.array([aff.get(s, 0.0) for s in subs])
        co_v = np.array([self._cycle_closeness(days_map.get(s), self.cycles.get(s, {}))
                         for s in subs])
        score = p["w_pop"] * pop + p["w_affinity"] * aff_v + p["w_co"] * co_v
        if p.get("epsilon", 0) > 0:
            rng = np.random.default_rng(self._uid_seed(uid) ^ 0xABCD)
            mask = rng.random(len(subs)) < p["epsilon"]
            score = score + mask * rng.random(len(subs)) * 0.5   # 受控探索
        order = np.argsort(-score)
        return [(subs[i], float(score[i])) for i in order if score[i] > 0]

    def build_exposures(self, uid, p):
        """由类目得分生成 top_k 曝光列表（类目内按训练期热度取头部商品）"""
        picks, seen, cat_count = [], set(), {}
        for sub, _ in self.score_categories(uid, p):
            for pid in self.sub_top_items.get(sub, []):
                if pid in seen:
                    continue
                if cat_count.get(sub, 0) >= self.params_slots:
                    break  # 该类目曝光位已满，进入下一类目
                picks.append((pid, sub))
                seen.add(pid)
                cat_count[sub] = cat_count.get(sub, 0) + 1
                if len(picks) >= self.top_k:
                    return picks
        return picks

    # ---------------- 响应模型（显式模拟假设） ----------------

    def simulate_response(self, uid, exposures, model_rng):
        """给定曝光列表，按响应模型生成点击/转化（Bernoulli 采样）

        p_click(item) = base_ctr × (1 + affinity_gain×affinity)
                        × (1 + co_gain×cycle_closeness) × position_decay^rank
        p_conv(item)  = base_cvr × (1 + price_fit_gain×price_fit)
        price_fit     = 商品价与用户训练期件均价的接近度（高斯核）
        """
        aff = self.user_affinity.get(uid, {})
        days_map = self.user_last_days.get(uid, {})
        avg_price = self.user_avg_price.get(uid)
        clicks, convs, rev = 0, 0, 0.0
        for rank, (pid, sub) in enumerate(exposures):
            a = aff.get(sub, 0.0)
            cc = self._cycle_closeness(days_map.get(sub), self.cycles.get(sub, {}))
            p_click = min(0.95, self.base_ctr * (1 + self.affinity_gain * a)
                          * (1 + self.co_gain * cc) * (self.position_decay ** rank))
            if model_rng.random() < p_click:
                clicks += 1
                price = self.item_price.get(pid, 0.0)
                pf = 0.0
                if avg_price and avg_price > 0 and price > 0:
                    pf = math.exp(-((math.log1p(price) - math.log1p(avg_price)) ** 2) / 0.5)
                p_conv = min(0.95, self.base_cvr * (1 + self.price_fit_gain * pf))
                if model_rng.random() < p_conv:
                    convs += 1
                    rev += price
        return clicks, convs, rev

    # ---------------- 真实锚点 ----------------

    def real_hits(self, uid, exposures):
        """测试期真实购买命中（真实锚点，无模拟成分）"""
        bought = self.test_bought.get(uid)
        if not bought:
            return 0
        return sum(1 for pid, _ in exposures if pid in bought)

    # ---------------- 主流程 ----------------

    def run(self, strategies=None, seed=42, sample_ratio=1.0, params=None, save=True):
        t0 = datetime.now()
        cfg = dict(DEFAULT_PARAMS)
        if params:
            cfg.update(params)
        self.params_cfg = cfg
        self.split_date = cfg["split_date"]
        self.top_k = cfg["top_k"]
        self.params_slots = cfg["slots_per_category"]
        self.base_ctr = cfg["base_ctr"]
        self.base_cvr = cfg["base_cvr"]
        self.affinity_gain = cfg["affinity_gain"]
        self.co_gain = cfg["co_gain"]
        self.position_decay = cfg["position_decay"]
        self.price_fit_gain = cfg["price_fit_gain"]

        strategies = strategies or PRESET_STRATEGIES

        print("=" * 70)
        print(f"A/B 模拟实验：{len(strategies)} 组策略 | seed={seed} | split={self.split_date}")
        print("=" * 70)

        self.load_data(self.split_date)
        self.load_temporal_signals()
        self.item_price = dict(pd.read_sql_query(
            "SELECT product_id, price FROM dim_product", self.conn).values)

        # 实验用户：测试期有购买且训练期有历史的用户（可评估真实锚点）
        test_users = set(self.test.groupby("user_id").size().index)
        train_users = set(self.train.groupby("user_id").size().index)
        exp_users = sorted(test_users & train_users)
        rng_u = np.random.default_rng(seed)
        if sample_ratio < 1.0:
            exp_users = sorted(rng_u.choice(exp_users,
                                            size=int(len(exp_users) * sample_ratio),
                                            replace=False).tolist())
        self.test_bought = {u: set(g) for u, g in self.test.groupby("user_id")["product_id"]}
        self.user_avg_price = (self.train.assign(up=self.train["item_amount"] / self.train["quantity"])
                               .groupby("user_id")["up"].mean().to_dict())
        self.build_user_features(exp_users)
        print(f"  实验用户: {len(exp_users)}（训练期有历史 ∩ 测试期有购买）")

        # 逐策略生成曝光 → 响应模拟 + 真实锚点（用户级配对）
        per_user = {s["key"]: {"clicks": np.zeros(len(exp_users)),
                               "convs": np.zeros(len(exp_users)),
                               "rev": np.zeros(len(exp_users)),
                               "real_hits": np.zeros(len(exp_users)),
                               "expo": np.zeros(len(exp_users)),
                               "div_subs": np.zeros(len(exp_users))}
                    for s in strategies}
        uid_index = {u: i for i, u in enumerate(exp_users)}

        for s in strategies:
            p = s["params"]
            for uid in exp_users:
                i = uid_index[uid]
                exposures = self.build_exposures(uid, p)
                c, cv, rv = self.simulate_response(
                    uid, exposures, np.random.default_rng(self._uid_seed(uid) ^ self._str_seed(s["key"], seed)))
                per_user[s["key"]]["clicks"][i] = c
                per_user[s["key"]]["convs"][i] = cv
                per_user[s["key"]]["rev"][i] = rv
                per_user[s["key"]]["real_hits"][i] = self.real_hits(uid, exposures)
                per_user[s["key"]]["expo"][i] = len(exposures)
                per_user[s["key"]]["div_subs"][i] = len({sub for _, sub in exposures})
            print(f"  [{s['key']}] 曝光/响应计算完成")

        # 汇总指标 + 配对 bootstrap 检验（vs 基线组）
        base_key = strategies[0]["key"]
        metric_arrays = {"sim_gmv_per_user": "rev", "real_hit_items_per_user": "real_hits",
                         "sim_clicks_per_user": "clicks"}
        results = []
        for s in strategies:
            k = s["key"]
            d = per_user[k]
            clicks, convs = d["clicks"].sum(), d["convs"].sum()
            expo = d["expo"].sum()
            row = {
                "key": k, "name": s["name"], "params": s["params"],
                "impressions": int(expo),
                "sim_ctr": round(clicks / expo, 5) if expo else 0,
                "sim_cvr": round(convs / clicks, 5) if clicks else 0,
                "sim_gmv": round(float(d["rev"].sum()), 2),
                "sim_gmv_per_user": round(float(d["rev"].mean()), 4),
                "sim_clicks_per_user": round(float(d["clicks"].mean()), 4),
                "real_hit_rate": round(float((d["real_hits"] > 0).mean()), 5),
                "real_hit_items_per_user": round(float(d["real_hits"].mean()), 4),
                "avg_diversity": round(float(d["div_subs"].mean()), 3),
                "simulated": True,
            }
            if k != base_key:
                b = per_user[base_key]
                for metric, field in metric_arrays.items():
                    diff = d[field] - b[field]
                    est, lo, hi, pval = self._paired_bootstrap(diff, cfg["n_bootstrap"], seed)
                    row[f"lift_{metric}"] = {
                        "vs": base_key, "abs_diff": round(float(est), 5),
                        "ci95": [round(float(lo), 5), round(float(hi), 5)],
                        "p_value": round(float(pval), 4),
                        "significant": bool(pval < 0.05),
                    }
            results.append(row)

        # 自动结论
        concl = self._conclusions(results, base_key)

        out = {
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "seed": seed,
            "sample_ratio": sample_ratio,
            "params": cfg,
            "strategies": [{"key": s["key"], "name": s["name"], "params": s["params"]}
                           for s in strategies],
            "n_users": len(exp_users),
            "train_period": [str(self.train["date"].min().date()), str(self.train["date"].max().date())],
            "test_period": [str(self.test["date"].min().date()), str(self.test["date"].max().date())],
            "results": results,
            "conclusions": concl,
            "method_note": (
                "真实锚点层（real_hit_rate 等）完全由测试期真实购买计算；"
                "模拟响应层（sim_ctr/sim_cvr/sim_gmv）由显式响应模型生成："
                "p_click = base_ctr × (1+affinity_gain×类目亲和) × (1+co_gain×周期接近度) × position_decay^rank，"
                "p_conv = base_cvr × (1+price_fit_gain×价格带匹配)。"
                "响应模型为建模假设而非数据事实；显著性为用户级配对 bootstrap 检验。"
                "同 (seed, 策略组) 配置重跑结果逐位一致。"),
        }
        if save:
            with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
                json.dump(out, f, ensure_ascii=False, indent=2)
            print(f"  结果已写入 {OUTPUT_PATH}")
        print(f"  耗时 {(datetime.now() - t0).total_seconds():.1f}s")
        return out

    # ---------------- 工具 ----------------

    @staticmethod
    def _uid_seed(uid):
        return int(hashlib.md5(str(uid).encode()).hexdigest()[:8], 16)

    @staticmethod
    def _str_seed(key, seed):
        return int(hashlib.md5(f"{key}:{seed}".encode()).hexdigest()[:8], 16)

    @staticmethod
    def _paired_bootstrap(diff, n_boot, seed):
        """配对 bootstrap：返回 (均值差, CI95, 双侧 p)"""
        rng = np.random.default_rng(seed)
        n = len(diff)
        est = float(diff.mean())
        boots = np.empty(n_boot)
        for i in range(n_boot):
            idx = rng.integers(0, n, n)
            boots[i] = diff[idx].mean()
        lo, hi = np.percentile(boots, [2.5, 97.5])
        p = 2.0 * min((boots <= 0).mean(), (boots >= 0).mean())
        return est, lo, hi, max(p, 1.0 / n_boot)

    @staticmethod
    def _conclusions(results, base_key):
        cs = []
        best = max(results, key=lambda r: r["sim_gmv_per_user"])
        cs.append(f"模拟 GMV/用户最优：{best['name']}（{best['sim_gmv_per_user']}，"
                  f"CTR {best['sim_ctr']*100:.2f}%，CVR {best['sim_cvr']*100:.1f}%）")
        for r in results[1:]:
            lk = "lift_sim_gmv_per_user"
            if lk in r:
                l = r[lk]
                sig = "显著" if l["significant"] else "不显著"
                direction = "高于" if l["abs_diff"] > 0 else "低于"
                cs.append(f"{r['name']} 相对 {base_key} {direction} "
                          f"{abs(l['abs_diff']):.3f} GMV/用户（95% CI [{l['ci95'][0]}, {l['ci95'][1]}]，"
                          f"p={l['p_value']}，{sig}）")
        rh = max(results, key=lambda r: r["real_hit_items_per_user"])
        cs.append(f"真实命中率最优：{rh['name']}（real_hit {rh['real_hit_items_per_user']*100:.2f}%/用户）"
                  "——该指标由测试期真实购买计算，不受响应模型假设影响")
        cs.append("注意：模拟层 CTR/CVR/GMV 依赖响应模型假设（建模假设，非数据事实），"
                  "论文引用时应以 real_hit_rate 等真实锚点为主要证据。")
        return cs


def main():
    sim = ABSimulator()
    out = sim.run()          # 默认三组预设策略，seed=42
    print()
    for r in out["results"]:
        print(f"  [{r['key']}] CTR={r['sim_ctr']:.4f} CVR={r['sim_cvr']:.4f} "
              f"GMV/u={r['sim_gmv_per_user']:.3f} realHit={r['real_hit_rate']:.4f} div={r['avg_diversity']}")
    print()
    for c in out["conclusions"]:
        print("  •", c)


if __name__ == "__main__":
    main()
