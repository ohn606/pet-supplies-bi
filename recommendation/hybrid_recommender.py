"""
宠物用品电商 - 混合推荐系统 (纯NumPy实现)
融合多特征的推荐算法实现与效果评估

模型:
1. User-Based Collaborative Filtering (用户协同过滤)
2. Item-Based Collaborative Filtering (物品协同过滤)  
3. Matrix Factorization (SVD矩阵分解)
4. Content-Based Filtering (基于内容推荐)
5. Hybrid Ensemble Model (混合集成模型)

评估指标: Precision, Recall, F1, Coverage, Diversity, Novelty
"""
import os
import sys
import json
import sqlite3
import math
import time
import random
from datetime import datetime
from collections import defaultdict, Counter
import warnings

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")
random.seed(42)
np.random.seed(42)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WAREHOUSE_DIR = os.path.join(BASE_DIR, "warehouse")
REC_DIR = os.path.join(BASE_DIR, "recommendation")
ANALYTICS_DIR = os.path.join(BASE_DIR, "analytics")
DB_PATH = os.path.join(WAREHOUSE_DIR, "pet_supplies_dw.db")
TEMPORAL_RULES_PATH = os.path.join(ANALYTICS_DIR, "temporal_rules.json")


# ============== 数据加载器 ==============

class DataLoader:
    def __init__(self, db_path=DB_PATH):
        self.conn = sqlite3.connect(db_path)
        
    def load_all(self):
        print("加载推荐数据...")
        self.user_item_matrix = pd.read_sql_query("""
            SELECT user_id, product_id, COUNT(DISTINCT order_id) as purchase_count,
                   SUM(quantity) as total_quantity, SUM(item_amount) as total_amount,
                   MIN(order_date_id) as first_date_id, MAX(order_date_id) as last_date_id
            FROM fact_order_items GROUP BY user_id, product_id
        """, self.conn)
        
        self.behaviors = pd.read_sql_query("""
            SELECT user_id, product_id, behavior_type, COUNT(*) as cnt
            FROM fact_user_behavior GROUP BY user_id, product_id, behavior_type
        """, self.conn)
        
        self.users = pd.read_sql_query("""
            SELECT user_id, age, gender, city_tier, pet_type, 
                   pet_age_group, income_level, activity_level,
                   consume_level, rfm_score, lifetime_orders, lifetime_amount
            FROM dim_user
        """, self.conn)
        
        self.products = pd.read_sql_query("""
            SELECT product_id, product_name, main_category, sub_category,
                   leaf_category, brand, price, quality_score,
                   total_sales, total_revenue, avg_rating, review_count
            FROM dim_product
        """, self.conn)
        
        self.reviews = pd.read_sql_query("""
            SELECT user_id, product_id, rating FROM fact_reviews
        """, self.conn)
        
        print(f"  用户: {len(self.users)}, 商品: {len(self.products)}")
        print(f"  购买记录: {len(self.user_item_matrix)}, 行为: {len(self.behaviors)}")
        # 数据终点（补货临近度参考日期）
        self.max_order_date_id = self.conn.execute(
            "SELECT MAX(order_date_id) FROM fact_order_items").fetchone()[0]
        return self
    
    def build_rating_matrix(self):
        """构建用户-商品评分矩阵 (显式+隐式反馈融合)"""
        explicit = self.reviews.groupby(["user_id", "product_id"])["rating"].mean().reset_index()
        explicit.columns = ["user_id", "product_id", "explicit_rating"]
        
        # 保留购买日期边界，供 Co-Score（时序规则特征）构建用户历史
        purchase = self.user_item_matrix.groupby(["user_id", "product_id"]).agg(
            purchase_count=("purchase_count", "sum"),
            first_date_id=("first_date_id", "min"),
            last_date_id=("last_date_id", "max")
        ).reset_index()
        
        behavior_pivot = self.behaviors.pivot_table(
            index=["user_id", "product_id"], columns="behavior_type", values="cnt", fill_value=0
        ).reset_index()
        
        matrix = purchase.merge(explicit, on=["user_id", "product_id"], how="outer")
        if not behavior_pivot.empty and "浏览" in behavior_pivot.columns:
            matrix = matrix.merge(behavior_pivot, on=["user_id", "product_id"], how="left")
        
        for col in ["explicit_rating", "purchase_count"]:
            if col in matrix.columns: matrix[col] = matrix[col].fillna(0)
        for col in ["浏览", "收藏", "加购", "搜索"]:
            if col in matrix.columns: matrix[col] = matrix[col].fillna(0).astype(int)
        
        matrix["implicit_score"] = np.minimum(matrix.get("purchase_count", 0), 5) * 0.8
        if "加购" in matrix.columns: matrix["implicit_score"] += np.minimum(matrix["加购"], 3) * 0.5
        if "收藏" in matrix.columns: matrix["implicit_score"] += np.minimum(matrix["收藏"], 3) * 0.3
        if "浏览" in matrix.columns: matrix["implicit_score"] += np.minimum(matrix["浏览"] / 5, 2) * 0.1
        
        matrix["rating"] = np.where(
            matrix["explicit_rating"] > 0,
            matrix["explicit_rating"],
            np.minimum(matrix["implicit_score"], 5.0)
        )
        return matrix


# ============== 工具函数 ==============

def cosine_sim(a, b):
    """两个向量的余弦相似度"""
    dot = np.dot(a, b)
    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)

def pearson_sim(a, b):
    """两个向量的皮尔逊相关系数"""
    a_centered = a - np.mean(a)
    b_centered = b - np.mean(b)
    dot = np.dot(a_centered, b_centered)
    norm_a = np.linalg.norm(a_centered)
    norm_b = np.linalg.norm(b_centered)
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


# ============== 时序购物篮规则特征 (Co-Score) ==============

class CoScoreFeature:
    """时序关联规则 → Co-Score 显式特征（C3 创新点核心）

    对 (user, item) 计算协同购买置信度得分 ∈ [0, 1]：
    1. 跨类目时序规则：用户买过 A 且距上次购买天数接近规则中位间隔
       → confidence × 高斯接近度衰减；
    2. 同类目复购周期：距上次购买该类目的天数接近消耗周期中位数
       → rebuy_rate × 高斯接近度衰减。
    特征仅由训练集历史构建（避免测试信息泄漏）。

    注意：真实数据中存在大量"早已超出补货窗口"的用户，纯高斯核会使
    得分趋近 0（信息等死）。因此引入 TIME_DECAY_FLOOR：窗口外保留
    长期品类亲和基线（α × 规则置信度），窗口内再叠加时序增益。
    """

    def __init__(self, rules_path=TEMPORAL_RULES_PATH):
        with open(rules_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.rules_by_consequent = defaultdict(list)
        for r in data.get("rules", []):
            self.rules_by_consequent[r["consequent"]].append(r)
        self.cycles = {c["sub_category"]: c for c in data.get("repurchase_cycles", [])}
        self._ref_dt = None
        self._days_cache = {}

    def fit(self, interactions, products, ref_date_id):
        """interactions 须含 last_date_id 列（训练集口径）"""
        self.item_sub = dict(zip(products["product_id"], products["sub_category"]))
        from datetime import datetime as _dt
        self._ref_dt = _dt.strptime(str(int(ref_date_id)), "%Y%m%d")
        self._days_cache = {}
        self.user_history = {}   # user -> {sub: last_date_id}
        for row in interactions.itertuples(index=False):
            s = self.item_sub.get(row.product_id)
            d = getattr(row, "last_date_id", None)
            if s is None or d is None:
                continue
            try:
                if pd.isna(d):
                    continue
            except TypeError:
                pass
            d = int(d)
            self._days_cache[d] = None  # 占位，惰性转换
            hist = self.user_history.setdefault(row.user_id, {})
            if s not in hist or d > hist[s]:
                hist[s] = d
        return self

    def _to_dt(self, date_id):
        v = self._days_cache.get(date_id)
        if v is None:
            from datetime import datetime as _dt
            v = _dt.strptime(str(date_id), "%Y%m%d")
            self._days_cache[date_id] = v
        return v

    def _days_since(self, date_id):
        return max(0, (self._ref_dt - self._to_dt(date_id)).days)

    # 窗口外的长期品类亲和下限（防止信息等死，见类注释）
    TIME_DECAY_FLOOR = 0.15

    @staticmethod
    def _gauss(x, mu, sigma):
        return math.exp(-((x - mu) ** 2) / (2.0 * sigma * sigma))

    def _proximity(self, days, mu, sigma):
        return max(self._gauss(days, mu, sigma), self.TIME_DECAY_FLOOR)

    def user_sub_scores(self, user_id):
        """用户对各二级类目的 Co-Score（类目仅 14 个，开销可忽略）"""
        hist = self.user_history.get(user_id)
        if not hist:
            return {}
        out = {}
        for target_sub, rules in self.rules_by_consequent.items():
            best = 0.0
            for r in rules:
                a = r["antecedent"]
                if a not in hist:
                    continue
                days = self._days_since(hist[a])
                sigma = max(7.0, (r["gap_p75"] - r["gap_p25"]) / 2.0)
                v = r["confidence"] * self._proximity(days, r["gap_median"], sigma)
                if v > best:
                    best = v
            if best > 0:
                out[target_sub] = best
        for s, d in hist.items():
            cyc = self.cycles.get(s)
            if not cyc or cyc["cycle_median"] <= 0:
                continue
            days = self._days_since(d)
            sigma = max(7.0, (cyc["cycle_p75"] - cyc["cycle_p25"]) / 2.0)
            v = cyc["rebuy_rate"] * self._proximity(days, cyc["cycle_median"], sigma)
            out[s] = max(out.get(s, 0.0), v)
        return out

    def coverage_stats(self, interactions):
        """训练集 (user, item) 对中 Co-Score > 0 的占比（特征覆盖率）"""
        n, hit = 0, 0
        cache = {}
        for row in interactions.itertuples(index=False):
            n += 1
            if row.user_id not in cache:
                cache[row.user_id] = self.user_sub_scores(row.user_id)
            if cache[row.user_id].get(self.item_sub.get(row.product_id), 0) > 0:
                hit += 1
        return {"pairs": n, "covered": hit, "coverage": round(hit / n, 4) if n else 0}


# ============== 推荐算法 ==============

class UserBasedCF:
    """基于用户的协同过滤"""
    
    def __init__(self, k_neighbors=40):
        self.name = "User-Based CF"
        self.k = k_neighbors
        
    def fit(self, interactions, products=None, users=None):
        print(f"  [{self.name}] 训练中...")
        self.user_ids = interactions["user_id"].unique()
        self.item_ids = interactions["product_id"].unique()
        
        self.u2i = {u: i for i, u in enumerate(self.user_ids)}
        self.i2p = {i: p for i, p in enumerate(self.item_ids)}
        self.p2i = {p: i for i, p in enumerate(self.item_ids)}
        
        self.R = np.zeros((len(self.user_ids), len(self.item_ids)))
        for _, row in interactions.iterrows():
            u = self.u2i.get(row["user_id"])
            p = self.p2i.get(row["product_id"])
            if u is not None and p is not None:
                self.R[u][p] = row["rating"]
        
        # 计算用户相似度
        self.n_users = len(self.user_ids)
        max_users = min(self.n_users, 2000)
        print(f"  [{self.name}] 计算相似度矩阵 ({max_users}x{max_users})...")
        
        self.sim = np.zeros((max_users, max_users))
        R_sub = self.R[:max_users]
        means = np.mean(R_sub, axis=1, keepdims=True)
        centered = R_sub - means
        centered = np.nan_to_num(centered)
        norms = np.linalg.norm(centered, axis=1, keepdims=True)
        norms[norms == 0] = 1e-8
        normalized = centered / norms
        self.sim = np.dot(normalized, normalized.T)
        np.fill_diagonal(self.sim, 0)
        
        # Top-K
        for i in range(max_users):
            row = self.sim[i]
            top_k = min(self.k, max_users)
            indices = np.argpartition(row, -top_k)[-top_k:]
            mask = np.ones(max_users, dtype=bool)
            mask[indices] = False
            self.sim[i][mask] = 0
        
        print(f"  [{self.name}] 训练完成")
    
    def recommend(self, user_id, top_k=10):
        if user_id not in self.u2i: return []
        u = self.u2i[user_id]
        max_u = min(self.n_users, 2000)
        if u >= max_u: return []
        
        interacted = set(np.where(self.R[u] > 0)[0])
        
        # 向量化: scores = similarity @ rating_matrix
        sim_weights = self.sim[u][:max_u].copy()
        sim_weights[u] = 0
        sim_weights[sim_weights < 0] = 0
        
        scores = np.dot(sim_weights, self.R[:max_u])
        scores = scores[:len(self.item_ids)]
        
        # 归一化
        sim_sum = np.abs(self.sim[u][:max_u]).sum()
        if sim_sum > 0:
            scores = scores / sim_sum
        
        scores[list(interacted)] = -999
        top = np.argsort(scores)[::-1][:top_k]
        return [self.i2p[i] for i in top if scores[i] > 0]


class ItemBasedCF:
    """基于物品的协同过滤"""
    
    def __init__(self, k_neighbors=30):
        self.name = "Item-Based CF"
        self.k = k_neighbors
        
    def fit(self, interactions, products=None, users=None):
        print(f"  [{self.name}] 训练中...")
        self.user_ids = interactions["user_id"].unique()
        self.item_ids = interactions["product_id"].unique()
        
        self.u2i = {u: i for i, u in enumerate(self.user_ids)}
        self.p2i = {p: i for i, p in enumerate(self.item_ids)}
        self.i2p = {i: p for i, p in enumerate(self.item_ids)}
        
        self.R = np.zeros((len(self.user_ids), len(self.item_ids)))
        for _, row in interactions.iterrows():
            u = self.u2i.get(row["user_id"])
            p = self.p2i.get(row["product_id"])
            if u is not None and p is not None:
                self.R[u][p] = row["rating"]
        
        self.n_users = len(self.user_ids)
        self.n_items = len(self.item_ids)
        max_items = min(self.n_items, 1500)
        
        print(f"  [{self.name}] 计算物品相似度 ({max_items}x{max_items})...")
        item_matrix = self.R[:, :max_items].T
        item_centered = item_matrix - np.mean(item_matrix, axis=1, keepdims=True)
        item_centered = np.nan_to_num(item_centered)
        norms = np.linalg.norm(item_centered, axis=1, keepdims=True)
        norms[norms == 0] = 1e-8
        item_norm = item_centered / norms
        self.item_sim = np.dot(item_norm, item_norm.T)
        np.fill_diagonal(self.item_sim, 0)
        
        for i in range(max_items):
            row = self.item_sim[i]
            top = min(self.k, max_items)
            indices = np.argpartition(row, -top)[-top:]
            mask = np.ones(max_items, dtype=bool)
            mask[indices] = False
            self.item_sim[i][mask] = 0
        
        self.max_items = max_items
        print(f"  [{self.name}] 训练完成")
    
    def recommend(self, user_id, top_k=10):
        if user_id not in self.u2i: return []
        u = self.u2i[user_id]
        max_items = self.max_items
        
        interacted = set(np.where(self.R[u][:max_items] > 0)[0])
        
        # 向量化预测: scores = sim_matrix @ user_ratings
        user_ratings = self.R[u][:max_items]
        sim_mat = self.item_sim[:max_items, :max_items]
        scores = np.dot(sim_mat, user_ratings)  # O(max_items^2) but vectorized in C
        
        # 归一化（除以相似度绝对值之和）
        sim_abs_sum = np.abs(sim_mat).sum(axis=1)
        sim_abs_sum[sim_abs_sum == 0] = 1
        scores = scores / sim_abs_sum
        
        scores[list(interacted)] = -999
        top = np.argsort(scores)[::-1][:top_k]
        return [self.i2p[i] for i in top if scores[i] > 0 and i not in interacted]


class SVDRecommender:
    """
    SVD矩阵分解推荐 (随机梯度下降实现)
    P: 用户潜在因子矩阵 (n_users × n_factors)
    Q: 物品潜在因子矩阵 (n_items × n_factors)
    """
    
    def __init__(self, n_factors=40, lr=0.005, reg=0.02, n_epochs=20):
        self.name = "SVD Matrix Factorization"
        self.n_factors = n_factors
        self.lr = lr
        self.reg = reg
        self.n_epochs = n_epochs
        
    def fit(self, interactions, products=None, users=None):
        print(f"  [{self.name}] 训练中 (factors={self.n_factors}, epochs={self.n_epochs})...")
        
        self.user_ids = interactions["user_id"].unique()
        self.item_ids = interactions["product_id"].unique()
        
        self.u2i = {u: i for i, u in enumerate(self.user_ids)}
        self.p2i = {p: i for i, p in enumerate(self.item_ids)}
        self.i2p = {i: p for i, p in enumerate(self.item_ids)}
        
        n_users = len(self.user_ids)
        n_items = len(self.item_ids)
        
        # 初始化潜在因子矩阵
        self.global_mean = interactions["rating"].mean()
        self.user_bias = np.zeros(n_users)
        self.item_bias = np.zeros(n_items)
        self.P = np.random.normal(0, 0.1, (n_users, self.n_factors))
        self.Q = np.random.normal(0, 0.1, (n_items, self.n_factors))
        
        # 构建训练数据
        self.train_data = []
        for _, row in interactions.iterrows():
            u = self.u2i.get(row["user_id"])
            p = self.p2i.get(row["product_id"])
            if u is not None and p is not None:
                self.train_data.append((u, p, row["rating"]))
        
        # SGD训练
        for epoch in range(self.n_epochs):
            np.random.shuffle(self.train_data)
            total_loss = 0
            
            for u, p, r in self.train_data:
                # 预测
                pred = self.global_mean + self.user_bias[u] + self.item_bias[p] + np.dot(self.P[u], self.Q[p])
                err = r - pred
                total_loss += err ** 2
                
                # 更新
                self.user_bias[u] += self.lr * (err - self.reg * self.user_bias[u])
                self.item_bias[p] += self.lr * (err - self.reg * self.item_bias[p])
                
                pu_old = self.P[u].copy()
                self.P[u] += self.lr * (err * self.Q[p] - self.reg * self.P[u])
                self.Q[p] += self.lr * (err * pu_old - self.reg * self.Q[p])
            
            rmse = math.sqrt(total_loss / len(self.train_data))
            print(f"  [{self.name}] Epoch {epoch+1}/{self.n_epochs}, RMSE={rmse:.4f}")
        
        print(f"  [{self.name}] 训练完成, 最终RMSE={rmse:.4f}")
    
    def recommend(self, user_id, top_k=10):
        if user_id not in self.u2i: return []
        u = self.u2i[user_id]
        
        # 预测所有物品的评分
        preds = self.global_mean + self.user_bias[u] + self.item_bias + np.dot(self.P[u], self.Q.T)
        
        # 过滤已交互物品
        self._build_interaction_set()
        if u in self.interacted:
            preds[list(self.interacted[u])] = -999
        
        top = np.argsort(preds)[::-1][:top_k]
        return [self.i2p[i] for i in top if preds[i] > 0]
    
    def _build_interaction_set(self):
        if hasattr(self, 'interacted'): return
        self.interacted = defaultdict(set)
        for u, p, r in self.train_data:
            self.interacted[u].add(p)


class ContentBasedRecommender:
    """基于内容的推荐 - 利用商品属性"""
    
    def __init__(self):
        self.name = "Content-Based"
        
    def fit(self, interactions, products=None, users=None):
        print(f"  [{self.name}] 训练中...")
        
        self.product_ids = products["product_id"].values.tolist() if products is not None else interactions["product_id"].unique().tolist()
        
        # 构建商品特征矩阵
        n_items = len(self.product_ids)
        self.pid_to_idx = {p: i for i, p in enumerate(self.product_ids)}
        
        feat_dim = 20
        self.item_features = np.zeros((n_items, feat_dim))
        
        if products is not None:
            product_idx_map = {p: i for i, p in enumerate(products["product_id"])}
            
            for i, pid in enumerate(self.product_ids):
                if pid in product_idx_map:
                    pi = product_idx_map[pid]
                    row = products.iloc[pi]
                    
                    # 特征编码
                    feat_idx = 0
                    # 价格 (归一化)
                    if "price" in products.columns:
                        max_price = products["price"].max()
                        self.item_features[i, 0] = row["price"] / max(max_price, 1)
                    # 评分
                    if "avg_rating" in products.columns:
                        self.item_features[i, 1] = row.get("avg_rating", 3.0) / 5.0
                    # 质量
                    if "quality_score" in products.columns:
                        self.item_features[i, 2] = row.get("quality_score", 3.0) / 5.0
                    
                    # 类别哈希编码
                    hash_dim = 10
                    cat_str = f"{row.get('main_category','')}_{row.get('sub_category','')}_{row.get('brand','')}"
                    for j, ch in enumerate(cat_str[:hash_dim * 2]):
                        if j % 2 == 0 and j // 2 + 3 < feat_dim:
                            self.item_features[i, j // 2 + 3] = (ord(ch) / 255.0)
        else:
            self.item_features = np.random.randn(n_items, feat_dim)
        
        # 构建用户画像
        self.user_profiles = {}
        for uid, group in interactions.groupby("user_id"):
            pidx_list = [self.pid_to_idx.get(p) for p in group["product_id"] if self.pid_to_idx.get(p) is not None]
            if pidx_list:
                ratings = group["rating"].values[:len(pidx_list)]
                weights = ratings / (ratings.sum() + 1e-8)
                profile = np.average(self.item_features[pidx_list], axis=0, weights=weights)
                self.user_profiles[uid] = profile
        
        print(f"  [{self.name}] 训练完成, 用户画像: {len(self.user_profiles)}")
    
    def recommend(self, user_id, top_k=10):
        if user_id not in self.user_profiles:
            # 冷启动：随机推荐热门商品
            popular = list(self.product_ids[:min(top_k, len(self.product_ids))])
            return popular
        
        user_vec = self.user_profiles[user_id].reshape(1, -1)
        dot = np.dot(user_vec, self.item_features.T)[0]
        norms_user = np.linalg.norm(user_vec)
        norms_items = np.linalg.norm(self.item_features, axis=1)
        norms_items[norms_items == 0] = 1e-8
        scores = dot / (norms_user * norms_items)
        
        top = np.argsort(scores)[::-1][:top_k + 50]
        return [self.product_ids[i] for i in top[:top_k]]


class PopularityRecommender:
    """基于流行度的基线推荐器"""

    def __init__(self):
        self.name = "Popularity Baseline"

    def fit(self, interactions, products=None, users=None):
        self.popularity = interactions["product_id"].value_counts().to_dict()
        self.popular_items = sorted(self.popularity, key=self.popularity.get, reverse=True)

    def recommend(self, user_id, top_k=10):
        return self.popular_items[:top_k]


class RulesBoostRecommender:
    """第 7 模型：时序规则增强推荐（Co-Score 注入 · v2 新增）

    将时序购物篮规则作为新信号注入：score = 0.3×Co-Score + 0.7×热度归一分
    （权重经 {0.3,0.5,0.7,0.9} 网格扫描选定），过滤用户已购商品；
    无规则信号的用户回退热度榜（不虚构信号）。
    """

    def __init__(self, co_weight=0.3, cos_feature=None):
        self.name = "RulesBoost (Temporal)"
        self.co_weight = co_weight
        self._cos = cos_feature

    def fit(self, interactions, products=None, users=None, ref_date_id=None):
        if self._cos is None:
            if ref_date_id is None and "last_date_id" in interactions.columns:
                ref_date_id = interactions["last_date_id"].max()
            self._cos = CoScoreFeature()
            self._cos.fit(interactions, products, ref_date_id)
        self.item_sub = self._cos.item_sub
        # 热度榜（dim_product.total_sales，全量真实口径）
        pop = products.set_index("product_id")["total_sales"].astype(float)
        self.pop_norm = (pop / max(pop.max(), 1)).to_dict()
        self.product_ids = list(pop.sort_values(ascending=False).index)
        self.bought = interactions.groupby("user_id")["product_id"].apply(set).to_dict()
        self._cache = {}
        return self

    def _sub_scores(self, uid):
        if uid not in self._cache:
            self._cache[uid] = self._cos.user_sub_scores(uid)
        return self._cache[uid]

    def recommend(self, user_id, top_k=10):
        bought = self.bought.get(user_id, set())
        sub_scores = self._sub_scores(user_id)
        cand = []
        if sub_scores:
            for pid in self.product_ids:
                if pid in bought:
                    continue
                s = sub_scores.get(self.item_sub.get(pid), 0.0)
                if s <= 0:
                    continue
                score = self.co_weight * s + (1 - self.co_weight) * self.pop_norm.get(pid, 0.0)
                cand.append((score, pid))
            cand.sort(key=lambda x: -x[0])
        ids = [p for _, p in cand[:top_k]]
        if len(ids) < top_k:  # 热度兜底补齐
            idset = set(ids)
            for p in self.product_ids:
                if p not in bought and p not in idset:
                    ids.append(p)
                if len(ids) >= top_k:
                    break
        return ids


class FMRecommender:
    """简化因子分解机（纯 NumPy 全批 Adam · v2 新增）

    pred = sigmoid( w0 + user_lin[u] + item_lin[i] + <vU, vI>
                    + co × ( w_co + <v_co, vU> + <v_co, vI> ) )

    - 线性层：用户画像（宠物类型/消费层级/订单与金额对数）+ 商品内容
      （二级/一级类目 one-hot、价格对数、评分、热度）归一化数值特征
    - 二阶交叉：user×item 隐因子交叉（MF 通道）+ Co-Score×(user/item)
      显式规则交叉（数仓知识注入通道）
    - 变体开关 use_co / use_mf 供消融实验
    """

    def __init__(self, n_factors=16, lr=0.05, n_iters=200, l2=1e-5,
                 use_co=True, use_mf=True, name=None):
        self.name = name or ("FM (Full)" if use_co and use_mf else
                             "FM (w/o Co-Score)" if use_mf else
                             "FM (w/o MF-ID)" if use_co else "FM (Content-Only)")
        self.k = n_factors
        self.lr = lr
        self.n_iters = n_iters
        self.l2 = l2
        self.use_co = use_co
        self.use_mf = use_mf

    # ---- 特征工程 ----

    def _build_features(self, interactions, products, users):
        # 商品线性特征
        prods = products.reset_index(drop=True)
        self.item_ids = prods["product_id"].tolist()
        self.item_index = {p: i for i, p in enumerate(self.item_ids)}
        self.item_sub = dict(zip(prods["product_id"], prods["sub_category"]))
        subs = sorted(prods["sub_category"].dropna().unique())
        mcats = sorted(prods["main_category"].dropna().unique())
        self.sub_index = {s: i for i, s in enumerate(subs)}
        self.mcat_index = {m: i for i, m in enumerate(mcats)}
        n_items = len(prods)
        X_item = np.zeros((n_items, len(subs) + len(mcats) + 3), dtype=np.float32)
        max_sales = max(prods["total_sales"].max(), 1)
        for r in prods.itertuples(index=False):
            i = self.item_index[r.product_id]
            j = self.sub_index.get(r.sub_category)
            if j is not None:
                X_item[i, j] = 1.0
            j = self.mcat_index.get(r.main_category)
            if j is not None:
                X_item[i, len(subs) + j] = 1.0
            o = len(subs) + len(mcats)
            X_item[i, o] = math.log1p(float(r.price) if r.price else 0)
            X_item[i, o + 1] = float(r.avg_rating or 0) / 5.0
            X_item[i, o + 2] = math.log1p(float(r.total_sales or 0)) / math.log1p(max_sales)
        self.X_item = X_item
        self.item_sub_idx = np.array(
            [self.sub_index.get(self.item_sub.get(p), -1) for p in self.item_ids])

        # 用户线性特征（宠物类型 / 消费层级 one-hot + 订单金额对数）
        ufeat = users.set_index("user_id")
        pets = sorted(ufeat["pet_type"].dropna().unique())
        consumes = sorted(ufeat["consume_level"].dropna().unique())
        self.pet_index = {p: i for i, p in enumerate(pets)}
        self.consume_index = {c: i for i, c in enumerate(consumes)}
        train_users = sorted(interactions["user_id"].unique())
        self.user_ids = train_users
        self.user_index = {u: i for i, u in enumerate(train_users)}
        max_orders = max(float(ufeat["lifetime_orders"].max()), 1)
        max_amount = max(float(ufeat["lifetime_amount"].max()), 1)
        X_user = np.zeros((len(train_users), len(pets) + len(consumes) + 2), dtype=np.float32)
        for u in train_users:
            i = self.user_index[u]
            if u in ufeat.index:
                row = ufeat.loc[u]
                j = self.pet_index.get(row["pet_type"])
                if j is not None:
                    X_user[i, j] = 1.0
                j = self.consume_index.get(row["consume_level"])
                if j is not None:
                    X_user[i, len(pets) + j] = 1.0
                o = len(pets) + len(consumes)
                X_user[i, o] = math.log1p(float(row["lifetime_orders"] or 0)) / math.log1p(max_orders)
                X_user[i, o + 1] = math.log1p(float(row["lifetime_amount"] or 0)) / math.log1p(max_amount)
        self.X_user = X_user

    # ---- 训练 ----

    def fit(self, interactions, products=None, users=None, ref_date_id=None,
            cos_feature=None, neg_ratio=1.0, seed=42):
        print(f"  [{self.name}] 构建特征与训练样本...")
        self._build_features(interactions, products, users)

        # Co-Score（仅训练集历史）
        if self.use_co:
            if cos_feature is None:
                if ref_date_id is None and "last_date_id" in interactions.columns:
                    ref_date_id = interactions["last_date_id"].max()
                cos_feature = CoScoreFeature()
                cos_feature.fit(interactions, products, ref_date_id)
            self._cos = cos_feature
            sub_scores = {}
            def co_of(uid, pid):
                if uid not in sub_scores:
                    sub_scores[uid] = cos_feature.user_sub_scores(uid)
                return sub_scores[uid].get(cos_feature.item_sub.get(pid), 0.0)
        else:
            self._cos = None
            co_of = lambda uid, pid: 0.0

        # 正负样本（负例 = 用户未交互过的随机商品，1:1）
        rng = np.random.default_rng(seed)
        bought = interactions.groupby("user_id")["product_id"].apply(set).to_dict()
        n_items = len(self.item_ids)
        pos_u, pos_i, pos_co = [], [], []
        for r in interactions.itertuples(index=False):
            ui, ii = self.user_index.get(r.user_id), self.item_index.get(r.product_id)
            if ui is None or ii is None:
                continue
            pos_u.append(ui); pos_i.append(ii); pos_co.append(co_of(r.user_id, r.product_id))
        n_pos = len(pos_u)
        n_neg = int(n_pos * neg_ratio)
        neg_u = rng.choice(len(self.user_ids), size=n_neg)
        neg_i, neg_co = [], []
        for ui in neg_u:
            uid = self.user_ids[ui]
            excl = bought.get(uid, set())
            for _ in range(10):
                cand = int(rng.integers(n_items))
                if self.item_ids[cand] not in excl:
                    break
            neg_i.append(cand)
            neg_co.append(co_of(uid, self.item_ids[cand]))
        u_idx = np.array(pos_u + list(neg_u), dtype=np.int64)
        i_idx = np.array(pos_i + neg_i, dtype=np.int64)
        co = np.array(pos_co + neg_co, dtype=np.float32)
        y = np.array([1.0] * n_pos + [0.0] * n_neg, dtype=np.float32)
        print(f"  [{self.name}] 样本: 正例 {n_pos} + 负例 {n_neg}")

        # 参数初始化
        rng2 = np.random.default_rng(seed)
        k = self.k
        self.w0 = 0.0
        self.wu = np.zeros(self.X_user.shape[1], dtype=np.float32)
        self.wi = np.zeros(self.X_item.shape[1], dtype=np.float32)
        self.VU = (rng2.standard_normal((len(self.user_ids), k)) * 0.05).astype(np.float32)
        self.VI = (rng2.standard_normal((n_items, k)) * 0.05).astype(np.float32)
        self.vco = (rng2.standard_normal(k) * 0.05).astype(np.float32)
        self.wco = 0.0

        # Adam 状态
        def mk(p):
            return np.zeros_like(p)
        m = {
            "w0": 0.0, "wu": mk(self.wu), "wi": mk(self.wi),
            "VU": mk(self.VU), "VI": mk(self.VI), "vco": mk(self.vco), "wco": 0.0,
        }
        v = {k2: (0.0 if np.isscalar(m[k2]) else np.zeros_like(m[k2])) for k2 in m}
        b1, b2, eps, lr = 0.9, 0.999, 1e-8, self.lr
        n = len(y)

        # 预聚合线性贡献
        ulin_all = self.X_user @ self.wu   # 随参数更新重算
        ilin_all = self.X_item @ self.wi

        for it in range(self.n_iters):
            ulin = ulin_all[u_idx]
            ilin = ilin_all[i_idx]
            vu = self.VU[u_idx]
            vi = self.VI[i_idx]
            mf = np.einsum("sk,sk->s", vu, vi) if self.use_mf else 0.0
            if self.use_co:
                cocross = co * (self.wco + (vu @ self.vco + vi @ self.vco if self.use_mf else 0.0))
            else:
                cocross = 0.0
            logits = self.w0 + ulin + ilin + mf + cocross
            p = 1.0 / (1.0 + np.exp(-np.clip(logits, -30, 30)))
            e = (p - y) / n

            gw0 = float(e.sum())
            # 样本梯度聚合（bincount，C 级速度）
            e_user = np.bincount(u_idx, weights=e, minlength=len(self.user_ids))
            e_item = np.bincount(i_idx, weights=e, minlength=n_items)
            dwu = self.X_user.T @ e_user
            dwi = self.X_item.T @ e_item

            if self.use_mf:
                avec = vu + (co[:, None] * self.vco if self.use_co else 0.0)
                bvec = vi + (co[:, None] * self.vco if self.use_co else 0.0)
                Wa = e[:, None] * avec   # → dVU
                Wb = e[:, None] * bvec   # → dVI
                dVU = np.stack([np.bincount(u_idx, weights=Wa[:, kk],
                                            minlength=len(self.user_ids)) for kk in range(k)], axis=1)
                dVI = np.stack([np.bincount(i_idx, weights=Wb[:, kk],
                                            minlength=n_items) for kk in range(k)], axis=1)
            else:
                dVU = np.zeros_like(self.VU)
                dVI = np.zeros_like(self.VI)
            if self.use_co:
                gwco = float((e * co).sum())
                dvco = (((vu + vi) * co[:, None]).T @ e) if self.use_mf else np.zeros_like(self.vco)
            else:
                dvco = np.zeros_like(self.vco)
                gwco = 0.0

            # L2 正则（仅隐向量）
            grads = {"w0": gw0, "wu": dwu, "wi": dwi, "VU": dVU + self.l2 * self.VU,
                     "VI": dVI + self.l2 * self.VI, "vco": dvco + self.l2 * self.vco,
                     "wco": gwco}
            params = {"w0": self.w0, "wu": self.wu, "wi": self.wi,
                      "VU": self.VU, "VI": self.VI, "vco": self.vco, "wco": self.wco}
            t = it + 1
            for key in params:
                g = grads[key]
                m[key] = b1 * m[key] + (1 - b1) * g
                v[key] = b2 * v[key] + (1 - b2) * (g * g if not np.isscalar(g) else g * g)
                mh = m[key] / (1 - b1 ** t)
                vh = v[key] / (1 - b2 ** t)
                params[key] = params[key] - lr * mh / (np.sqrt(vh) + eps)
            self.w0, self.wu, self.wi = params["w0"], params["wu"], params["wi"]
            self.VU, self.VI, self.vco, self.wco = params["VU"], params["VI"], params["vco"], params["wco"]
            ulin_all = self.X_user @ self.wu
            ilin_all = self.X_item @ self.wi
            if (it + 1) % 100 == 0:
                loss = -np.mean(y * np.log(np.clip(p, 1e-7, 1)) +
                                (1 - y) * np.log(np.clip(1 - p, 1e-7, 1)))
                print(f"    iter {it+1}/{self.n_iters}, logloss={loss:.4f}")
        self._sub_cache = {}
        self.bought = interactions.groupby("user_id")["product_id"].apply(set).to_dict()
        print(f"  [{self.name}] 训练完成")
        return self

    # ---- 推理 ----

    def score_items(self, user_id, item_list):
        """返回 (logit) 分数数组，供 AUC/排序使用"""
        ui = self.user_index.get(user_id)
        if ui is None:
            return np.zeros(len(item_list), dtype=np.float32)
        idxs = np.array([self.item_index.get(p, -1) for p in item_list], dtype=np.int64)
        valid = idxs >= 0
        scores = np.zeros(len(item_list), dtype=np.float32)
        if valid.any():
            ii = idxs[valid]
            s = self.w0 + float(self.X_user[ui] @ self.wu) + self.X_item[ii] @ self.wi
            if self.use_mf:
                s = s + self.VI[ii] @ self.VU[ui]
            if self.use_co and self._cos is not None:
                sub_scores = self._user_subs(user_id)
                co_arr = np.array(
                    [sub_scores.get(self.item_sub.get(p), 0.0) for p in item_list],
                    dtype=np.float32)[valid]
                cocross = self.wco
                if self.use_mf:
                    cocross = cocross + float(self.VU[ui] @ self.vco) + self.VI[ii] @ self.vco
                s = s + co_arr * cocross
            scores[valid] = s
        return scores

    def _user_subs(self, uid):
        if uid not in self._sub_cache:
            self._sub_cache[uid] = self._cos.user_sub_scores(uid) if self._cos else {}
        return self._sub_cache[uid]

    def recommend(self, user_id, top_k=10):
        scores = self.score_items(user_id, self.item_ids)
        order = np.argsort(-scores)
        bought = self.bought.get(user_id, set()) if hasattr(self, "bought") else set()
        out = []
        for i in order:
            pid = self.item_ids[i]
            if pid in bought:
                continue
            out.append(pid)
            if len(out) >= top_k:
                break
        return out


class HybridRecommender:
    """混合推荐系统 - 加权融合"""
    
    def __init__(self, mode="weighted"):
        self.name = "Hybrid Ensemble"
        self.models = []
        self.weights = []
        self.mode = mode
        
    def add_model(self, model, weight=1.0):
        self.models.append(model)
        self.weights.append(weight)
        
    def fit(self, interactions, products=None, users=None):
        print(f"  [{self.name}] 训练 {len(self.models)} 个子模型...")
        for model in self.models:
            model.fit(interactions, products, users)
        self.user_ids = interactions["user_id"].unique()
        print(f"  [{self.name}] 融合完成")
    
    def recommend(self, user_id, top_k=10):
        all_scores = defaultdict(float)
        
        for model, w in zip(self.models, self.weights):
            recs = model.recommend(user_id, top_k=top_k * 3)
            if recs:
                for rank, item_id in enumerate(recs):
                    all_scores[item_id] += w * (1.0 / (rank + 1))
        
        sorted_items = sorted(all_scores.items(), key=lambda x: -x[1])
        return [item for item, _ in sorted_items[:top_k]]


# ============== 评估系统 ==============

class RecommenderEvaluator:
    """推荐系统全面评估"""
    
    def __init__(self):
        self.results = {}
    
    def train_test_split(self, interactions, test_ratio=0.2):
        """按用户划分训练/测试集（留出最近交互）"""
        train_list, test_list = [], []
        
        for uid, group in interactions.groupby("user_id"):
            if len(group) >= 5:
                n_test = max(1, int(len(group) * test_ratio))
                sorted_group = group.sort_values("rating", ascending=False)
                test = sorted_group.head(n_test)
                train = group.drop(test.index)
                train_list.append(train)
                test_list.append(test)
        
        train = pd.concat(train_list, ignore_index=True) if train_list else interactions
        test = pd.concat(test_list, ignore_index=True) if test_list else interactions
        return train, test
    
    def evaluate(self, recommender, test_data, train_data, top_k=10):
        """评估推荐器"""
        print(f"\n{'='*60}")
        print(f"评估: {recommender.name} (top_k={top_k})")
        print(f"{'='*60}")
        
        test_users = test_data["user_id"].unique()
        test_positives = defaultdict(set)
        for _, row in test_data.iterrows():
            test_positives[row["user_id"]].add(row["product_id"])
        
        metrics = {"precisions": [], "recalls": [], "hits": []}
        all_recommended = set()
        n_users = 0
        
        for uid in test_users[:100]:  # 采样评估
            recs = recommender.recommend(uid, top_k)
            if not recs: continue
            
            n_users += 1
            all_recommended.update(recs)
            relevant = set(recs) & test_positives.get(uid, set())
            n_relevant = len(test_positives.get(uid, []))
            
            metrics["precisions"].append(len(relevant) / top_k)
            metrics["recalls"].append(len(relevant) / max(n_relevant, 1))
            metrics["hits"].append(1 if len(relevant) > 0 else 0)
        
        avg = {}
        avg["precision"] = np.mean(metrics["precisions"]) if metrics["precisions"] else 0
        avg["recall"] = np.mean(metrics["recalls"]) if metrics["recalls"] else 0
        avg["f1"] = 2 * avg["precision"] * avg["recall"] / (avg["precision"] + avg["recall"]) if (avg["precision"] + avg["recall"]) > 0 else 0
        avg["hit_rate"] = np.mean(metrics["hits"]) if metrics["hits"] else 0
        
        # 覆盖率
        all_items = set(test_data["product_id"].unique())
        avg["coverage"] = len(all_recommended) / len(all_items) if all_items else 0
        
        # 多样性
        avg["diversity"] = self._calc_diversity(recommender, test_users[:200], top_k)
        # 新颖性
        avg["novelty"] = self._calc_novelty(recommender, test_users[:200], top_k, train_data)
        
        print(f"  Precision@{top_k}: {avg['precision']:.4f}")
        print(f"  Recall@{top_k}:    {avg['recall']:.4f}")
        print(f"  F1@{top_k}:        {avg['f1']:.4f}")
        print(f"  Hit Rate@{top_k}:  {avg['hit_rate']:.4f}")
        print(f"  Coverage:          {avg['coverage']:.4f}")
        print(f"  Diversity:         {avg['diversity']:.4f}")
        print(f"  Novelty:           {avg['novelty']:.4f}")
        
        self.results[recommender.name] = avg
        return avg
    
    def _calc_diversity(self, rec, users, k):
        """计算类目多样性"""
        all_recs = []
        for uid in users:
            all_recs.extend(rec.recommend(uid, k))
        if not all_recs: return 0
        counts = Counter(all_recs)
        total = len(all_recs)
        entropy = -sum((c/total) * math.log(c/total) for c in counts.values())
        max_entropy = math.log(len(counts)) if len(counts) > 1 else 1
        return entropy / max_entropy
    
    def _calc_novelty(self, rec, users, k, train):
        """计算新颖性"""
        popularity = train["product_id"].value_counts()
        max_pop = popularity.max()
        novs = []
        for uid in users:
            recs = rec.recommend(uid, k)
            if recs:
                novs.append(np.mean([1 - popularity.get(p, 0) / max_pop for p in recs]))
        return np.mean(novs) if novs else 0
    
    def compare(self):
        """模型对比"""
        print(f"\n{'='*60}")
        print("模型综合对比")
        print(f"{'='*60}")
        cols = ["precision", "recall", "f1", "hit_rate", "coverage", "diversity", "novelty"]
        print(f"\n{'模型':<25s}", end="")
        for c in cols: print(f"{c:>10s}", end="")
        print()
        print("-" * (25 + 10 * len(cols)))
        for name, r in sorted(self.results.items()):
            print(f"{name:<25s}", end="")
            for c in cols: print(f"{r[c]:>10.4f}", end="")
            print()
        return self.results


# ============== AUC / LogLoss 评估（二分类口径，v2 新增） ==============

def auc_from_scores(pos_scores, neg_scores):
    """基于秩的 AUC（并列分数取平均秩）"""
    scores = np.concatenate([np.asarray(pos_scores, dtype=np.float64),
                             np.asarray(neg_scores, dtype=np.float64)])
    n_pos, n_neg = len(pos_scores), len(neg_scores)
    if n_pos == 0 or n_neg == 0:
        return 0.5
    ranks = pd.Series(scores).rank(method="average").values
    pos_rank_sum = ranks[:n_pos].sum()
    return float((pos_rank_sum - n_pos * (n_pos + 1) / 2.0) / (n_pos * n_neg))


def logloss_from_logits(pos_logits, neg_logits):
    logits = np.concatenate([np.asarray(pos_logits, dtype=np.float64),
                             np.asarray(neg_logits, dtype=np.float64)])
    y = np.concatenate([np.ones(len(pos_logits)), np.zeros(len(neg_logits))])
    p = 1.0 / (1.0 + np.exp(-np.clip(logits, -30, 30)))
    return float(-np.mean(y * np.log(np.clip(p, 1e-7, 1)) +
                          (1 - y) * np.log(np.clip(1 - p, 1e-7, 1))))


def evaluate_auc(score_fn, test_data, train_data, all_items, n_neg=10, seed=42):
    """
    全局 pooled AUC：正例 = 测试集交互；负例 = 每用户抽取未交互商品 n_neg 个。
    score_fn(user_id, item_list) → 分数数组（越高越可能为正）。
    """
    rng = np.random.default_rng(seed)
    items = np.array(all_items)
    train_map = train_data.groupby("user_id")["product_id"].apply(set).to_dict()
    test_map = test_data.groupby("user_id")["product_id"].apply(set).to_dict()
    pos_all, neg_all = [], []
    for uid in test_data["user_id"].unique():
        pos = sorted(test_map.get(uid, set()))
        if not pos:
            continue
        excl = train_map.get(uid, set()) | test_map.get(uid, set())
        neg = []
        tries = 0
        while len(neg) < n_neg and tries < n_neg * 20:
            cand = items[rng.integers(len(items))]
            tries += 1
            if cand not in excl and cand not in neg:
                neg.append(cand)
        if not neg:
            continue
        pos_all.extend(score_fn(uid, pos).tolist())
        neg_all.extend(score_fn(uid, neg).tolist())
    return {
        "auc": round(auc_from_scores(pos_all, neg_all), 4),
        "logloss": round(logloss_from_logits(pos_all, neg_all), 4),
        "positives": len(pos_all),
        "negatives": len(neg_all),
    }


# ============== 主程序 ==============

def main():
    print("=" * 60)
    print("宠物用品电商 - 混合推荐系统")
    print(f"启动: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # 1. 加载数据
    loader = DataLoader()
    loader.load_all()
    rating_data = loader.build_rating_matrix()
    print(f"评分矩阵: {len(rating_data)} 条, 评分范围 {rating_data['rating'].min():.1f}-{rating_data['rating'].max():.1f}")
    
    # 2. 划分数据集
    evaluator = RecommenderEvaluator()
    train_set, test_set = evaluator.train_test_split(rating_data, test_ratio=0.2)
    print(f"训练集: {len(train_set)} 条, 测试集: {len(test_set)} 条")
    
    # 3. 训练所有模型
    print(f"\n{'='*60}")
    print("训练推荐模型")
    print(f"{'='*60}")
    
    popular = PopularityRecommender()
    popular.fit(train_set, loader.products, loader.users)
    
    ubcf = UserBasedCF(k_neighbors=30)
    ubcf.fit(train_set, loader.products, loader.users)
    
    ibcf = ItemBasedCF(k_neighbors=30)
    ibcf.fit(train_set, loader.products, loader.users)
    
    svd = SVDRecommender(n_factors=30, lr=0.01, reg=0.02, n_epochs=15)
    svd.fit(train_set, loader.products, loader.users)
    
    cb = ContentBasedRecommender()
    cb.fit(train_set, loader.products, loader.users)
    
    hybrid = HybridRecommender(mode="weighted")
    hybrid.add_model(ubcf, weight=0.30)
    hybrid.add_model(ibcf, weight=0.25)
    hybrid.add_model(svd, weight=0.30)
    hybrid.add_model(cb, weight=0.15)
    hybrid.fit(train_set, loader.products, loader.users)
    
    # 4. 评估（v1 六模型，口径不变）
    TOP_K = 10
    for model in [popular, ubcf, ibcf, svd, cb, hybrid]:
        evaluator.evaluate(model, test_set, train_set, top_k=TOP_K)

    # ==================== v2 新增：时序规则注入（第 7/8 模型 + 消融） ====================
    print(f"\n{'='*60}")
    print("v2 时序规则注入: RulesBoost / FM / 消融实验")
    print(f"{'='*60}")

    cos_feature = CoScoreFeature()
    cos_feature.fit(train_set, loader.products, loader.max_order_date_id)
    cov_stats = cos_feature.coverage_stats(train_set)
    print(f"  Co-Score 特征覆盖率(训练集): {cov_stats['covered']}/{cov_stats['pairs']} "
          f"= {cov_stats['coverage']:.2%}")

    # 第 7 模型: RulesBoost（时序规则增强重排序）
    rb = RulesBoostRecommender(cos_feature=cos_feature)
    rb.fit(train_set, loader.products, loader.users)
    evaluator.evaluate(rb, test_set, train_set, top_k=TOP_K)

    # Hybrid + RulesBoost（闭环增强证据）
    hybrid_rb = HybridRecommender(mode="weighted")
    hybrid_rb.name = "Hybrid + RulesBoost"  # 区别于 v1 Hybrid，避免结果键覆盖
    hybrid_rb.add_model(ubcf, weight=0.30)
    hybrid_rb.add_model(ibcf, weight=0.25)
    hybrid_rb.add_model(svd, weight=0.30)
    hybrid_rb.add_model(cb, weight=0.15)
    hybrid_rb.add_model(rb, weight=0.20)
    hybrid_rb.fit(train_set, loader.products, loader.users)
    evaluator.evaluate(hybrid_rb, test_set, train_set, top_k=TOP_K)

    # 第 8 模型: FM（全特征）+ 7 维指标
    fm_full = FMRecommender(use_co=True, use_mf=True, name="FM (Full)")
    fm_full.fit(train_set, loader.products, loader.users, cos_feature=cos_feature)
    evaluator.evaluate(fm_full, test_set, train_set, top_k=TOP_K)

    # AUC / LogLoss：FM 变体消融 + 热度基线参照
    print(f"\n{'='*60}")
    print("AUC / LogLoss 评估（正例=测试集交互，负例=每用户抽 10 个未交互商品）")
    print(f"{'='*60}")

    pop_score = dict(zip(loader.products["product_id"],
                         np.log1p(loader.products["total_sales"].astype(float))))
    def pop_fn(uid, items):
        return np.array([pop_score.get(p, 0.0) for p in items])

    auc_results = {}
    auc_results["Popularity Baseline"] = evaluate_auc(pop_fn, test_set, train_set,
                                                      loader.products["product_id"].tolist())
    print(f"  Popularity Baseline: AUC={auc_results['Popularity Baseline']['auc']:.4f}")

    fm_variants = [
        FMRecommender(use_co=True, use_mf=True, name="FM (Full)"),
        FMRecommender(use_co=False, use_mf=True, name="FM (w/o Co-Score)"),
        FMRecommender(use_co=True, use_mf=False, name="FM (w/o MF-ID)"),
        FMRecommender(use_co=False, use_mf=False, name="FM (Content-Only)"),
    ]
    for fm in fm_variants:
        fm.fit(train_set, loader.products, loader.users, cos_feature=cos_feature)
        fn = (lambda m: (lambda uid, items: m.score_items(uid, items)))(fm)
        auc_results[fm.name] = evaluate_auc(fn, test_set, train_set,
                                            loader.products["product_id"].tolist())
        r = auc_results[fm.name]
        print(f"  {fm.name}: AUC={r['auc']:.4f}, LogLoss={r['logloss']:.4f}")

    results = evaluator.compare()
    
    # 5. 示例推荐
    print(f"\n{'='*60}")
    print("示例用户推荐 (Top-5)")
    print(f"{'='*60}")
    sample_users = train_set["user_id"].value_counts().head(5).index.tolist()
    for uid in sample_users[:3]:
        print(f"\n用户 {uid}:")
        recs = hybrid.recommend(uid, 5)
        if recs and "product_name" in loader.products.columns:
            pnames = loader.products.set_index("product_id")
            for r in recs:
                if r in pnames.index:
                    print(f"  → {pnames.loc[r, 'product_name']}")
    
    # 6. 保存结果
    full_auc = auc_results.get("FM (Full)", {})
    no_co_auc = auc_results.get("FM (w/o Co-Score)", {})
    output = {
        "evaluation_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "data_stats": {"users": len(loader.users), "products": len(loader.products),
                       "interactions": len(rating_data), "train": len(train_set), "test": len(test_set)},
        "results": {k: {mk: float(mv) for mk, mv in v.items()} for k, v in results.items()},
        "auc_results": auc_results,
        "co_score_stats": cov_stats,
        "ablation_summary": {
            "co_score_gain_auc": round(full_auc.get("auc", 0) - no_co_auc.get("auc", 0), 4),
            "co_score_gain_logloss": round(no_co_auc.get("logloss", 0) - full_auc.get("logloss", 0), 4),
            "note": "gain = FM(Full) - FM(w/o Co-Score)；LogLoss 越低越好，取 w/o - Full",
        },
    }
    path = os.path.join(REC_DIR, "recommendation_results.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\n结果已保存: {path}")

if __name__ == "__main__":
    main()
