# PetSupplies-BI · 宠物用品电商用户行为分析与智能推荐系统

基于**多源真实公开数据集**的端到端 BI 系统：数据采集 → 数仓建模 → ETL → 多维分析 → 推荐算法 → 可视化看板，覆盖毕业设计的全部工程与实验环节。

> 数据非模拟：全部来自 4 个公开数据集（Amazon Reviews 2018 / Amazon 印度站 / Chewy / 淘宝 UserBehavior）。
> 方法论三铁律：**数据真实（缺失显式标注，禁随机填充）· 口径同源（分域统计，禁跨源混算）· 负面结果如实报告**。

---

## 1. 核心成果

| 维度 | 成果 |
|---|---|
| 数据规模 | 48,473 用户 / 20,836 商品 / 169,708 订单 / 2,648,323 行为日志 / 169,708 评价（2017-07 ~ 2018-10） |
| 数据仓库 | SQLite 星型模型（3 维 4 事实 + 10 表 9 索引）+ DWS 主题宽表 4 张 + ADS 聚合层；全量/增量双模式幂等 ETL；内置逐值对账 |
| 多维分析 | RFM / 留存队列 / 转化漏斗 / 复购 / Apriori + 自研时序购物篮引擎 + 宠物生命阶段画像 + 在线模拟 A/B |
| 推荐算法 | 8+1 模型纯 NumPy 手写（Popularity / User-CF / Item-CF / SVD / Content / Hybrid / RulesBoost / FM），7 维指标 + AUC/LogLoss + 受控消融 |
| 可视化 | 7 页面 / 37 JSON API / 业务·管理双角色视图，ECharts 纯前端，零第三方后端依赖，已上云 |

---

## 2. 系统架构

```
┌────────────────────────────────────────────────────────────────┐
│ 展示层  dashboard/templates/  ·  ECharts SPA（双版本皮肤）        │
│         业务视角：总览 / 商品分析 / 用户分析 / 宠物热销榜           │
│         管理视角：推荐监控 / 购物篮洞察 / A/B 模拟实验             │
├────────────────────────────────────────────────────────────────┤
│ 服务层  dashboard/server.py（纯标准库 HTTP，多线程）              │
│         37 个 JSON API · 时间/类目筛选 · CSV 导出 · 登录鉴权       │
├────────────────────────────────────────────────────────────────┤
│ 计算层  recommendation/hybrid_recommender.py   8+1 推荐模型      │
│         analytics/  用户指标 · 时序购物篮 · 生命阶段 · A/B 模拟    │
├────────────────────────────────────────────────────────────────┤
│ 数据层  warehouse/pet_supplies_dw.db（SQLite）                   │
│         ODS → DWD 星型(3维4事实) → DWS 主题宽表(4) → ADS 聚合(5)  │
├────────────────────────────────────────────────────────────────┤
│ 接入层  data/build_real_ods.py   4 数据集字段对齐 → ODS           │
│         etl/run_etl.py           full / incremental 双模式        │
│         etl/build_dws.py         DWS 宽表 + 4 项逐值对账           │
└────────────────────────────────────────────────────────────────┘
```

---

## 3. 技术栈

| 层次 | 技术 | 说明 |
|---|---|---|
| 数据处理 | Python 3.12+ / pandas / NumPy | ODS 构建、ETL、分析 |
| 数据仓库 | SQLite（星型模型） | 建模可平移 Hive / Spark SQL |
| 分析引擎 | pandas + NumPy（PySpark 风格 API） | `spark_analytics.py` 算子思路可直迁集群 |
| 推荐算法 | **纯 NumPy 手写** | 无 sklearn / TensorFlow；全局 seed=42 可复现 |
| 可视化 | Python 标准库 `http.server` + ECharts 5.5（本地部署） | 后端零第三方依赖，离线可用 |
| 部署 | systemd 服务 + 阿里云 ECS | `deploy/` 脚本已脱敏（`<SERVER_IP>` 占位） |

---

## 4. 数据源与规模

| 编号 | 数据集 | 平台 | 规模 | 许可 | 角色 |
|---|---|---|---|---|---|
| S1 | Amazon Reviews 2018（rogate16 镜像） | Kaggle | 55 万条 → Pet_Supplies 清洗后 **169,708** | 限研究（引 Ni et al. 2019） | 用户/商品/订单/评价主源 |
| S2 | Amazon Pet Supplies Data（印度站） | Kaggle | 343 商品 | CC0 | 商品属性 + 四级类目 |
| S3 | Chewy Products Dataset | Kaggle | 215 商品 | CC0 | 价格带与评分校准 |
| S4 | UserBehavior（dataId=649） | 阿里云天池 | 1 亿条 → 抽样 **2,478,615** | CC BY-NC-SA 4.0 | 真实 pv/fav/cart/buy 行为序列 |

**ODS 字段对齐三原则**：① 直接映射（源有 → 直搬）② 显式派生（可推导 → 规则写进数据字典，如宠物类型=购买品类众数、消费层级=金额 P90/P60 分位）③ 明确缺失（不存在 → 置「未知」/0，**严禁随机填充**，limitations 登记）。

---

## 5. 目录结构（= 仓库实际内容）

```
├── data/                  # ODS 贴源层（原始数据集 + 构建脚本；*.csv/jsonl 不入库）
│   ├── build_real_ods.py          # 多源字段对齐 → ODS（628 行）
│   ├── download_real_datasets.py  # 数据集下载（断点续传 + 完整性校验）
│   └── generate_data.py           # 早期模拟数据生成器（已退役，仅对照存档）
├── etl/                   # ETL 层
│   ├── run_etl.py                  # 星型建仓：full / incremental 双模式，INSERT OR IGNORE 幂等
│   ├── build_dws.py                # DWS 主题宽表 ×4 + 4 项逐值对账
│   ├── build_ads_behavior_monthly.py / build_ads_user_behavior_daily.py  # ADS 预聚合
│   └── download_product_images.py / build_product_images.py              # 商品图本地化
├── warehouse/             # 数仓（*.db 不入库，按 SQL 重建）
│   └── sql/              # 01 星型 DDL · 02 DWS · 03 生命周期 · 04 月度行为 · 05 用户行为日宽表
├── analytics/             # 分析层（4 引擎 + 4 结果 JSON）
│   ├── spark_analytics.py          # 用户指标（RFM/留存/漏斗/复购）+ Apriori 购物篮
│   ├── temporal_basket.py          # ★ 时序购物篮引擎（跨订单「买 A → T 天 → 买 B」）
│   ├── pet_lifecycle.py            # ★ 宠物生命阶段画像推断（规则推断 + 切换检测）
│   └── ab_simulation.py            # ★ 在线模拟 A/B 实验（显式响应模型 + bootstrap）
├── recommendation/        # 智能层
│   ├── hybrid_recommender.py       # 8+1 模型训练与评估（1280 行，纯 NumPy）
│   └── recommendation_results.json # 评估结果（Top-K 七维 + AUC + 消融）
├── dashboard/             # 可视化层
│   ├── server.py                   # 37 API + 静态服务（1669 行，零第三方依赖）
│   ├── templates/                  # petops.html（现行 SaaS 风）/ dashboard.html（旧版存档）
│   └── static/                     # ECharts 本地文件 + 类目插画（商品图 *.jpg 不入库）
├── deploy/                # 云部署（systemd 单元 + 部署脚本，已脱敏）
├── docs/                  # REAL_DATASETS.md（数据集说明）· DWS_LAYER.md（宽表设计）
├── legacy/                # 早期 Flask 原型（存档）
├── CHANGELOG.md  README.md  .gitignore
```

**不入库内容**（本地保留，克隆后按 §7 重建）：`data/raw/` 原始数据集、`warehouse/*.db` 数仓、商品图、`thesis/` 论文与汇报材料。

---

## 6. 核心模块详解

### 6.1 ODS 构建 · `data/build_real_ods.py`

**职责**：把 4 个异构公开数据集对齐为下游零改动的统一贴源层。
**实现**：商品聚合（标题哈希主键 + 关键词类目推断 + 历史评分均值）+ 用户画像派生（宠物类型=品类众数 / 消费层级=分位分档 / 活跃度=订单数分档）+ 淘宝行为序列抽样（`user_mod=40`）+ 评价事件补链；输出 6 类 CSV+JSONL 双格式 + `data_dictionary.json`（字段级映射 + 7 条 limitations）。
**产物**：`data/raw/*.jsonl|csv`、`ods_build_report.json`。

### 6.2 ETL 建仓 · `etl/run_etl.py`

**职责**：ODS → 星型数据仓库，支持全量与增量。
**实现**：`full`（删表重建 → 全量加载 → 重算 ADS）/ `incremental`（`INSERT OR IGNORE` 幂等 → 仅增量更新 ADS）；加载时计算 RFM 写维表、派生 `order_hour`/`is_first_order`；时间维度由 `data_period.json` 数据驱动；WAL 模式 + 外键约束。
**约定**：`date_id` 为整数 `YYYYMMDD`（范围过滤免 JOIN 日期维）；事实表查询必须走 date/user/product 索引。

### 6.3 DWS 主题宽表 · `etl/build_dws.py`

**职责**：固化主题口径的单一事实来源，避免下游重复聚合 265 万行明细。
**实现**：4 张宽表——`dws_user_behavior_1d`（26.4 万行，留存/漏斗/热力图）、`dws_user_purchase_1d`（8.9 万，RFM/推荐矩阵）、`dws_product_sales_1d`（13.2 万，ADS/热销榜）、`dws_user_category_cum`（9.8 万，Co-Score/补货候选）；口径：有效订单 = 非退款/取消，金额 = `item_amount` 实付；**内置 4 项 DWS↔DWD 逐值对账**（PV 2,218,299 / 销售额 ¥21,737,311.09，全 PASS 交付）。

### 6.4 用户指标与购物篮 · `analytics/spark_analytics.py`

**职责**：RFM 分层、月度留存队列、同源转化漏斗、复购分析、Apriori 类目交叉销售。
**真实结论**：高价值客户 364 人（0.75%，人均 ¥6,605.8）；次月留存 6.3%~19.3%，运营窗口在首购后 30 天；漏斗（同源）浏览 24,566 → 收藏 39.6% → 加购 75.4% → 下单 68.5%；整体复购率 60.1%；产品级 Apriori 因真实长尾支持度不足为空，类目级交叉销售有效。

### 6.5 时序购物篮引擎 · `analytics/temporal_basket.py` ★

**职责**：突破单订单静态关联，挖掘跨订单「买 A → 间隔 T 天 → 买 B」类目级时序规律与消耗品补货周期。
**实现**：用户-类目购买序列构建（同日同类目去重）→ 时序规则挖掘（support/confidence/lift + 自研 **temporal_share** 方向性指标 + 间隔 P25/中位/P75，用户级一人一票）→ 复购周期画像 → 补货候选（距上次购买 ≥ 周期中位×0.8，附建议商品与 RFM）。
**结果**：158 条规则，节律集中 30–37 天；13 类目周期（零食 30 / 主粮 32 / 猫砂 37 / 清洁美容 38 天）；补货候选 96,539（输出 TOP 300）。
**下游**：推荐 Co-Score 特征、A/B 策略 C 信号、看板「购物篮洞察」3 个 API。

### 6.6 宠物生命阶段画像 · `analytics/pet_lifecycle.py` ★

**职责**：原始数据无宠物年龄字段，从商品文本**显式派生**阶段证据，推断用户宠物生命阶段并检测「幼年粮 → 成年粮」迁移。
**实现**：6 个消耗品二级类目 + 三组阶段正则（幼年/成年/老年）→ 冲突分级处理（senior 优先于 adult、真冲突记「多阶段」、剔除 maintenance 防误判）→ 用户证据聚合（保留可追溯商品）→ 置信度三档推断 → 迁移检测四态机（confirmed 86 / ongoing 15 / same_day_mixed / short_interval_undetermined，30 天最小间隔阈值）。
**边界**：观测窗口 274 天 < 成长周期，结果仅作「粮型偏好迁移」行为证据，**不等于宠物月龄**；无证据用户显式标注「未知」（41,772 人）。

### 6.7 在线模拟 A/B 实验 · `analytics/ab_simulation.py` ★

**职责**：不动线上流量，用显式响应模型受控回放多策略，输出带显著性检验的对比结论。
**实现**：2018-08-01 切训练/测试（策略只见训练期防泄漏）→ 四维权重的类目打分（热度/亲和/时序接近度/探索）→ top-10 曝光生成 → 响应模型 `p_click = base_ctr×(1+亲和增益)×(1+周期接近增益)×0.85^位次`、`p_conv = base_cvr×(1+价格贴合)` → **双层口径**（真实锚点 `real_hit_rate` 零模拟成分 + 模拟层全带 `simulated` 标记）→ 用户级配对 bootstrap（1000 次，CI95 + p 值）；每用户 RNG 由 `md5(uid)^md5(strategy:seed)` 派生，同配置重跑逐位一致。
**结果**（seed=42，2,371 用户）：策略 B +2.11 GMV/用户（p=0.008 显著）；策略 C 加时序信号 +0.59 不显著（p=0.386）——负面结论：时序信号定位是补货触发，非排序权重。

### 6.8 推荐系统 · `recommendation/hybrid_recommender.py` ★

**职责**：8+1 模型同台实证对比，回答“真实稀疏场景下哪种推荐策略可靠”。
**实现**：显式评分 + 隐式反馈加权（购买 0.8/加购 0.5/收藏 0.3/浏览 0.1）构建统一评分矩阵；模型：Popularity / User-CF（K=40）/ Item-CF（K=30）/ SVD（手写 SGD，40 因子）/ Content-Based / Hybrid（四路加权）/ RulesBoost（Co-Score 重排序，无信号回退热度）/ FM（全批 Adam，16 因子，二阶 = MF 通道 + Co-Score 显式规则交叉）；按用户留出 20% 交互作测试；Top-10 七维 + AUC/LogLoss + 四变体消融。
**Co-Score**：把时序规则转成 (user,item) 显式特征——规则置信度 × 时序高斯接近度，窗口外保留 0.15 长期亲和下限（防信息等死），仅用训练集历史防泄漏。
**结果**：Hybrid P@10=0.0050（基线 5 倍，Top-K 综合最优）；**负面结果**：SVD 全零（密度 0.017%）、FM 的 ID 隐因子通道过拟合（Full AUC 0.7685 < w/o ID 0.8662）、Co-Score 在全局 AUC 为负贡献（-0.0243，价值在补货时点排序）——两个独立模型同根因，构成一致的稀疏性证据链。

### 6.9 可视化看板 · `dashboard/`

**职责**：数仓 + 分析 + 推荐结果的统一出口，业务/管理双视角。
**实现**：纯标准库多线程 HTTP 服务，37 个 JSON API（总览 8 / 商品 6 / 推荐 2 / 用户 7 / 热销榜 5 / 购物篮 3 / 生命周期 1 / A-B 2 / 导出）；前端 SPA + 本地 ECharts，登录鉴权，业务/管理视角切换（localStorage 记忆）；顶栏时间范围 + 类目筛选全局联动；CSV 导出。
**页面**：业务视角 = 运营总览 / 商品分析 / 用户分析 / 宠物热销榜；管理视角 = 推荐监控 / 购物篮洞察 / A/B 模拟。

---

## 7. 快速开始（克隆后复现）

```bash
# 0. 环境：Python 3.12+，pip install numpy pandas（推荐 venv）

# 1. 下载数据集 → data/raw/real/（下载脚本 + 手动指引见 docs/REAL_DATASETS.md）
python data/download_real_datasets.py --list
python data/download_real_datasets.py          # Kaggle 自动；天池按提示手动放入

# 2. 构建 ODS（字段对齐；自动备份既有模拟数据）
python data/build_real_ods.py

# 3. ETL 建仓（生成 warehouse/pet_supplies_dw.db）
python etl/run_etl.py                          # 全量；增量：--mode incremental
python etl/build_dws.py                        # DWS 宽表 + 对账（约 13 秒）

# 4. 分析（须先于推荐，产出 Co-Score 依赖的时序规则）
python analytics/temporal_basket.py            # ① 先跑：时序规则喂给推荐
python analytics/spark_analytics.py            # ② 用户指标 + 购物篮
python analytics/pet_lifecycle.py              # ③ 生命阶段画像
python analytics/ab_simulation.py              # ④ A/B 模拟（约 7 秒）

# 5. 推荐训练评估（耗时较长）
python recommendation/hybrid_recommender.py

# 6. 启动看板 → http://localhost:9888
python dashboard/server.py                     # 端口由 DASHBOARD_HOST/PORT 环境变量可调
```

看板所需的 4 个分析结果 JSON（时序规则/生命阶段/A-B/分析）已随仓库分发，克隆后**不跑第 4 步也能点亮看板**；想复现实验再按顺序执行。

---

## 8. 数据口径铁律（改代码前必读）

1. **活跃用户分域**：行为域（淘宝，24,673）与评价域（Amazon，48,473）用户 ID 空间互不相交，按域分别呈现，**严禁跨源去重混算**。
2. **类目营收**：以 `fact_order_items.item_amount` 为准（¥21,737,311）；ADS 预聚合与明细逐值对账。
3. **有效订单**：`order_status NOT IN ('已退款','已取消')`；金额取实付 `item_amount`。
4. **缺失字段**：一律「未知」/0 + limitations 登记，禁止随机填充。
5. **生命阶段**：弱监督推断，证据可追溯，274 天窗口边界须随结果声明。
6. **模拟指标**：必须带 `simulated` 标记，论文引用以真实锚点为主要证据。

---

## 9. 文档索引

| 文档 | 内容 |
|---|---|
| [docs/REAL_DATASETS.md](docs/REAL_DATASETS.md) | 数据集来源、下载、字段对齐与许可 |
| [docs/DWS_LAYER.md](docs/DWS_LAYER.md) | DWS 主题宽表建模口径与血缘 |
| [CHANGELOG.md](CHANGELOG.md) | 版本变更记录 |

## License

仅供学习与毕业设计展示。数据集版权归原平台所有，遵循各自使用条款（Amazon Reviews 限研究用途，引用 Ni et al. 2019；淘宝 UserBehavior 遵循 CC BY-NC-SA 4.0）。
