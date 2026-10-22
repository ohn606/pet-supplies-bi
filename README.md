# PetSupplies-BI · 宠物用品电商用户行为分析与智能推荐系统

基于多源真实公开数据集的端到端 BI 系统：**数据采集 → 数仓建模 → ETL → 多维分析 → 推荐算法 → 可视化看板**，覆盖毕业设计的全部工程与实验环节。

## 核心成果

- **数据规模**：48,473 用户 / 20,836 商品 / 169,708 订单 / 264.8 万行为日志（2017.07–2018.10，三平台公开真实数据集）
- **数据仓库**：星型模型（3 维 4 事实）+ DWS 主题宽表 4 张 + ADS 聚合层，全量/增量幂等 ETL，逐值对账
- **推荐算法**：8+1 模型纯 NumPy 手写实现与实证对比（CF / SVD / Content / Hybrid / RulesBoost / FM），7 维指标 + AUC + 受控消融
- **时序购物篮**：158 条跨类目时序规则（「买 A → 间隔 T 天 → 买 B」），TOP 300 补货候选
- **BI 看板**：7 页面 / 36 API，业务·管理双角色视图，已部署云服务器
- **实验方法论**：负面结果如实报告（SVD 全零）、模拟 A/B 双层口径、弱监督画像可追溯

## 系统架构

```
┌─────────────────────────────────────────────────────────┐
│  展示层   运营总览 · 商品分析 · 用户分析 · 热销榜          │
│           推荐监控 · 购物篮洞察 · A/B 实验（7 页面）        │
├─────────────────────────────────────────────────────────┤
│  服务层   BI API 服务（36 个 JSON 接口）· 查询引擎 · CSV   │
├─────────────────────────────────────────────────────────┤
│  计算层   RFM/留存/漏斗 · 时序购物篮 · 推荐引擎 · A/B 模拟  │
├─────────────────────────────────────────────────────────┤
│  数据层   ODS → DWD(星型 3维4事实) → DWS(4 宽表) → ADS    │
└─────────────────────────────────────────────────────────┘
```

## 快速开始

```bash
# 1. 环境
Python 3.12+，依赖：numpy、pandas（推荐 venv）

# 2. 获取数据集并放置到 data/raw/real/（详见 docs/REAL_DATASETS.md）
#    - Amazon Pet Supplies Reviews 2018（Kaggle）
#    - 淘宝 UserBehavior 子集（阿里云天池）
#    - 宠物垂类商品数据 × 2

# 3. 建仓 + ETL（生成 warehouse/pet_supplies_dw.db）
python etl/run_etl.py
python etl/build_dws.py

# 4. 分析与推荐（产出 analytics/*.json、recommendation/*.json）
python analytics/spark_analytics.py
python recommendation/hybrid_recommender.py
python analytics/pet_lifecycle.py
python analytics/ab_simulation.py

# 5. 启动看板（默认 http://localhost:9888）
python dashboard/server.py
```

## 目录结构

```
├── analytics/        # 多维分析与实验（RFM/时序购物篮/生命阶段/模拟A/B）
├── recommendation/   # 推荐系统：8+1 模型实现、训练与评估
├── dashboard/        # BI 看板（Python 标准库 HTTP 服务 + ECharts）
│   ├── server.py     #   后端：36 个 JSON API + 静态服务
│   ├── templates/    #   前端页面
│   └── static/       #   静态资源
├── etl/              # ETL：全量/增量加载、ADS 预聚合、商品图本地化
├── warehouse/        # 数仓 SQL（schema + DWS）与本地 SQLite 数仓
├── data/             # 数据集存放（不入库，见 docs/REAL_DATASETS.md）
├── docs/             # 项目文档（数据集说明、数仓设计）
├── thesis/           # 毕业论文（Markdown）
├── deploy/           # 云部署脚本与说明
└── legacy/           # 早期 Flask 原型（存档）
```

## 数据口径说明（重要）

- **活跃用户分域**：行为域（淘宝）与评价域（Amazon）用户 ID 空间**互不相交**，全站按域分别呈现（24,673 / 48,473），严禁跨源去重混算
- **类目营收**：以 `fact_order_items.item_amount` 为准（¥21,737,311），ADS 层预聚合与明细逐值对账
- **生命阶段**：基于商品文本关键词的弱监督推断，无证据用户显式标注「未知」，观测窗口 274 天的边界已声明
- **模拟 A/B**：真实锚点 + 模拟响应双层口径，所有模拟指标带 `simulated` 标记

## 技术栈

Python 3.12+ · NumPy / pandas（算法纯手写实现）· SQLite（数仓）· PySpark（分析管线）· ECharts（可视化）· Python 标准库 HTTP（零依赖部署）

## 文档索引

| 文档 | 内容 |
|---|---|
| [docs/REAL_DATASETS.md](docs/REAL_DATASETS.md) | 数据集来源、下载与字段对齐 |
| [docs/DWS_LAYER.md](docs/DWS_LAYER.md) | DWS 主题宽表设计 |
| [CHANGELOG.md](CHANGELOG.md) | 版本变更记录 |

## License

仅供学习与毕业设计展示使用。数据集版权归原平台所有，遵循其各自使用条款。
