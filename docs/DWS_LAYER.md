# DWS 汇总服务层 · 建模口径与依赖血缘（v1.5.0）

> 数据铁律适用：本层所有字段均为 DWD 明细的**确定性汇总**，不引入任何模拟或臆测值；
> 类目字段为 `dim_product` 构建日快照冗余，源变更需重建。

## 1. 层级定位

```
ODS/RAW  data/raw/*.csv|jsonl
   │  ETL（etl/run_etl.py，全量/增量幂等）
DWD      fact_orders / fact_order_items / fact_user_behavior / fact_reviews
   │  DWS 构建（etl/build_dws.py，全量幂等重建 + 对账校验）   ←── 本层
DWS      dws_user_behavior_1d / dws_user_purchase_1d / dws_product_sales_1d / dws_user_category_cum
   │  ADS 聚合（etl/run_etl.py compute_aggregations）＋ 分析/推荐/看板直接消费
ADS      ads_daily_sales / ads_daily_behavior / ads_category_sales / ads_pet_lifecycle
```

DWS 是"主题宽表"层：以**业务主体（用户/商品）× 时间**为主键粒度做一次汇总，
向上同时服务 ADS 聚合、RFM/留存/漏斗分析、推荐特征（Co-Score）与生命周期证据抽取，
避免同类口径在多处 SQL 中重复实现。

## 2. 表清单与粒度

| 表名 | 粒度（主键） | 行数 | 用途 |
|---|---|---|---|
| `dws_user_behavior_1d` | (date_id, user_id) | 263,955 | 每日行为宽表 → 留存/漏斗/热力图/`ads_daily_behavior` |
| `dws_user_purchase_1d` | (date_id, user_id) | 88,928 | 每日购买宽表 → RFM/推荐评分矩阵 |
| `dws_product_sales_1d` | (date_id, product_id) | 132,025 | 商品日销售 → `ads_daily_sales`/`ads_category_sales`/热销榜 |
| `dws_user_category_cum` | (user_id, sub_category) | 97,543 | 用户×类目累计亲和 → Co-Score/生命周期证据/补货候选 |

## 3. 口径定义（唯一权威口径）

| 口径 | 定义 | 与既有模块的一致性 |
|---|---|---|
| 有效订单 | `order_status NOT IN ('已退款','已取消')` | 与 `ads_daily_sales` 相同（当前数据全部为"已完成"，过滤为向前兼容保留） |
| 金额 | `item_amount`（实付，明细级合计） | 与看板总览"累计销售额 ¥2174 万"一致（21,737,311.09） |
| 浏览/PV | `behavior_type='浏览'` 计数 | 与用户分析页 PV 2,218,299 一致 |
| 类目冗余 | `dim_product.main_category / sub_category` 构建日快照 | 商品维变更有需重建 DWS（幂等，约 13s） |

## 4. 依赖血缘

**上游（DWD → DWS）**

```
fact_user_behavior ──────────────────────────→ dws_user_behavior_1d
fact_order_items ⋈ fact_orders ──────────────→ dws_user_purchase_1d
                             └──────────────→ dws_product_sales_1d ⋈ dim_product(类目快照)
fact_order_items ⋈ fact_orders ⋈ dim_product → dws_user_category_cum
```

**下游（DWS → 消费方）**

| 消费方 | 读取的 DWS 表 | 说明 |
|---|---|---|
| `ads_daily_behavior` | dws_user_behavior_1d | 现阶段 ADS 仍直读 DWD（口径等价），迁移路径见 §6 |
| `ads_daily_sales` / `ads_category_sales` | dws_product_sales_1d | 同上 |
| RFM / 留存 / 漏斗（analytics/spark_analytics.py） | dws_user_purchase_1d / dws_user_behavior_1d | 可迁移 |
| Co-Score（recommendation/hybrid_recommender.py） | dws_user_category_cum | 与 `last_date_id` 口径对应 |
| 生命周期证据（analytics/pet_lifecycle.py） | dws_user_category_cum（first/last_purchase_date_id） | 可迁移 |

## 5. 构建与校验

```bash
# ETL 之后执行（全量幂等重建，约 13s）
python etl/build_dws.py
```

构建脚本内置 **对账校验**（对账失败会显式输出 FAIL）：

| 校验项 | 结果 |
|---|---|
| dws_user_behavior_1d.pv = DWD 浏览数 | PASS（2,218,299） |
| dws_user_purchase_1d.amount = DWD 有效订单销售额 | PASS（21,737,311.09） |
| dws_product_sales_1d.amount = DWD 有效订单销售额 | PASS（21,737,311.09） |
| dws_user_category_cum.amount = DWD 有效订单销售额 | PASS（21,737,311.09） |

## 6. 迁移说明（ADS 现状）

ADS 三张聚合表现仍由 `run_etl.py` 直读 DWD 构建，与 DWS 口径**逐值等价**（对账已证）。
将 ADS 输入切换为 DWS 属纯等价重构，为避免触动已发布指标，本版本不改变其数据路径；
论文与文档按"DWS 为主题口径单一来源、ADS 为应用聚合"的分层描述对齐（PROJECT_DOC §2.1）。
