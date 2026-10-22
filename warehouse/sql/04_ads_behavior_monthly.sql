-- =============================================================
-- ADS 层：月度行为/转化主题聚合表（v1.7.0）
-- =============================================================
-- 建立动机（性能 + 架构双重原因）：
--   recommendation_daily 接口的月度口径原先直查 fact_user_behavior（264.8 万行）
--   做 COUNT(DISTINCT user_id)，单次请求耗时 12s，5 并发时最坏 15s，
--   演示场景不可接受。已实测：加覆盖索引 (date_id, behavior_type, user_id)
--   反而更慢（6.96s → 8.79s，且索引体积拖累全表扫描），属"索引解决不了
--   的聚合代价"。正确做法是在 ADS 层一次性物化——这也符合本项目
--   ODS → DWD → DWS → ADS 的分层初衷：请求时不应直接聚合明细事实表。

-- 口径定义：
--   年月 ym = substr(date_id, 1, 6)，来自行为/订单发生日期
--   活跃/浏览/收藏/加购 UV = COUNT(DISTINCT user_id)，去重后可直接累加到人
--   下单/支付 UV 也在此物化，但来源为 fact_orders
--
-- ⚠ 跨源声明（务必阅读）：
--   行为域（淘宝子采样）与订单域（Amazon 评论推导）是两个**不相交的用户空间**，
--   且时间窗几乎不重叠（行为域集中在 2017-11/12，订单域自 2018-01 起）。
--   因此本表**不做**行为用户 ∩ 订单用户的交叉去重，也不提供跨源转化率：
--   下游计算比率时，分母必须取同域 UV（加购率 = cart_users/browse_users，
--   支付转化率不可用 browse 作分母）。原实现用 /browse 计算转化率曾得到
--   52700% 的失真值，即源于此。

CREATE TABLE IF NOT EXISTS ads_behavior_monthly (
    ym              TEXT PRIMARY KEY,   -- 'YYYYMM'
    total_users     INTEGER DEFAULT 0,  -- 行为域活跃 UV（去重）
    browse_users    INTEGER DEFAULT 0,  -- 浏览 UV
    collect_users   INTEGER DEFAULT 0,  -- 收藏 UV
    cart_users      INTEGER DEFAULT 0,  -- 加购 UV
    behavior_pv     INTEGER DEFAULT 0,  -- 行为总条数（非去重）
    order_users     INTEGER DEFAULT 0,  -- 订单域下单 UV（跨源，不可与上面交叉去重）
    paid_users      INTEGER DEFAULT 0,  -- 订单域支付 UV
    order_amount    REAL    DEFAULT 0,  -- 订单域成交额
    etl_update_time TEXT
);

-- 派生列（比率）不落表：比率依赖"是否允许跨源"的策略口径，属于展示层决策，
-- 落表会把策略固化。统一由 dashboard/server.py 的 _rate() 按分母是否为 0
-- 决定是否返回 null（断线）。

CREATE INDEX IF NOT EXISTS idx_abm_ym ON ads_behavior_monthly(ym);
