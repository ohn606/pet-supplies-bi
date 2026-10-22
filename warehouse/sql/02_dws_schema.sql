-- ============================================================================
-- DWS 汇总服务层 Schema（v1.5.0 P2-1）
-- 位置：介于 DWD 事实明细 与 ADS 应用聚合 之间的"主题宽表"层
-- 命名规范：dws_<主题>_<粒度>；粒度后缀 _1d = 天粒度明细汇总，_cum = 截至最新全量累计快照
-- 字段规范：snake_case；主键=(粒度键)；金额统一 item_amount 口径（含折扣后实付）
-- 口径铁律：有效订单 = order_status NOT IN ('已退款','已取消')（与 ads_daily_sales 同一口径）
-- 依赖血缘（DWD → DWS）：
--   fact_user_behavior ────────────────→ dws_user_behavior_1d
--   fact_order_items ⋈ fact_orders ───→ dws_user_purchase_1d
--                                      └→ dws_product_sales_1d
--   dws_user_purchase_1d ⋈ dim_product → dws_user_category_cum
-- 下游消费（DWS → ADS/应用）：
--   dws_user_behavior_1d  → ads_daily_behavior / 留存 / 漏斗 / 热力图
--   dws_product_sales_1d  → ads_daily_sales / ads_category_sales / 热销榜
--   dws_user_purchase_1d  → RFM / 推荐评分矩阵
--   dws_user_category_cum → Co-Score 特征 / 宠物生命周期证据 / 补货候选
-- 构建与刷新：etl/build_dws.py（全量幂等重建，ETL 完成后执行）
-- ============================================================================

DROP TABLE IF EXISTS dws_user_behavior_1d;
CREATE TABLE dws_user_behavior_1d (
    date_id          INTEGER NOT NULL,          -- 行为日期（dim_date.date_id，yyyymmdd）
    user_id          TEXT    NOT NULL,          -- 用户（dim_user.user_id）
    pv               INTEGER NOT NULL DEFAULT 0,-- 浏览次数（behavior_type='浏览'）
    fav_cnt          INTEGER NOT NULL DEFAULT 0,-- 收藏次数
    cart_cnt         INTEGER NOT NULL DEFAULT 0,-- 加购次数
    search_cnt       INTEGER NOT NULL DEFAULT 0,-- 搜索次数
    share_cnt        INTEGER NOT NULL DEFAULT 0,-- 分享次数
    total_actions    INTEGER NOT NULL DEFAULT 0,-- 行为总次数（上行四类+浏览）
    avg_duration     REAL    NOT NULL DEFAULT 0,-- 浏览平均停留秒数（仅浏览行为）
    PRIMARY KEY (date_id, user_id)
);

DROP TABLE IF EXISTS dws_user_purchase_1d;
CREATE TABLE dws_user_purchase_1d (
    date_id          INTEGER NOT NULL,          -- 下单日期（fact_orders.order_date_id）
    user_id          TEXT    NOT NULL,          -- 下单用户
    order_cnt        INTEGER NOT NULL DEFAULT 0,-- 有效订单数（剔除退款/取消）
    sku_cnt          INTEGER NOT NULL DEFAULT 0,-- 购买商品 SKU 数（distinct product_id）
    quantity         INTEGER NOT NULL DEFAULT 0,-- 购买件数合计
    amount           REAL    NOT NULL DEFAULT 0,-- 实付金额（item_amount 合计）
    PRIMARY KEY (date_id, user_id)
);

DROP TABLE IF EXISTS dws_product_sales_1d;
CREATE TABLE dws_product_sales_1d (
    date_id          INTEGER NOT NULL,          -- 下单日期
    product_id       TEXT    NOT NULL,          -- 商品（dim_product.product_id）
    main_category    TEXT    NOT NULL,          -- 一级类目（冗余自 dim_product，构建日快照）
    sub_category     TEXT    NOT NULL,          -- 二级类目（同上）
    order_cnt        INTEGER NOT NULL DEFAULT 0,-- 有效订单数
    buyer_cnt        INTEGER NOT NULL DEFAULT 0,-- 购买人数（distinct user）
    quantity         INTEGER NOT NULL DEFAULT 0,-- 销量件数
    amount           REAL    NOT NULL DEFAULT 0,-- 销售额（实付）
    PRIMARY KEY (date_id, product_id)
);

DROP TABLE IF EXISTS dws_user_category_cum;
CREATE TABLE dws_user_category_cum (
    user_id              TEXT NOT NULL,         -- 用户
    sub_category         TEXT NOT NULL,         -- 二级类目
    order_cnt            INTEGER NOT NULL DEFAULT 0, -- 有效订单数
    quantity             INTEGER NOT NULL DEFAULT 0, -- 累计件数
    amount               REAL    NOT NULL DEFAULT 0, -- 累计实付金额
    first_purchase_date_id INTEGER,                  -- 首次购买该类目日期
    last_purchase_date_id  INTEGER,                  -- 最近购买该类目日期
    PRIMARY KEY (user_id, sub_category)
);

CREATE INDEX IF NOT EXISTS idx_dws_ubehavior_user  ON dws_user_behavior_1d(user_id);
CREATE INDEX IF NOT EXISTS idx_dws_upurch_user     ON dws_user_purchase_1d(user_id);
CREATE INDEX IF NOT EXISTS idx_dws_psales_product  ON dws_product_sales_1d(product_id);
CREATE INDEX IF NOT EXISTS idx_dws_psales_cat      ON dws_product_sales_1d(main_category, date_id);
CREATE INDEX IF NOT EXISTS idx_dws_uccum_user      ON dws_user_category_cum(user_id);
CREATE INDEX IF NOT EXISTS idx_dws_uccum_cat       ON dws_user_category_cum(sub_category);
