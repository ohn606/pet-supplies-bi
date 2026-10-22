-- ============================================================
-- 宠物用品电商数据仓库 - 星型模型 DDL
-- 技术栈: SQLite / 可迁移至 Hive/Spark SQL
-- ============================================================

-- ------------------------------------------------------------
-- 维度表
-- ------------------------------------------------------------

-- 时间维度表 (日期维)
CREATE TABLE IF NOT EXISTS dim_date (
    date_id          INTEGER PRIMARY KEY,       -- 日期代理键 YYYYMMDD
    date_str         TEXT NOT NULL,              -- 日期字符串 '2025-07-01'
    year             INTEGER NOT NULL,
    quarter          INTEGER NOT NULL,           -- 1-4
    month            INTEGER NOT NULL,           -- 1-12
    month_name       TEXT NOT NULL,              -- '7月'
    week_of_year     INTEGER NOT NULL,
    day_of_month     INTEGER NOT NULL,
    day_of_week      INTEGER NOT NULL,           -- 0=周日
    day_name         TEXT NOT NULL,              -- '周一'
    is_weekend       INTEGER NOT NULL DEFAULT 0,
    is_holiday       INTEGER NOT NULL DEFAULT 0,
    holiday_name     TEXT,
    year_month       TEXT NOT NULL,              -- '2025-07'
    quarter_label    TEXT NOT NULL               -- '2025Q3'
);

-- 用户维度表
CREATE TABLE IF NOT EXISTS dim_user (
    user_id          TEXT PRIMARY KEY,
    nickname         TEXT,
    age              INTEGER,
    gender           TEXT,
    city             TEXT,
    city_tier        TEXT,                       -- 一线/新一线/二线
    pet_type         TEXT,                       -- 猫/狗/猫和狗/小宠
    pet_age_group    TEXT,
    income_level     TEXT,
    register_date    TEXT,
    register_source  TEXT,
    is_member        INTEGER DEFAULT 0,
    activity_level   TEXT,                       -- 高活跃/中活跃/低活跃
    consume_level    TEXT,                       -- 高消费/中消费/低消费
    first_order_date TEXT,                       -- 首购日期
    last_order_date  TEXT,                       -- 末购日期
    lifetime_orders  INTEGER DEFAULT 0,          -- 累计订单数
    lifetime_amount  REAL DEFAULT 0.0,           -- 累计消费金额
    avg_order_amount REAL DEFAULT 0.0,           -- 平均客单价
    rfm_r            INTEGER DEFAULT 1,          -- RFM-Recency
    rfm_f            INTEGER DEFAULT 1,          -- RFM-Frequency
    rfm_m            INTEGER DEFAULT 1,          -- RFM-Monetary
    rfm_score        INTEGER DEFAULT 3,          -- RFM总分
    rfm_segment      TEXT DEFAULT '一般客户',     -- RFM分层
    etl_update_time  TEXT                        -- ETL更新时间
);

-- 商品维度表
CREATE TABLE IF NOT EXISTS dim_product (
    product_id       TEXT PRIMARY KEY,
    product_name     TEXT,
    main_category    TEXT,
    sub_category     TEXT,
    leaf_category    TEXT,
    brand            TEXT,
    price            REAL,
    quality_score    REAL,
    stock            INTEGER,
    list_date        TEXT,
    shelf_status     INTEGER DEFAULT 1,
    description      TEXT,
    image_count      INTEGER DEFAULT 0,
    has_video        INTEGER DEFAULT 0,
    total_sales      INTEGER DEFAULT 0,          -- 累计销量
    total_revenue    REAL DEFAULT 0.0,           -- 累计收入
    avg_rating       REAL DEFAULT 0.0,           -- 平均评分
    review_count     INTEGER DEFAULT 0,          -- 评价数
    conversion_rate  REAL DEFAULT 0.0,           -- 转化率
    image_url        TEXT,                       -- 商品主图 URL（派生态，见下方说明）
    etl_update_time  TEXT
);

-- image_url 说明（v1.7.0 新增）：
--   1) 属派生列，非源数据直采。源数据（amazon_reviews.csv 的 image 列）仅提供
--      原始 URL 数组，ETL 阶段被降级为 image_count/has_video 计数，URL 被丢弃。
--   2) 由 etl/build_product_images.py 按 ODS 同款哈希规则回填，并做尺寸升级
--      （_SS40_ → _SS400_）与 CDN 域名迁移（images-na.ssl-images-amazon.com
--      → m.media-amazon.com）。
--   3) 可追溯性：原始 URL 与来源存于 data/raw/product_images.csv
--      （字段 product_id / image_url / image_url_src / image_source）。
--   4) 覆盖率：全量 17,775 件有图，动销商品覆盖 85.4%，热销 TOP10 覆盖 100%。
--      无图商品前端回退为类目插画，不臆造图片。

-- ============================================================
-- 事实表
-- ============================================================

-- 订单事实表
CREATE TABLE IF NOT EXISTS fact_orders (
    order_id         TEXT PRIMARY KEY,
    order_date_id    INTEGER NOT NULL,           -- 关联 dim_date
    user_id          TEXT NOT NULL,              -- 关联 dim_user
    total_amount     REAL DEFAULT 0.0,
    discount_amount  REAL DEFAULT 0.0,
    actual_amount    REAL DEFAULT 0.0,
    item_count       INTEGER DEFAULT 1,          -- 商品种类数
    total_quantity   INTEGER DEFAULT 1,          -- 商品总数量
    is_promo         INTEGER DEFAULT 0,
    promo_name       TEXT,
    pay_method       TEXT,
    order_status     TEXT,
    order_source     TEXT,
    shipping_city    TEXT,
    order_hour       INTEGER,                    -- 下单小时(0-23)
    is_first_order   INTEGER DEFAULT 0,          -- 是否首单
    FOREIGN KEY (order_date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (user_id) REFERENCES dim_user(user_id)
);

-- 订单明细事实表 (事实表的明细层)
CREATE TABLE IF NOT EXISTS fact_order_items (
    item_id          TEXT PRIMARY KEY,
    order_id         TEXT NOT NULL,
    order_date_id    INTEGER NOT NULL,
    user_id          TEXT NOT NULL,
    product_id       TEXT NOT NULL,
    quantity         INTEGER DEFAULT 1,
    unit_price       REAL DEFAULT 0.0,
    item_amount      REAL DEFAULT 0.0,
    is_gift          INTEGER DEFAULT 0,
    FOREIGN KEY (order_id) REFERENCES fact_orders(order_id),
    FOREIGN KEY (order_date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (user_id) REFERENCES dim_user(user_id),
    FOREIGN KEY (product_id) REFERENCES dim_product(product_id)
);

-- 用户行为事实表
CREATE TABLE IF NOT EXISTS fact_user_behavior (
    log_id           TEXT PRIMARY KEY,
    date_id          INTEGER NOT NULL,           -- 关联 dim_date
    user_id          TEXT NOT NULL,              -- 关联 dim_user
    product_id       TEXT NOT NULL,              -- 关联 dim_product
    behavior_type    TEXT NOT NULL,              -- 浏览/收藏/加购/搜索/分享/评价
    behavior_hour    INTEGER,
    device_type      TEXT,
    duration_seconds INTEGER DEFAULT 0,
    session_id       TEXT,
    ip_region        TEXT,
    -- 说明（多源融合场景）：行为事实表主要来自外源行为日志系统（淘宝 UserBehavior），
    -- 其用户/商品实体空间与主数据域（Amazon 宠物商品域）不同，采用"松耦合引用"：
    -- 仅对时间维度强制外键，用户/商品维度以 data_source 字段做跨源关联，不强制外键约束，
    -- 从而支持异构数据源融合而无需人为伪造实体映射。
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
);

-- 评价事实表
CREATE TABLE IF NOT EXISTS fact_reviews (
    review_id        TEXT PRIMARY KEY,
    date_id          INTEGER NOT NULL,
    user_id          TEXT NOT NULL,
    product_id       TEXT NOT NULL,
    order_id         TEXT,
    rating           INTEGER NOT NULL,           -- 1-5
    review_content   TEXT,
    has_image        INTEGER DEFAULT 0,
    has_video        INTEGER DEFAULT 0,
    is_anonymous     INTEGER DEFAULT 0,
    helpful_count    INTEGER DEFAULT 0,
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (user_id) REFERENCES dim_user(user_id),
    FOREIGN KEY (product_id) REFERENCES dim_product(product_id)
);

-- ============================================================
-- 聚合表 (ADS层 - 应用数据服务层)
-- ============================================================

-- 每日销售汇总
CREATE TABLE IF NOT EXISTS ads_daily_sales (
    date_id          INTEGER PRIMARY KEY,
    order_count      INTEGER DEFAULT 0,
    user_count       INTEGER DEFAULT 0,
    total_amount     REAL DEFAULT 0.0,
    actual_amount    REAL DEFAULT 0.0,
    discount_amount  REAL DEFAULT 0.0,
    avg_order_amount REAL DEFAULT 0.0,
    new_user_count   INTEGER DEFAULT 0,
    promo_order_count INTEGER DEFAULT 0
);

-- 每日行为汇总
CREATE TABLE IF NOT EXISTS ads_daily_behavior (
    date_id             INTEGER PRIMARY KEY,
    pv                  INTEGER DEFAULT 0,       -- 浏览量
    uv                  INTEGER DEFAULT 0,       -- 访客数
    collect_count       INTEGER DEFAULT 0,       -- 收藏数
    cart_add_count      INTEGER DEFAULT 0,       -- 加购数
    search_count        INTEGER DEFAULT 0,       -- 搜索数
    share_count         INTEGER DEFAULT 0,       -- 分享数
    avg_duration        REAL DEFAULT 0.0,        -- 平均停留时长
    bounce_count        INTEGER DEFAULT 0,        -- 跳出(只浏览一个商品)
    conversion_rate     REAL DEFAULT 0.0         -- 浏览到下单转化率
);

-- 类目销售汇总
CREATE TABLE IF NOT EXISTS ads_category_sales (
    date_id          INTEGER,
    main_category    TEXT,
    sub_category     TEXT,
    order_count      INTEGER DEFAULT 0,
    sales_quantity   INTEGER DEFAULT 0,
    sales_amount     REAL DEFAULT 0.0,
    user_count       INTEGER DEFAULT 0,
    avg_rating       REAL DEFAULT 0.0,
    PRIMARY KEY (date_id, main_category, sub_category)
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_fo_date ON fact_orders(order_date_id);
CREATE INDEX IF NOT EXISTS idx_fo_user ON fact_orders(user_id);
CREATE INDEX IF NOT EXISTS idx_foi_order ON fact_order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_foi_product ON fact_order_items(product_id);
CREATE INDEX IF NOT EXISTS idx_fub_date ON fact_user_behavior(date_id);
CREATE INDEX IF NOT EXISTS idx_fub_user ON fact_user_behavior(user_id);
CREATE INDEX IF NOT EXISTS idx_fub_product ON fact_user_behavior(product_id);
CREATE INDEX IF NOT EXISTS idx_fr_date ON fact_reviews(date_id);
CREATE INDEX IF NOT EXISTS idx_fr_product ON fact_reviews(product_id);
