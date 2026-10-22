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
