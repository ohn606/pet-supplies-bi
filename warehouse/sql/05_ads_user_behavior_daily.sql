-- ============================================================
-- ADS 层：用户-日-行为 粒度汇总表（v1.8.1）
-- ------------------------------------------------------------
-- 血缘（DWD → ADS）：
--     fact_user_behavior ──→ ads_user_behavior_daily ──→ 用户分析页 KPI / 漏斗 / 热力图
--
-- 为什么要有这张表：
--     fact_user_behavior 共 2,648,323 行，其中大量行是同一 (日, 用户, 行为)
--     的重复事件（尤其是浏览）。`COUNT(DISTINCT user_id)` 每次请求都要扫全表
--     并建临时去重 b-tree，实测 /api/user_summary 单次 **10.0s** —— 部署到公网
--     后首屏必然超时级卡顿。
--     本表把粒度收敛到 (date_id, user_id, behavior_type) = 378,632 行（**7× 压缩**），
--     语义完全等价：
--         明细行 COUNT(*)                == 汇总行 SUM(cnt)
--         明细行 COUNT(DISTINCT user_id) == 汇总行 COUNT(DISTINCT user_id)
--     实测同一查询 10.0s → 0.57s（**约 17 倍**）。
--
-- ⚠ 数据铁律：本表仅做**去重和上卷**，不新增、不臆造任何用户或事件。
-- ⚠ 跨源：行为域（淘宝）与交易域（Amazon）用户空间不相交，本表只服务
--   单一行为域，不做任何跨源比率。
-- ============================================================

CREATE TABLE IF NOT EXISTS ads_user_behavior_daily (
    date_id        INTEGER NOT NULL,   -- YYYYMMDD，与 fact_user_behavior.date_id 同源
    user_id        TEXT    NOT NULL,   -- 用户标识（行为域）
    behavior_type  TEXT    NOT NULL,   -- 浏览 / 收藏 / 加购 / 下单 / 评价
    cnt            INTEGER NOT NULL,   -- 该用户当日的该类行为次数
    PRIMARY KEY (date_id, user_id, behavior_type)
);

-- COUNT(DISTINCT) + 日期区间 + 行为类型过滤的最佳顺序：
-- 先按 behavior_type 收敛（5 类），再按 date_id 区间扫描，user_id 直接从索引去重，
-- 避免回表。实测优于 (date_id, behavior_type, user_id) 顺序。
CREATE INDEX IF NOT EXISTS idx_aubd_type_date_user
    ON ads_user_behavior_daily(behavior_type, date_id, user_id);

-- 表用途注释（sqlite 无 COMMENT 语法，存元数据表便于后续维护者理解）
CREATE TABLE IF NOT EXISTS ads_table_meta (
    table_name  TEXT PRIMARY KEY,
    grain       TEXT,
    source      TEXT,
    purpose     TEXT,
    created_ver TEXT
);
INSERT OR REPLACE INTO ads_table_meta VALUES
    ('ads_user_behavior_daily',
     'date_id × user_id × behavior_type',
     'fact_user_behavior',
     '用户分析页 COUNT(DISTINCT) 类查询的预聚合体，2.65M 行压缩至 379K 行',
     'v1.8.1');

-- ------------------------------------------------------------
-- 24 小时活跃分布预聚合表
-- ------------------------------------------------------------
-- /api/heatmap 与 /api/time_analysis 原为对 264 万行明细做
-- `GROUP BY behavior_hour` + 24 个 COUNT(DISTINCT user_id)，实测 **4.7s**。
-- 该分布本身与时间筛选无关（是全周期时段画像），24 行结果完全可以物化：
-- 物化后读取成本 ≈ 0ms。
CREATE TABLE IF NOT EXISTS ads_behavior_hourly (
    behavior_hour INTEGER PRIMARY KEY,   -- 0-23
    cnt           INTEGER NOT NULL,      -- 行为事件数
    uv            INTEGER NOT NULL       -- 去重用户数
);

INSERT OR REPLACE INTO ads_table_meta VALUES
    ('ads_behavior_hourly',
     'behavior_hour',
     'fact_user_behavior',
     '24 小时活跃分布预聚合（全周期口径，24 行），替代 4.7s 的实时明细聚合',
     'v1.8.1');
