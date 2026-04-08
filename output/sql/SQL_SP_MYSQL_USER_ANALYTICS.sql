-- ============================================================================
-- Converted from: C:\Users\pidoudet\OneDrive - Microsoft\Boulot\PBI SME\OracleToPostgre\InformaticaToDBFabric\input\sql\SP_MYSQL_USER_ANALYTICS.sql
-- DB Type: MYSQL
-- Conversion: MySQL → Spark SQL
-- Date: 2026-04-07
-- Agent: sql-migration (automated)
-- ============================================================================
-- Review all TODO comments before deploying to Fabric.
-- ============================================================================

-- ============================================================
-- Stored Procedure: SP_MYSQL_USER_ANALYTICS
-- Source DB: MySQL 8.x
-- Purpose: User engagement analytics with MySQL-specific SQL
-- Tests: IFNULL, current_timestamp(), current_date(), GROUP_CONCAT, DATE_FORMAT,
--        STR_TO_DATE, LIMIT, -- AUTO_INCREMENT removed (use monotonically_increasing_id() in PySpark), backtick identifiers,
--        , , DEFAULT CHARSET, BOOLEAN, ENUM
-- ============================================================

-- Step 1: Create staging table with MySQL-specific types
CREATE TABLE IF NOT EXISTS analytics.user_engagement_stage (
    id BIGINT  -- AUTO_INCREMENT removed (use monotonically_increasing_id() in PySpark) PRIMARY KEY,
    user_id INT  NOT NULL,
    username VARCHAR(100) NOT NULL,
    engagement_score DECIMAL(8,2) DEFAULT 0.00,
    is_active BOOLEAN NOT NULL DEFAULT 1,
    user_tier STRING DEFAULT 'FREE',
    last_login TIMESTAMP DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user_tier (user_tier),
    INDEX idx_active (is_active)
)   COLLATE=utf8mb4_unicode_ci;

-- Step 2: Populate engagement metrics
INSERT INTO analytics.user_engagement_stage
    (user_id, username, engagement_score, is_active, user_tier, last_login)
SELECT
    u.user_id,
    u.username,
    COALESCE(
        (SELECT COUNT(*) FROM events.page_views pv
         WHERE pv.user_id = u.user_id
           AND pv.event_date >= DATE_SUB(current_date(), INTERVAL 30 DAY)),
        0
    ) * 0.5
    + COALESCE(
        (SELECT COUNT(*) FROM events.purchases p
         WHERE p.user_id = u.user_id
           AND p.purchase_date >= DATE_SUB(current_date(), INTERVAL 30 DAY)),
        0
    ) * 2.0 AS engagement_score,
    IF(u.last_login >= DATE_SUB(current_timestamp(), INTERVAL 90 DAY), 1, 0) AS is_active,
    u.tier AS user_tier,
    u.last_login
FROM users.user_profiles u
WHERE u.status != 'DELETED'
LIMIT 100000;

-- Step 3: Aggregated cohort analysis with GROUP_CONCAT
SELECT
    date_format(u.registration_date, '%Y-%m') AS cohort_month,
    u.user_tier,
    COUNT(*) AS user_count,
    AVG(e.engagement_score) AS avg_engagement,
    CONCAT_WS(',', COLLECT_LIST(DISTINCT u.country ORDER BY u.country SEPARATOR ', ')) AS countries,
    CONCAT_WS(',', COLLECT_LIST(
        CASE WHEN e.engagement_score > 50 THEN u.username END
        ORDER BY e.engagement_score DESC
        SEPARATOR '; '
    )) AS top_users,
    COALESCE(SUM(CASE WHEN e.is_active = 1 THEN 1 ELSE 0 END), 0) AS active_count,
    COALESCE(SUM(CASE WHEN e.is_active = 0 THEN 1 ELSE 0 END), 0) AS inactive_count
FROM users.user_profiles u
LEFT JOIN analytics.user_engagement_stage e
    ON u.user_id = e.user_id
WHERE u.registration_date >= to_date('2023-01-01', '%Y-%m-%d')
GROUP BY date_format(u.registration_date, '%Y-%m'), u.user_tier
ORDER BY cohort_month DESC, user_count DESC
LIMIT 500;

-- Step 4: Date conversion examples
SELECT
    user_id,
    username,
    date_format(last_login, '%d/%m/%Y %H:%i:%s') AS last_login_formatted,
    date_format(registration_date, '%W, %M %d, %Y') AS registration_display,
    to_date(CONCAT(YEAR(current_date()), '-01-01'), '%Y-%m-%d') AS year_start,
    DATEDIFF(current_date(), last_login) AS days_inactive,
    TIMESTAMPDIFF(MONTH, registration_date, current_timestamp()) AS months_since_reg
FROM users.user_profiles
WHERE is_active = 1
  AND last_login IS NOT NULL
ORDER BY last_login DESC
LIMIT 1000;

-- Step 5: Cleanup staging
DROP TABLE IF EXISTS analytics.user_engagement_stage;
