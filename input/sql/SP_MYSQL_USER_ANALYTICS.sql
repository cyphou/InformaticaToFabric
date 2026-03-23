-- ============================================================
-- Stored Procedure: SP_MYSQL_USER_ANALYTICS
-- Source DB: MySQL 8.x
-- Purpose: User engagement analytics with MySQL-specific SQL
-- Tests: IFNULL, NOW(), CURDATE(), GROUP_CONCAT, DATE_FORMAT,
--        STR_TO_DATE, LIMIT, AUTO_INCREMENT, backtick identifiers,
--        UNSIGNED, ENGINE=InnoDB, DEFAULT CHARSET, TINYINT(1), ENUM
-- ============================================================

-- Step 1: Create staging table with MySQL-specific types
CREATE TABLE IF NOT EXISTS `analytics`.`user_engagement_stage` (
    `id` BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    `user_id` INT UNSIGNED NOT NULL,
    `username` VARCHAR(100) NOT NULL,
    `engagement_score` DECIMAL(8,2) DEFAULT 0.00,
    `is_active` TINYINT(1) NOT NULL DEFAULT 1,
    `user_tier` ENUM('FREE', 'BASIC', 'PREMIUM', 'ENTERPRISE') DEFAULT 'FREE',
    `last_login` DATETIME DEFAULT NULL,
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX `idx_user_tier` (`user_tier`),
    INDEX `idx_active` (`is_active`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Step 2: Populate engagement metrics
INSERT INTO `analytics`.`user_engagement_stage`
    (`user_id`, `username`, `engagement_score`, `is_active`, `user_tier`, `last_login`)
SELECT
    u.`user_id`,
    u.`username`,
    IFNULL(
        (SELECT COUNT(*) FROM `events`.`page_views` pv
         WHERE pv.`user_id` = u.`user_id`
           AND pv.`event_date` >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)),
        0
    ) * 0.5
    + IFNULL(
        (SELECT COUNT(*) FROM `events`.`purchases` p
         WHERE p.`user_id` = u.`user_id`
           AND p.`purchase_date` >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)),
        0
    ) * 2.0 AS engagement_score,
    IF(u.`last_login` >= DATE_SUB(NOW(), INTERVAL 90 DAY), 1, 0) AS is_active,
    u.`tier` AS user_tier,
    u.`last_login`
FROM `users`.`user_profiles` u
WHERE u.`status` != 'DELETED'
LIMIT 100000;

-- Step 3: Aggregated cohort analysis with GROUP_CONCAT
SELECT
    DATE_FORMAT(u.`registration_date`, '%Y-%m') AS cohort_month,
    u.`user_tier`,
    COUNT(*) AS user_count,
    AVG(e.`engagement_score`) AS avg_engagement,
    GROUP_CONCAT(DISTINCT u.`country` ORDER BY u.`country` SEPARATOR ', ') AS countries,
    GROUP_CONCAT(
        CASE WHEN e.`engagement_score` > 50 THEN u.`username` END
        ORDER BY e.`engagement_score` DESC
        SEPARATOR '; '
    ) AS top_users,
    IFNULL(SUM(CASE WHEN e.`is_active` = 1 THEN 1 ELSE 0 END), 0) AS active_count,
    IFNULL(SUM(CASE WHEN e.`is_active` = 0 THEN 1 ELSE 0 END), 0) AS inactive_count
FROM `users`.`user_profiles` u
LEFT JOIN `analytics`.`user_engagement_stage` e
    ON u.`user_id` = e.`user_id`
WHERE u.`registration_date` >= STR_TO_DATE('2023-01-01', '%Y-%m-%d')
GROUP BY DATE_FORMAT(u.`registration_date`, '%Y-%m'), u.`user_tier`
ORDER BY cohort_month DESC, user_count DESC
LIMIT 500;

-- Step 4: Date conversion examples
SELECT
    `user_id`,
    `username`,
    DATE_FORMAT(`last_login`, '%d/%m/%Y %H:%i:%s') AS last_login_formatted,
    DATE_FORMAT(`registration_date`, '%W, %M %d, %Y') AS registration_display,
    STR_TO_DATE(CONCAT(YEAR(CURDATE()), '-01-01'), '%Y-%m-%d') AS year_start,
    DATEDIFF(CURDATE(), `last_login`) AS days_inactive,
    TIMESTAMPDIFF(MONTH, `registration_date`, NOW()) AS months_since_reg
FROM `users`.`user_profiles`
WHERE `is_active` = 1
  AND `last_login` IS NOT NULL
ORDER BY `last_login` DESC
LIMIT 1000;

-- Step 5: Cleanup staging
DROP TABLE IF EXISTS `analytics`.`user_engagement_stage`;
