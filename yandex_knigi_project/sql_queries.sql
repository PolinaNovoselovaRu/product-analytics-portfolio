-- 1. MAU авторов (топ-3 в ноябре)

SELECT 
    a.main_author_name,
    COUNT(DISTINCT au.puid) AS mau
FROM bookmate.audition au
JOIN bookmate.content c ON au.main_content_id = c.main_content_id
JOIN bookmate.author a ON c.main_author_id = a.main_author_id
WHERE au.msk_business_dt_str >= '2024-11-01' 
    AND au.msk_business_dt_str < '2024-12-01'
GROUP BY a.main_author_name
ORDER BY mau DESC
LIMIT 3;


-- 2. MAU произведений (топ-3 в ноябре)

SELECT 
    c.main_content_name,
    c.published_topic_title_list,
    a.main_author_name,
    COUNT(DISTINCT au.puid) AS mau
FROM bookmate.audition au
JOIN bookmate.content c ON au.main_content_id = c.main_content_id
JOIN bookmate.author a ON c.main_author_id = a.main_author_id
WHERE au.msk_business_dt_str >= '2024-11-01' 
    AND au.msk_business_dt_str < '2024-12-01'
GROUP BY 
    c.main_content_name,
    c.published_topic_title_list,
    a.main_author_name
ORDER BY mau DESC
LIMIT 3;


-- 3. Retention Rate (когорта 2 декабря 2024)

WITH cohort AS (
    SELECT DISTINCT puid
    FROM bookmate.audition
    WHERE DATE(msk_business_dt_str) = '2024-12-02'
)
SELECT 
    DATE(a.msk_business_dt_str) - DATE('2024-12-02') AS day_since_install,
    COUNT(DISTINCT a.puid) AS retained_users,
    ROUND(
        COUNT(DISTINCT a.puid) * 1.0 / MAX(COUNT(DISTINCT a.puid)) OVER (), 
        2
    ) AS retention_rate
FROM bookmate.audition a
JOIN cohort c ON a.puid = c.puid
WHERE DATE(a.msk_business_dt_str) >= '2024-12-02'
GROUP BY DATE(a.msk_business_dt_str) - DATE('2024-12-02')
ORDER BY day_since_install;


-- 4. LTV по городам (Москва и Санкт-Петербург)

WITH city_activity AS (
    SELECT
        g.usage_geo_id_name AS city,
        au.puid,
        COUNT(DISTINCT DATE_TRUNC('month', DATE(au.msk_business_dt_str))) AS active_months
    FROM bookmate.audition au
    JOIN bookmate.geo g ON au.usage_geo_id = g.usage_geo_id
    WHERE g.usage_geo_id_name IN ('Москва', 'Санкт-Петербург')
    GROUP BY g.usage_geo_id_name, au.puid
)
SELECT
    city,
    COUNT(puid) AS total_users,
    ROUND(
        SUM(active_months) * 399.0 / COUNT(puid),
        2
    ) AS ltv
FROM city_activity
GROUP BY city
ORDER BY city;


-- 5. Средняя выручка за час (сентябрь-ноябрь)

WITH monthly_stats AS (
    SELECT 
        DATE_TRUNC('month', DATE(msk_business_dt_str))::DATE AS month,
        COUNT(DISTINCT puid) AS mau,
        SUM(hours) AS total_hours
    FROM bookmate.audition
    WHERE DATE(msk_business_dt_str) >= '2024-09-01' 
        AND DATE(msk_business_dt_str) < '2024-12-01'
    GROUP BY DATE_TRUNC('month', DATE(msk_business_dt_str))
)
SELECT 
    month,
    mau,
    ROUND(total_hours, 2) AS hours,
    ROUND((mau * 399.0) / total_hours, 2) AS avg_hour_rev
FROM monthly_stats
ORDER BY month;


-- 6. Данные для проверки гипотезы (активность по городам)

SELECT 
    g.usage_geo_id_name AS city,
    au.puid,
    SUM(au.hours) AS hours
FROM bookmate.audition au
JOIN bookmate.geo g ON au.usage_geo_id = g.usage_geo_id
WHERE g.usage_geo_id_name IN ('Москва', 'Санкт-Петербург')
GROUP BY g.usage_geo_id_name, au.puid
ORDER BY city, puid;
