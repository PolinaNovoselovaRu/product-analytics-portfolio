-- ПРОЕКТ: Анализ рынка недвижимости Санкт-Петербурга и Ленинградской области
-- Задачи: 1. Время активности объявлений
--         2. Сезонность объявлений


-- ============================================================
-- ЧАСТЬ 1. Фильтрация выбросов (общее CTE для всех запросов)
-- ============================================================

WITH limits AS (
    SELECT  
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY total_area) AS total_area_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY rooms) AS rooms_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY balcony) AS balcony_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_h,
        PERCENTILE_DISC(0.01) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_l
    FROM real_estate.flats     
),
filtered_ids AS (
    SELECT id
    FROM real_estate.flats  
    WHERE 
        total_area < (SELECT total_area_limit FROM limits)
        AND (rooms < (SELECT rooms_limit FROM limits) OR rooms IS NULL)
        AND (balcony < (SELECT balcony_limit FROM limits) OR balcony IS NULL)
        AND ((ceiling_height < (SELECT ceiling_height_limit_h FROM limits)
            AND ceiling_height > (SELECT ceiling_height_limit_l FROM limits)) OR ceiling_height IS NULL)
)


-- ============================================================
-- ЗАДАЧА 1. Время активности объявлений
-- ============================================================
-- Анализ: какие типы квартир продаются быстро, какие - долго
-- Разбивка на категории: 1-30, 31-90, 91-180, 181+ дней, non category
-- Сравнение Санкт-Петербурга и городов Ленинградской области
-- Период: 2015-2018 годы, только города

WITH ad_categories AS (
    SELECT 
        a.id,
        a.days_exposition,
        CASE 
            WHEN a.days_exposition BETWEEN 1 AND 30 THEN '1-30 days'
            WHEN a.days_exposition BETWEEN 31 AND 90 THEN '31-90 days'
            WHEN a.days_exposition BETWEEN 91 AND 180 THEN '91-180 days'
            WHEN a.days_exposition >= 181 THEN '181+ days'
            ELSE 'non category'
        END AS time_category,
        f.total_area,
        f.rooms,
        f.balcony,
        f.kitchen_area,
        f.floor,
        f.floors_total,
        f.is_apartment,
        f.open_plan,
        c.city,
        t.type,
        -- Стоимость квадратного метра
        a.last_price / f.total_area AS price_per_sqm
    FROM real_estate.advertisement a
    JOIN filtered_ids fi ON a.id = fi.id
    JOIN real_estate.flats f ON a.id = f.id
    JOIN real_estate.city c ON f.city_id = c.city_id
    JOIN real_estate.type t ON f.type_id = t.type_id
    WHERE 
        -- Только города
        t.type = 'город'
        -- Полные годы 2015-2018
        AND EXTRACT(YEAR FROM a.first_day_exposition) IN (2015, 2016, 2017, 2018)
        -- Только снятые объявления (кроме non category)
        AND a.days_exposition IS NOT NULL
)
-- Агрегация по категориям и региону
SELECT 
    CASE 
        WHEN c.city = 'Санкт-Петербург' THEN 'Санкт-Петербург'
        ELSE 'Ленинградская область'
    END AS region,
    ac.time_category,
    COUNT(DISTINCT ac.id) AS flats_count,
    ROUND(AVG(ac.price_per_sqm), 2) AS avg_price_per_sqm,
    ROUND(AVG(ac.total_area), 2) AS avg_total_area,
    ROUND(AVG(ac.rooms), 1) AS avg_rooms,
    ROUND(AVG(ac.balcony), 1) AS avg_balcony,
    ROUND(AVG(ac.kitchen_area), 2) AS avg_kitchen_area,
    ROUND(AVG(ac.floor), 1) AS avg_floor,
    ROUND(AVG(ac.floors_total), 1) AS avg_floors_total,
    ROUND(100.0 * COUNT(DISTINCT ac.id) / SUM(COUNT(DISTINCT ac.id)) OVER (PARTITION BY 
        CASE WHEN c.city = 'Санкт-Петербург' THEN 'Санкт-Петербург' ELSE 'Ленинградская область' END
    ), 2) AS percentage
FROM ad_categories ac
JOIN real_estate.flats f ON ac.id = f.id
JOIN real_estate.city c ON f.city_id = c.city_id
GROUP BY 
    CASE 
        WHEN c.city = 'Санкт-Петербург' THEN 'Санкт-Петербург'
        ELSE 'Ленинградская область'
    END,
    ac.time_category
ORDER BY region, 
    CASE ac.time_category
        WHEN '1-30 days' THEN 1
        WHEN '31-90 days' THEN 2
        WHEN '91-180 days' THEN 3
        WHEN '181+ days' THEN 4
        ELSE 5
    END;


-- ============================================================
-- ЗАДАЧА 2. Сезонность объявлений
-- ============================================================
-- Анализ: по месяцам активность публикации и снятия объявлений
-- Период: 2015-2018 годы, только города

WITH dates_calc AS (
    SELECT 
        a.id,
        a.first_day_exposition,
        -- Дата снятия = дата публикации + days_exposition
        a.first_day_exposition + INTERVAL '1 day' * a.days_exposition AS removal_date,
        f.total_area,
        a.last_price / f.total_area AS price_per_sqm,
        t.type
    FROM real_estate.advertisement a
    JOIN filtered_ids fi ON a.id = fi.id
    JOIN real_estate.flats f ON a.id = f.id
    JOIN real_estate.type t ON f.type_id = t.type_id
    WHERE 
        t.type = 'город'
        AND EXTRACT(YEAR FROM a.first_day_exposition) IN (2015, 2016, 2017, 2018)
        AND a.days_exposition IS NOT NULL
        AND a.days_exposition > 0
),
-- Статистика по месяцам публикации
publication_stats AS (
    SELECT 
        EXTRACT(MONTH FROM first_day_exposition) AS month_num,
        TO_CHAR(first_day_exposition, 'Month') AS month_name,
        COUNT(DISTINCT id) AS publications_count,
        ROUND(AVG(price_per_sqm), 2) AS avg_price_per_sqm_pub,
        ROUND(AVG(total_area), 2) AS avg_area_pub
    FROM dates_calc
    GROUP BY EXTRACT(MONTH FROM first_day_exposition), TO_CHAR(first_day_exposition, 'Month')
),
-- Статистика по месяцам снятия
removal_stats AS (
    SELECT 
        EXTRACT(MONTH FROM removal_date) AS month_num,
        TO_CHAR(removal_date, 'Month') AS month_name,
        COUNT(DISTINCT id) AS removals_count,
        ROUND(AVG(price_per_sqm), 2) AS avg_price_per_sqm_rem,
        ROUND(AVG(total_area), 2) AS avg_area_rem
    FROM dates_calc
    WHERE EXTRACT(YEAR FROM removal_date) IN (2015, 2016, 2017, 2018)
    GROUP BY EXTRACT(MONTH FROM removal_date), TO_CHAR(removal_date, 'Month')
)
-- Объединяем результаты
SELECT 
    COALESCE(p.month_num, r.month_num) AS month_num,
    COALESCE(TRIM(p.month_name), TRIM(r.month_name)) AS month_name,
    COALESCE(p.publications_count, 0) AS publications_count,
    COALESCE(r.removals_count, 0) AS removals_count,
    p.avg_price_per_sqm_pub,
    r.avg_price_per_sqm_rem,
    p.avg_area_pub,
    r.avg_area_rem
FROM publication_stats p
FULL OUTER JOIN removal_stats r ON p.month_num = r.month_num
ORDER BY month_num;


-- ============================================================
-- ДОПОЛНИТЕЛЬНЫЙ ЗАПРОС: Сравнение СПб и Ленобласти по сезонам
-- ============================================================

WITH seasonal_data AS (
    SELECT 
        a.id,
        CASE 
            WHEN c.city = 'Санкт-Петербург' THEN 'Санкт-Петербург'
            ELSE 'Ленинградская область'
        END AS region,
        EXTRACT(MONTH FROM a.first_day_exposition) AS pub_month,
        CASE 
            WHEN EXTRACT(MONTH FROM a.first_day_exposition) IN (12, 1, 2) THEN 'Зима'
            WHEN EXTRACT(MONTH FROM a.first_day_exposition) IN (3, 4, 5) THEN 'Весна'
            WHEN EXTRACT(MONTH FROM a.first_day_exposition) IN (6, 7, 8) THEN 'Лето'
            ELSE 'Осень'
        END AS season,
        a.days_exposition,
        a.last_price / f.total_area AS price_per_sqm,
        f.total_area
    FROM real_estate.advertisement a
    JOIN filtered_ids fi ON a.id = fi.id
    JOIN real_estate.flats f ON a.id = f.id
    JOIN real_estate.city c ON f.city_id = c.city_id
    JOIN real_estate.type t ON f.type_id = t.type_id
    WHERE 
        t.type = 'город'
        AND EXTRACT(YEAR FROM a.first_day_exposition) IN (2015, 2016, 2017, 2018)
        AND a.days_exposition IS NOT NULL
)
SELECT 
    region,
    season,
    COUNT(DISTINCT id) AS flats_count,
    ROUND(AVG(days_exposition), 1) AS avg_days_on_site,
    ROUND(AVG(price_per_sqm), 2) AS avg_price_per_sqm,
    ROUND(AVG(total_area), 2) AS avg_area
FROM seasonal_data
GROUP BY region, season
ORDER BY region, 
    CASE season
        WHEN 'Весна' THEN 1
        WHEN 'Лето' THEN 2
        WHEN 'Осень' THEN 3
        WHEN 'Зима' THEN 4
    END;
