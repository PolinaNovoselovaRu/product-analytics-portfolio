-- Olist - SQL-запросы для создания аналитических витрин
-- Исполнитель: Новоселова П.И.
-- Дата: 03.04.2026
-- База данных: PostgreSQL

-- Использованные конструкции SQL:
-- - CTE (WITH) для декомпозиции сложных запросов
-- - CASE WHEN для условной логики (расчет статуса доставки)
-- - LEFT JOIN для объединения таблиц с сохранением всех записей из левой
-- - Агрегатные функции (COUNT, SUM, AVG, ROUND)
-- - EXTRACT для извлечения года/месяца из даты
-- - NULLIF для защиты от деления на ноль
-- - Временные таблицы (TEMPORARY TABLE) для промежуточных данных



-- ВИТРИНА 1: Логистика

-- Цель: оценить сроки доставки, долю просрочек и их влияние на оценки
-- Агрегация: по году, месяцу, штату клиента
-- Ключевые метрики: доля просрочек, средняя задержка в днях, средняя оценка

CREATE TABLE olit.logistics_vitrina AS
WITH orders_with_reviews AS (
    -- Объединяем заказы с отзывами, приводим даты к timestamp
    -- Используем CASE WHEN для безопасного преобразования дат
    SELECT 
        o.order_id,
        o.customer_id,
        o.order_status,
        o.order_purchase_timestamp,
        CASE 
            WHEN o.order_purchase_timestamp IS NOT NULL AND o.order_purchase_timestamp != '' 
            THEN o.order_purchase_timestamp::TIMESTAMP 
            ELSE NULL 
        END AS order_purchase_ts,
        CASE 
            WHEN o.order_delivered_customer_date IS NOT NULL AND o.order_delivered_customer_date != '' 
            THEN o.order_delivered_customer_date::TIMESTAMP 
            ELSE NULL 
        END AS order_delivered_ts,
        CASE 
            WHEN o.order_estimated_delivery_date IS NOT NULL AND o.order_estimated_delivery_date != '' 
            THEN o.order_estimated_delivery_date::TIMESTAMP 
            ELSE NULL 
        END AS order_estimated_ts,
        r.review_score
    FROM olit.olist_orders_dataset o
    LEFT JOIN olit.olist_order_reviews_dataset r 
        ON o.order_id = r.order_id
    WHERE o.order_status = 'delivered'
        AND o.order_delivered_customer_date IS NOT NULL 
        AND o.order_delivered_customer_date != ''
        AND o.order_purchase_timestamp IS NOT NULL 
        AND o.order_purchase_timestamp != ''
        AND o.order_estimated_delivery_date IS NOT NULL 
        AND o.order_estimated_delivery_date != ''
),
delivery_metrics AS (
    -- Рассчитываем метрики доставки для каждого заказа
    -- Используем EXTRACT для разницы в днях, CASE WHEN для статуса
    SELECT 
        *,
        EXTRACT(DAY FROM order_delivered_ts - order_purchase_ts) AS actual_delivery_days,
        EXTRACT(DAY FROM order_estimated_ts - order_purchase_ts) AS estimated_delivery_days,
        CASE 
            WHEN order_delivered_ts <= order_estimated_ts THEN 'вовремя'
            ELSE 'опоздание'
        END AS delivery_status,
        CASE 
            WHEN order_delivered_ts > order_estimated_ts 
            THEN EXTRACT(DAY FROM order_delivered_ts - order_estimated_ts)
            ELSE 0
        END AS delay_days
    FROM orders_with_reviews
    WHERE order_purchase_ts IS NOT NULL 
        AND order_delivered_ts IS NOT NULL 
        AND order_estimated_ts IS NOT NULL
)
-- Финальная агрегация по времени и регионам
-- Используем EXTRACT для группировки, агрегатные функции, NULLIF для защиты от деления на 0
SELECT 
    EXTRACT(YEAR FROM order_purchase_ts) AS order_year,
    EXTRACT(MONTH FROM order_purchase_ts) AS order_month,
    c.customer_state,  
    COUNT(*) AS total_orders,
    SUM(CASE WHEN delivery_status = 'опоздание' THEN 1 ELSE 0 END) AS delayed_orders,
    ROUND(100.0 * SUM(CASE WHEN delivery_status = 'опоздание' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS delay_rate_percent,
    ROUND(AVG(delay_days), 2) AS avg_delay_days,
    ROUND(AVG(actual_delivery_days), 2) AS avg_actual_delivery_days,
    ROUND(AVG(estimated_delivery_days), 2) AS avg_estimated_delivery_days,  
    ROUND(AVG(review_score), 2) AS avg_review_score  
FROM delivery_metrics dm
LEFT JOIN olit.olist_customers_dataset c 
    ON dm.customer_id = c.customer_id
GROUP BY 
    EXTRACT(YEAR FROM order_purchase_ts),
    EXTRACT(MONTH FROM order_purchase_ts),
    c.customer_state
ORDER BY 
    order_year, order_month, c.customer_state;


-- ВИТРИНА 2: Продажи

-- Цель: анализ динамики выручки, региональной структуры и среднего чека
-- Агрегация: по году, месяцу, штату клиента
-- Ключевые метрики: выручка, количество заказов, средний чек

CREATE TABLE olit.sales_vitrina AS
WITH valid_orders AS (
    -- Только завершенные (доставленные) заказы
    SELECT 
        order_id,
        customer_id,
        order_purchase_timestamp::TIMESTAMP AS order_purchase_ts
    FROM olit.olist_orders_dataset
    WHERE order_status = 'delivered'
        AND order_purchase_timestamp IS NOT NULL
        AND order_purchase_timestamp != ''
),
order_items_with_revenue AS (
    -- Добавляем выручку по каждому товару в заказе
    -- Используем приведение типов к NUMERIC, расчет суммы с доставкой
    SELECT 
        o.order_id,
        o.customer_id,
        o.order_purchase_ts,
        oi.price::NUMERIC,
        oi.freight_value::NUMERIC,
        (oi.price::NUMERIC + oi.freight_value::NUMERIC) AS total_order_value,
        oi.seller_id
    FROM valid_orders o
    JOIN olit.olist_order_items_dataset oi 
        ON o.order_id = oi.order_id
),
order_aggregates AS (
    -- Агрегируем по заказу (сумма выручки, количество продавцов)
    SELECT 
        order_id,
        customer_id,
        order_purchase_ts,
        SUM(total_order_value) AS order_revenue,
        COUNT(DISTINCT seller_id) AS sellers_in_order
    FROM order_items_with_revenue
    GROUP BY order_id, customer_id, order_purchase_ts
)
-- Финальная агрегация по времени и регионам
SELECT 
    EXTRACT(YEAR FROM order_purchase_ts) AS order_year,
    EXTRACT(MONTH FROM order_purchase_ts) AS order_month,   
    c.customer_state,  
    COUNT(*) AS total_orders,
    SUM(order_revenue) AS total_revenue,
    ROUND(AVG(order_revenue), 2) AS avg_order_value,
    COUNT(DISTINCT oa.customer_id) AS unique_customers,
    ROUND(SUM(order_revenue) / NULLIF(COUNT(DISTINCT oa.customer_id), 0), 2) AS revenue_per_customer  
FROM order_aggregates oa
LEFT JOIN olit.olist_customers_dataset c 
    ON oa.customer_id = c.customer_id
WHERE c.customer_state IS NOT NULL
GROUP BY 
    EXTRACT(YEAR FROM order_purchase_ts),
    EXTRACT(MONTH FROM order_purchase_ts),
    c.customer_state
ORDER BY 
    order_year, order_month, c.customer_state;


-- ВИТРИНА 3: Качество продавцов

-- Цель: оценка работы продавцов, выявление проблемных
-- Агрегация: по продавцу и его штату
-- Ключевые метрики: средняя оценка, доля негативных отзывов

-- Создаем временную таблицу с очищенными отзывами
-- Использовано: TEMP TABLE, регулярное выражение для проверки числовых значений
CREATE TEMP TABLE clean_reviews AS
SELECT 
    order_id,
    review_score,
    review_creation_date,
    review_answer_timestamp
FROM olit.olist_order_reviews_dataset
WHERE review_score IS NOT NULL
    AND review_score::TEXT != ''
    AND review_score::TEXT ~ '^[0-9]+$'
    AND review_score::INT BETWEEN 1 AND 5;

-- Создаем витрину качества
CREATE TABLE olit.quality_vitrina AS
SELECT 
    oi.seller_id,
    s.seller_state,
    COUNT(*) AS total_orders,
    ROUND(AVG(cr.review_score::NUMERIC), 2) AS avg_review_score,
    SUM(CASE WHEN cr.review_score::INT <= 2 THEN 1 ELSE 0 END) AS negative_reviews,
    SUM(CASE WHEN cr.review_score::INT >= 4 THEN 1 ELSE 0 END) AS positive_reviews,
    ROUND(100.0 * SUM(CASE WHEN cr.review_score::INT <= 2 THEN 1 ELSE 0 END) / COUNT(*), 2) AS negative_rate_percent,
    ROUND(100.0 * SUM(CASE WHEN cr.review_score::INT >= 4 THEN 1 ELSE 0 END) / COUNT(*), 2) AS positive_rate_percent
FROM olit.olist_order_items_dataset oi
JOIN olit.olist_orders_dataset ord 
    ON oi.order_id = ord.order_id
JOIN clean_reviews cr 
    ON oi.order_id = cr.order_id
LEFT JOIN olit.olist_sellers_dataset s 
    ON oi.seller_id = s.seller_id
WHERE ord.order_status = 'delivered'
GROUP BY oi.seller_id, s.seller_state
ORDER BY avg_review_score ASC;
