-- МАРКЕТПЛЕЙС - SQL-запросы для сбора витрин
-- Аттестационный проект Яндекс Практикум

-- 1. Сбор данных о пользователях
-- Отбираем пользователей, зарегистрированных в 2024 году.
-- Парсим JSON-поля user_params в отдельные столбцы.
-- Добавляем неделю и месяц привлечения (cohort_week, cohort_month).

SELECT 
    user_id,
    registration_date,
    user_params->>'age' AS age,
    user_params->>'gender' AS gender,
    user_params->>'region' AS region,
    user_params->>'acq_channel' AS acq_channel,
    user_params->>'buyer_segment' AS buyer_segment,
    DATE_TRUNC('week', registration_date)::DATE AS cohort_week,
    DATE_TRUNC('month', registration_date)::DATE AS cohort_month
FROM 
    pa_graduate.users
WHERE 
    EXTRACT(YEAR FROM registration_date) = 2024
ORDER BY 
    registration_date ASC
LIMIT 100;


-- 2. Сбор данных о событиях 
-- События за 2024 год. Парсим JSON event_params (os, device).
-- Подтягиваем название товара через LEFT JOIN с product_dict.

SELECT 
    e.event_id,
    e.user_id,
    e.timestamp AS event_date,
    e.event_type,
    e.event_params->>'os' AS os,
    e.event_params->>'device' AS device,
    p.product_name,
    DATE_TRUNC('week', e.timestamp)::DATE AS event_week,
    DATE_TRUNC('month', e.timestamp)::DATE AS event_month
FROM 
    pa_graduate.events e
LEFT JOIN 
    pa_graduate.product_dict p ON e.product_id = p.product_id
WHERE 
    EXTRACT(YEAR FROM e.timestamp) = 2024
ORDER BY 
    e.timestamp ASC
LIMIT 100;


-- 3. Сбор данных о заказах 
-- Заказы за 2024 год. Подтягиваем название товара и категории.

SELECT 
    o.order_id,
    o.user_id,
    o.order_date,
    p.product_name,
    o.quantity,
    o.unit_price,
    o.total_price,
    p.category_name,
    DATE_TRUNC('week', o.order_date)::DATE AS order_week,
    DATE_TRUNC('month', o.order_date)::DATE AS order_month
FROM 
    pa_graduate.orders o
LEFT JOIN 
    pa_graduate.product_dict p ON o.product_id = p.product_id
WHERE 
    EXTRACT(YEAR FROM o.order_date) = 2024
ORDER BY 
    o.order_date ASC
LIMIT 100;
