-- Подготовка данных для A/B-теста
-- Объединение участников теста с событиями пользователей

-- 1. Выделяем участников целевого теста interface_eu_test

SELECT 
    user_id,
    group,
    device
FROM ab_test_participants
WHERE ab_test = 'interface_eu_test';


-- 2. Получаем события пользователей с информацией о группе и устройстве

SELECT 
    e.user_id,
    e.event_dt,
    e.event_name,
    e.details,
    p.group,
    p.device
FROM ab_test_events e
JOIN ab_test_participants p ON e.user_id = p.user_id
WHERE p.ab_test = 'interface_eu_test';


-- 3. Находим пользователей с пересечением в разных тестах

SELECT 
    user_id,
    COUNT(DISTINCT ab_test) AS tests_count
FROM ab_test_participants
GROUP BY user_id
HAVING COUNT(DISTINCT ab_test) > 1;


-- 4. Распределение пользователей по группам и устройствам

SELECT 
    group,
    device,
    COUNT(DISTINCT user_id) AS users_count
FROM ab_test_participants
WHERE ab_test = 'interface_eu_test'
GROUP BY group, device
ORDER BY group, device;


-- 5. Количество событий по типам (для понимания воронки)

SELECT 
    event_name,
    COUNT(*) AS events_count,
    COUNT(DISTINCT user_id) AS unique_users
FROM ab_test_events
WHERE user_id != 'GLOBAL'
GROUP BY event_name
ORDER BY events_count DESC;


-- 6. Воронка событий для пользователей теста (первые 7 дней)

SELECT 
    p.group,
    COUNT(DISTINCT CASE WHEN e.event_name = 'registration' THEN e.user_id END) AS registrations,
    COUNT(DISTINCT CASE WHEN e.event_name = 'login' THEN e.user_id END) AS logins,
    COUNT(DISTINCT CASE WHEN e.event_name = 'product_page' THEN e.user_id END) AS product_page_views,
    COUNT(DISTINCT CASE WHEN e.event_name = 'product_cart' THEN e.user_id END) AS cart_adds,
    COUNT(DISTINCT CASE WHEN e.event_name = 'purchase' THEN e.user_id END) AS purchases
FROM ab_test_participants p
LEFT JOIN ab_test_events e ON p.user_id = e.user_id
WHERE p.ab_test = 'interface_eu_test'
GROUP BY p.group;
