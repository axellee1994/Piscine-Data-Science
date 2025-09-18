SELECT
    DATE(event_time) AS day,
    COUNT(*) AS num_customers
FROM customers
WHERE event_type = 'purchase'
  AND event_time >= '2022-10-01'
  AND event_time < '2023-03-01'
GROUP BY day
ORDER BY day;