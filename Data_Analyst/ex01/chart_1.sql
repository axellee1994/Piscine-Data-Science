SELECT
    TO_CHAR(event_time, 'Mon') AS month,
    ROUND(SUM(price) / 1000000.0, 2) AS total_sales_million
FROM customers
WHERE event_type = 'purchase'
  AND event_time >= '2022-10-01'
  AND event_time < '2023-02-01'
GROUP BY month
ORDER BY MIN(event_time);