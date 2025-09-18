SELECT
    event_type,
    COUNT(*) AS event_count
FROM
    customers
GROUP BY
    event_type
ORDER_BY
    event_count DESC;