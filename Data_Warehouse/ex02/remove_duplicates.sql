BEGIN;

DELETE FROM customers
WHERE ctid IN (
    SELECT ctid FROM (
        SELECT ctid,
               ROW_NUMBER() OVER (
                   PARTITION BY event_type, product_id,
                   FLOOR(EXTRACT(EPOCH FROM event_time))
                   ORDER BY event_time
               ) AS rn
        FROM customers
    ) t
    WHERE t.rn > 1
);

COMMIT;