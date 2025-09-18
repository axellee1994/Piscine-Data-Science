ALTER TABLE customers
ADD COLUMN IF NOT EXISTS category_id BIGINT,
ADD COLUMN IF NOT EXISTS category_code VARCHAR(255),
ADD COLUMN IF NOT EXISTS brand VARCHAR(255);

UPDATE customers c
SET
    category_id = i.category_id,
    category_code = i.category_code,
    brand = i.brand
FROM (
    SELECT
        product_id,
        MAX(category_id) AS category_id,
        MAX(category_code) AS category_code,
        MAX(brand) AS brand
    FROM items
    GROUP BY product_id
) i
WHERE c.product_id = i.product_id;