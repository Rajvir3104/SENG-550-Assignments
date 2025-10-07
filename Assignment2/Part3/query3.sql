-- Total Price vs. Actual Amount Difference (4 marks)
-- Write a query to calculate the sum of differences between listed product price (at the
-- time of the order) and paid amount (price - amount)
SELECT
    o.id,
    p.name,
    SUM(p.price - o.amount) AS Difference
FROM
    fact_orders o
    LEFT JOIN dim_products p
      ON o.product_id = p.product_id
      AND o.order_date >= p.effective_date
      AND (p.expiry_date IS NULL OR o.order_date < p.expiry_date)
GROUP BY
    o.id,
    p.name
ORDER BY o.id;