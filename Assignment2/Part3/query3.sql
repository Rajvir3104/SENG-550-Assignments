-- Total Price vs. Actual Amount Difference (4 marks)
-- Write a query to calculate the sum of differences between listed product price (at the
-- time of the order) and paid amount (price - amount)
SELECT price, amount, o.
FROM dim_products p
RIGHT JOIN fact_orders o
ON o.product_id = p.product_id AND o.order_date BETWEEN p.effective_date AND p.expiry_date;