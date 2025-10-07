--Total Sold Amount per City (4 marks)
-- Write a query to calculate the total amount grouped by customer’s cities (Their city at the
-- time of the order).
SELECT SUM(amount) as total_amount, city
FROM fact_orders
    INNER JOIN dim_customers
    ON fact_orders.customer_id = dim_customers.customer_id
    AND order_date BETWEEN dim_customers.effective_date AND dim_customers.expiry_date
GROUP BY city;  