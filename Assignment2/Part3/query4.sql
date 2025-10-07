SELECT order_id, city, order_date, dim_products.name, dim_products.price, amount FROM dim_customers
INNER JOIN fact_orders ON dim_customers.customer_id = fact_orders.customer_id AND order_date BETWEEN effective_date AND expiry_date
INNER JOIN dim_products ON fact_orders.product_id = dim_products.product_id AND order_date BETWEEN dim_products.effective_date AND dim_products.expiry_date
WHERE dim_customers.customer_id = 1
ORDER BY order_id;