SELECT orders.customer_id, customers.name, COUNT(order_id) AS order_count
FROM orders
INNER JOIN customers ON orders.customer_id = customers.customer_id
GROUP BY orders.customer_id, customers.name
HAVING COUNT(order_id) > 2;