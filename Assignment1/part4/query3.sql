-- Find all orders made by customer “Alice Johnson”
SELECT orders.* FROM orders INNER JOIN customers ON orders.customer_id = customers.customer_id WHERE customers.name = "Alice Johnson";