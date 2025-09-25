-- Find the total amount spent by each customer, sorted from highest to lowest.
SELECT c.name, SUM(total_amount) as Amount_Spent 
FROM orders o RIGHT JOIN customers c ON o.customer_id = c.customer_id 
GROUP BY c.name
 ORDER BY Amount_Spent DESC;  