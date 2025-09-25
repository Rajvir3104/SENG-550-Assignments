SELECT 
    product_category,
    COUNT(*) AS order_count
FROM 
    orders
GROUP BY 
    product_category
ORDER BY 
    order_count DESC;