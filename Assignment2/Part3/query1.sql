-- Count the number of different cities each customer has lived in. (2 marks)
SELECT name, COUNT(city) as city_count from dim_customers GROUP BY name;