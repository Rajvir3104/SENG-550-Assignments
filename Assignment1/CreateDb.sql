CREATE TABLE customers (
customer_id SERIAL PRIMARY KEY
,NAME TEXT
,email TEXT
,phone TEXT
,address TEXT
);

CREATE TABLE orders (
order_id SERIAL PRIMARY KEY
,customer_id INT REFERENCES customers(customer_id)
,order_date DATE
,total_amount NUMERIC
,product_id INT
,product_category TEXT
,product_name TEXT
);

CREATE TABLE deliveries (
delivery_id SERIAL PRIMARY KEY
,order_id INT REFERENCES orders(order_id)
,delivery_date DATE
,STATUS TEXT
);