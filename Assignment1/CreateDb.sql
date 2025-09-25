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

-- Address table links with customers
-- Customers table would have to be updated to include foreign key to address_id
CREATE TABLE addresses(
    address_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    street TEXT,
    city TEXT,
    province TEXT,
    postal_code TEXT
);

-- Payments table. Links with customers, and orders
-- Stores amount, payment type (credit card, PayPal, debit)
-- Orders table would have to have new column "payment_id" that is foreign key referencing payment row
CREATE TABLE payments(
    payment_id SERIAL PRIMARY KEY,
    order_id INT REFERENCES orders(order_id),
    amount INT,
    payment_type TEXT,
    status TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);