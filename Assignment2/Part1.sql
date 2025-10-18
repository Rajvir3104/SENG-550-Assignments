CREATE TABLE dim_customers (
    id SERIAL PRIMARY KEY,
    customer_id INT,
    name TEXT,
    email TEXT,
    city TEXT,
--- type 2 tracking columns
    effective_date TIMESTAMP DEFAULT NOW(),     
    expiry_date TIMESTAMP,                 
    is_current BOOLEAN DEFAULT TRUE   
);

CREATE TABLE dim_products (
    id SERIAL PRIMARY KEY,
    product_id INT,
    name TEXT,
    category TEXT,
    price REAL,
-- type 2 tracking columns
    effective_date TIMESTAMP DEFAULT NOW(),
    expiry_date TIMESTAMP, 
    is_current BOOLEAN DEFAULT TRUE
);

CREATE TABLE fact_orders (
    order_id SERIAL PRIMARY KEY,
    product_id INT NOT NULL,
    customer_id INT NOT NULL,
    order_date TIMESTAMP DEFAULT NOW(),
    amount REAL NOT NULL
);