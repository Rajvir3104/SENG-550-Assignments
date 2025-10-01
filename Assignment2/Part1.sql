CREATE TABLE dim_customers (
    id SERIAL PRIMARY KEY,
    customer_id INT,
    name TEXT,
    email TEXT,
    city TEXT,
--- type 2 tracking columns
    effective_date TIMESTAMP DEFAULT NOW(),      -- When this record became effective
    expiry_date TIMESTAMP,                  -- When this record expired (NULL if current)
    is_current BOOLEAN DEFAULT TRUE    -- Flag for current record
)

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
)

CREATE TABLE dim_orders (
    id SERIAL PRIMARY KEY,
    product_id INT REFERENCES dim_orders(order_id),
    customer_id INT REFERENCES dim_customers(customer_id),
    order_date DATE,
    amount REAL,
    -- type 2 tracking columns
    effective_date TIMESTAMP DEFAULT NOW(),
    expiry_date TIMESTAMP, 
    is_current BOOLEAN DEFAULT TRUE
)