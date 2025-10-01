CREATE TABLE dim_customers (
    id SERIAL PRIMARY KEY,
    customer_id INT,
    name TEXT,
    email TEXT,
    city TEXT,
--- type 2 tracking columns
    effective_date TIMESTAMP NOT NULL,      -- When this record became effective
    expiry_date TIMESTAMP,                  -- When this record expired (NULL if current)
    is_current BOOLEAN DEFAULT TRUE    -- Flag for current record
)
