CREATE TABLE address(
    address_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    street TEXT,
    city TEXT,
    province TEXT,
    postal_code TEXT,
);