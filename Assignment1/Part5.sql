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