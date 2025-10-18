# Assignment 2

Code is separated by the different parts of the assignments.

Aryan Karadia - 30140288

Rajvir Bhatti - 30150118

## Part 1
The code for part 1, which is creating the table is provided in the Part1.py file

```
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
```

## Part 2 

Those functions are written in Part2.py

Actions performed on the empty database is as follows:
```
print("Part 2 code running...")
    add_product(1, "Laptop", "Electronics", 1000)
    add_product(2, "Phone", "Electronics", 500)
    add_customer(1, name="Alice",city="New York")
    add_customer(2, name="Bob", city="Boston")
    add_order(customer_id=1, product_id=1, amount=1000)
    update_customer_city(1, "Chicago")
    update_product_price(1, 900)
    add_order(1, 1, 850)
    update_customer_city(2, "Calgary")
    add_order(2, 2, 500)
    add_order(1, 1, 900)
    update_customer_city(1, "San Francisco")
    add_order(1, 2, 450)
    add_order(2, 1, 900)
    print("Part 2 code finished.")
```

## Part 3

Query Outputs for the Postgresql queries are in the Part 3 folder

## etl.py
### Connects to Postgresql
```
postgres_connect = psycopg2.connect(
    dbname="postgres", user="postgres", password="postgres",           
    host="localhost", port=5432)
cursor = postgres_connect.cursor()
```
### Joins fact_orders, dim_customers and dim_products
```
join_query = """
    SELECT o.order_id, order_date, o.customer_id, 
    c.name AS customer_name, c.email, c.city AS customer_city, 
    p.product_id, p.name AS product_name, p.category, p.price AS product_price,
    amount
    FROM dim_customers c
    INNER JOIN fact_orders o ON c.customer_id = o.customer_id AND order_date BETWEEN c.effective_date AND c.expiry_date
    INNER JOIN dim_products p ON o.product_id = p.product_id AND order_date BETWEEN p.effective_date AND p.expiry_date"""
```
### Inserts data into orders_summary MongoDB
```
from pymongo import MongoClient

mongo_client = MongoClient("mongodb://localhost:27017/")

# sales_db

db = mongo_client["sales_db"]

# orders_summary collection

orders_summary_collection = db["orders_summary"]

# add results to orders_summary collection

for row in results:
    order = {
        "order_id": row[0],
        "order_date": row[1],
        "customer": {
            "customer_id": row[2],
            "name": row[3],
            "email": row[4],
            "city": row[5],
        },
        "product": {
            "product_id": row[6],
            "name": row[7],
            "category": row[8],
            "price": float(row[9]),
        },
        "amount": float(row[10]),
    }
    orders_summary_collection.insert_one(order)

# close mongo connection
mongo_client.close()
```

## MongoDB Queries

### Query 1 Output
```
Number of cities each customer has lived in:
{'name': 'Alice', 'city_count': 3}
{'name': 'Bob', 'city_count': 1}
```

### Query 2 Output
```
Total amount sold per city:
{'total_amount_sold': 3500.0, 'city': 'Chicago'}
{'total_amount_sold': 2000.0, 'city': 'New York'}
{'total_amount_sold': 2800.0, 'city': 'Calgary'}
{'total_amount_sold': 900.0, 'city': 'San Francisco'}
```

### Query 3 Output
```
Order ID, Product Name, Total Difference:
{'difference': 0.0, 'order_id': 1, 'product_name': 'Laptop'}
{'difference': 100.0, 'order_id': 2, 'product_name': 'Laptop'}
{'difference': 0.0, 'order_id': 3, 'product_name': 'Phone'}
{'difference': 0.0, 'order_id': 4, 'product_name': 'Laptop'}
{'difference': 100.0, 'order_id': 5, 'product_name': 'Phone'}
{'difference': 0.0, 'order_id': 6, 'product_name': 'Laptop'}
```

### Query 4 Output
```
All orders for customer with id 1:
{'_id': ObjectId('68f1633f2c7035ebdf0435b0'), 'order_id': 1, 'order_date': datetime.datetime(2025, 10, 7, 16, 1, 20, 886000), 'customer': {'customer_id': 1, 'name': 'Alice', 'email': None, 'city': 'New York'}, 'product': {'product_id': 1, 'name': 'Laptop', 'category': 'Electronics', 'price': 1000.0}, 'amount': 1000.0}


{'_id': ObjectId('68f1633f2c7035ebdf0435b1'), 'order_id': 2, 'order_date': datetime.datetime(2025, 10, 7, 16, 1, 20, 889000), 'customer': {'customer_id': 1, 'name': 'Alice', 'email': None, 'city': 'Chicago'}, 'product': {'product_id': 1, 'name': 'Laptop', 'category': 'Electronics', 'price': 900.0}, 'amount': 850.0}


{'_id': ObjectId('68f1633f2c7035ebdf0435b3'), 'order_id': 4, 'order_date': datetime.datetime(2025, 10, 7, 16, 1, 20, 891000), 'customer': {'customer_id': 1, 'name': 'Alice', 'email': None, 'city': 'Chicago'}, 'product': {'product_id': 1, 'name': 'Laptop', 'category': 'Electronics', 'price': 900.0}, 'amount': 900.0}


{'_id': ObjectId('68f1633f2c7035ebdf0435b4'), 'order_id': 5, 'order_date': datetime.datetime(2025, 10, 7, 16, 1, 20, 893000), 'customer': {'customer_id': 1, 'name': 'Alice', 'email': None, 'city': 'San Francisco'}, 'product': {'product_id': 2, 'name': 'Phone', 'category': 'Electronics', 'price': 500.0}, 'amount': 450.0}


{'_id': ObjectId('68f2d084e9e5e0d3f14386a0'), 'order_id': 1, 'order_date': datetime.datetime(2025, 10, 7, 16, 1, 20, 886000), 'customer': {'customer_id': 1, 'name': 'Alice', 'email': None, 'city': 'New York'}, 'product': {'product_id': 1, 'name': 'Laptop', 'category': 'Electronics', 'price': 1000.0}, 'amount': 1000.0}


{'_id': ObjectId('68f2d084e9e5e0d3f14386a1'), 'order_id': 2, 'order_date': datetime.datetime(2025, 10, 7, 16, 1, 20, 889000), 'customer': {'customer_id': 1, 'name': 'Alice', 'email': None, 'city': 'Chicago'}, 'product': {'product_id': 1, 'name': 'Laptop', 'category': 'Electronics', 'price': 900.0}, 'amount': 850.0}


{'_id': ObjectId('68f2d084e9e5e0d3f14386a3'), 'order_id': 4, 'order_date': datetime.datetime(2025, 10, 7, 16, 1, 20, 891000), 'customer': {'customer_id': 1, 'name': 'Alice', 'email': None, 'city': 'Chicago'}, 'product': {'product_id': 1, 'name': 'Laptop', 'category': 'Electronics', 'price': 900.0}, 'amount': 900.0}


{'_id': ObjectId('68f2d084e9e5e0d3f14386a4'), 'order_id': 5, 'order_date': datetime.datetime(2025, 10, 7, 16, 1, 20, 893000), 'customer': {'customer_id': 1, 'name': 'Alice', 'email': None, 'city': 'San Francisco'}, 'product': {'product_id': 2, 'name': 'Phone', 'category': 'Electronics', 'price': 500.0}, 'amount': 450.0}

```

## Part 8 
### Refer to the Part 8 ReadME