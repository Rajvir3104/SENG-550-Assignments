import psycopg2

# conect to postgres
postgres_connect = psycopg2.connect(
    dbname="postgres", user="postgres", password="postgres", host="localhost", port=5432)
cursor = postgres_connect.cursor()


# join fact_orders, dim_customers, dim_products
join_query = """
    SELECT o.order_id, order_date, o.customer_id, 
    c.name AS customer_name, c.email, c.city AS customer_city, 
    p.product_id, p.name AS product_name, p.category, p.price AS product_price,
    amount
    FROM dim_customers c
    INNER JOIN fact_orders o ON c.customer_id = o.customer_id AND order_date BETWEEN c.effective_date AND c.expiry_date
    INNER JOIN dim_products p ON o.product_id = p.product_id AND order_date BETWEEN p.effective_date AND p.expiry_date
"""

# get results
cursor.execute(join_query)
results = cursor.fetchall()

cursor.close()
postgres_connect.close()

# connect to mongodb
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