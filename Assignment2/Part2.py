import psycopg2

# Constants
db_connect = psycopg2.connect(
    dbname="postgres", user="postgres", password="postgres", host="localhost", port=5432
)

def reset_tables(tables: list):
    cursor = db_connect.cursor()
    for table in tables:
        cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
    db_connect.commit()
    cursor.close()
    print("Tables dropped.")

def create_tables():
    cursor = db_connect.cursor()
    with open("Part1.sql", "r") as f:
        cursor.execute(f.read())
    db_connect.commit()
    cursor.close()
    print("Tables created.")



# add product 
def add_product(prod_id: int, prod_name: str, category: str, price, float):
    cursor = db_connect.cursor()
    query = f"INSERT INTO products (prod_id, prod_name, category,price) VALUES (%d, %s, %s, %f)"
    cursor.execute(query, (prod_id, prod_name, category, price))
    db_connect.commit()
    fetched_row = cursor.fetchone()
    if not fetched_row:
        return -1
    new_id = int(fetched_row[0])
    cursor.close()
    return new_id

def add_customer(customer_id: int, name: str, email: str, city: str):
    cursor = db_connect.cursor()
    query = f"INSERT  INTO customers (customer_id, name, email, city) VALUES (%d, %s, %s, %s)"
    cursor.execute(query, (customer_id, name, email. city))
    db_connect.commit()
    fetched_row = cursor.fetchone()
    if not fetched_row:
        return -1
    new_id = int(fetched_row[0])
    cursor.close()
    return new_id
