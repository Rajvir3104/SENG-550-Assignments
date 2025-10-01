import psycopg2
from datetime import datetime

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
def add_product(product_id: int, name: str, category: str, price: float):
    cursor = db_connect.cursor()
    query = f"INSERT INTO dim_products (product_id, name, category, price) VALUES (%s, %s, %s, %s)"
    cursor.execute(query, (product_id, name, category, price))
    db_connect.commit()
    cursor.close()
    return


def add_customer(customer_id: int, name: str, city: str, email: str | None = None):
    cursor = db_connect.cursor()
    query = f"INSERT INTO dim_customers (customer_id, name, email, city) VALUES (%s, %s, %s, %s)"
    cursor.execute(query, (customer_id, name, email, city))
    db_connect.commit()
    cursor.close()
    return


# get customer info
def get_customer_info(customer_id: int) -> tuple:
    cursor = db_connect.cursor()
    query = f"SELECT name, email, city FROM dim_customers WHERE customer_id={customer_id} AND is_current=TRUE"
    cursor.execute(query)
    fetched_row = cursor.fetchone()
    if not fetched_row:
        return (-1, -1, -1)

    cursor.close()
    return fetched_row


def update_customer_city(customer_id: int, new_city: str) -> None:
    cursor = db_connect.cursor()

    # get name and email thats not changing
    customer = get_customer_info(customer_id)

    # update old row
    update_old_row = f"UPDATE dim_customers SET expiry_date = '{datetime.now().astimezone()}', is_current=FALSE WHERE customer_id={customer_id} AND is_current=TRUE"
    cursor.execute(update_old_row)

    # insert new row with new city
    update_city_query = f"INSERT INTO dim_customers (customer_id, name, email, city) VALUES (%s, %s, %s, %s)"
    cursor.execute(update_city_query, (customer_id, customer[0], customer[1], new_city))

    db_connect.commit()
    cursor.close()
    return


def get_product(product_id: int) -> tuple:
    cursor = db_connect.cursor()
    query = f"SELECT name, category, price FROM dim_products WHERE product_id={product_id} AND is_current=TRUE"
    cursor.execute(query)
    fetched_row = cursor.fetchone()
    if not fetched_row:
        return (-1, -1, -1)

    cursor.close()
    return fetched_row


def update_product_price(product_id: int, new_price: float) -> None:
    cursor = db_connect.cursor()
    # get info of product thats not changing
    product_info = get_product(product_id)

    # update old row
    update_old_product_query = f"UPDATE dim_products SET expiry_date='{datetime.now().astimezone()}', is_current=FALSE WHERE product_id={product_id} AND is_current=TRUE"
    cursor.execute(update_old_product_query)

    # insert new row with new price
    new_row_query = f"INSERT INTO dim_products (product_id, name, category, price) VALUES (%s, %s, %s, %s)"
    cursor.execute(
        new_row_query, (product_id, product_info[0], product_info[1], new_price)
    )

    db_connect.commit()
    cursor.close()
    return

def add_order(order_id: int, prod_id: int, customer_id: int, amount: float):
    pass

def part2code():
    add_product(1, "Laptop", "Electronics", 1000)
    add_product(2, "Phone", "Electronics", 500)
    add_customer(1, name="Alice",city="New York")
    add_customer(2, name="Bob", city="Boston")
    add_order(customer_id=1, prod_id=1, amount=1000)
    


if __name__ == "__main__":
    reset_tables(["dim_customers", "customers", "products", "fact_sales"])
    create_tables()
