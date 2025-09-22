import psycopg2

# Constants
db_connect = psycopg2.connect(
    dbname="postgres", user="postgres", password="postgres", host="localhost", port=5432
)

DATA_PATH = "./data/"


# for ever csv in data folder
def load_csv_to_db(file_path:str, table_name:str, columns:list) -> None:
    cursor = db_connect.cursor()
    # open the file
    with open(file_path, "r") as f:
        next(f)  # Skip the header row
        # use copy_from to copy the data to the table
        cursor.copy_from(f, table_name, sep=",", columns=columns)
    # push changes to the database
    db_connect.commit()

    # cleanup
    cursor.close()
    print(f"Data loaded into {table_name} from {file_path}")


# drop tables and run createDB script to reset foreign key id
def reset_tables(tables: list):
    cursor = db_connect.cursor()
    # drop tables if they exist
    for table in tables:
        cursor.execute(f"DROP TABLE IF EXISTS {table};")
    db_connect.commit()
    cursor.close()
    print("Tables dropped.")


def create_tables():
    # use sql file to create tables
    cursor = db_connect.cursor()
    with open("CreateDB.sql", "r") as f:
        cursor.execute(f.read())
    db_connect.commit()
    cursor.close()
    print("Tables created.")


# Add a new customer supplying name, email, phone, and address RETURNS new Id
def add_customer(name: str, email: str, phone: str, address: str) -> int:
    

    cursor = db_connect.cursor()
    query = f"INSERT INTO customers (name, email, phone, address) VALUES (%s, %s, %s, %s) RETURNING customer_id;"
    cursor.execute(query, (name,email,phone,address))
    fetched_row = cursor.fetchone()
    # if fetched row has no data, return -1
    if not fetched_row:
        return -1
    
    # else get the first element of the fetched row
    new_id = int(fetched_row[0])
    cursor.close()
    return new_id


# Add a new order with attributes including customer ID, order date, total amount, product_ID, product category, and product name. RETURNS order id
def add_order(
    customer_id: int,
    date: str,
    total: float,
    prod_id: int,
    prod_category: str,
    prod_name: str,
):

    cursor = db_connect.cursor()
    query = f"INSERT INTO orders (customer_id, order_date, total_amount, product_id, product_category, product_name) VALUES (%s, %s, %s, %s, %s, %s) RETURNING order_id;"
    cursor.execute(query,(customer_id, date, total, prod_id, prod_category, prod_name))
    fetched_row = cursor.fetchone() 
    if not fetched_row:
        return -1

    new_id = int(fetched_row[0])
    db_connect.commit()
    cursor.close()
    return new_id


# Add a new delivery with attributes such as order ID, delivery date, and status.
def add_delivery(order_id: int, date: str, status: str) -> int:
    cursor = db_connect.cursor()
    query = f"INSERT INTO deliveries (order_id, delivery_date, status) VALUES (%s, %s, %s) RETURNING delivery_id;"
    print(query)
    cursor.execute(query, (order_id, date, status))
    fetched_row = cursor.fetchone()
    # if fetched row has no data, return -1
    if not fetched_row:
        return -1

    # else get the first element of the fetched row
    new_id = int(fetched_row[0])
    cursor.close()
    return new_id

# Update the delivery status for an existing delivery, given its delivery ID.
def update_delivery(id: int, status: str) -> None:
    # cursor
    cursor = db_connect.cursor()
    # create query
    query = f"UPDATE deliveries SET status = '{status}' WHERE delivery_id={id}"
    print(query)
    # exec
    cursor.execute(query)
    cursor.close()
    return


def part2Code():
    # clear data first
    reset_tables(["deliveries", "orders", "customers"])

    # create tables
    create_tables()

    # load data
    load_csv_to_db(
        DATA_PATH + "customers.csv", "customers", ["name", "email", "phone", "address"]
    )
    load_csv_to_db(
        DATA_PATH + "orders.csv",
        "orders",
        [
            "customer_id",
            "order_date",
            "total_amount",
            "product_id",
            "product_category",
            "product_name",
        ],
    )
    load_csv_to_db(
        DATA_PATH + "deliveries.csv",
        "deliveries",
        ["order_id", "delivery_date", "status"],
    )


def part3Code():
    print("Part 3 code running...")
    print("Adding new customer, order, delivery, and updating delivery status...")
    # add new customer
    liam_id = add_customer(
        "Liam Nelson", "liam.nelson@example.com", "555-2468", "111 Elm Street"
    )
    print(f"New customer ID: {liam_id}")
    # add order
    order_id = add_order(
        liam_id, "2025-06-01", 180.00, 116, "Electronics", "Bluetooth Speaker"
    )
    print(f"New order ID: {order_id}")
    # add delivery
    delivery_id = add_delivery(order_id, "2025-06-03", "Pending")
    print(f"New delivery ID: {delivery_id}")
    print("Updating delivery status to 'Shipped'...")
    # update delivery to shipped
    update_delivery(delivery_id, "Shipped")
    print("Delivery status updated.")

    # TODO: Add one more customer, order, and delivery with any valid data...

    # update delivery #3
    update_delivery(3, "Delivered")

def part3_1Code():
    print("Part 3.1 code running...")
    # Add another customer
    emma_id = add_customer(
        "Emma Watson", "emma.watson@example.com", "555-7890", "222 Maple Avenue"
    )
    print(f"New customer ID: {emma_id}")

    # Add another order
    order_id = add_order(
        emma_id, "2025-07-15", 250.00, 117, "Home Appliances", "Vacuum Cleaner"
    )
    print(f"New order ID: {order_id}")

    # Add another delivery
    delivery_id = add_delivery(order_id, "2025-07-17", "Pending")
    print(f"New delivery ID: {delivery_id}")


if __name__ == "__main__":
    part3_1Code()
    db_connect.close()
    print("Database connection closed.")
