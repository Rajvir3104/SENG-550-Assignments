import psycopg2

db_connect = psycopg2.connect(
    dbname="postgres", user="postgres", password="postgres", host="localhost", port=5432
)

DATA_PATH = "./data/"


# for ever csv in data folder
def load_csv_to_db(file_path, table_name, columns):
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
def reset_tables(tables):
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
    with open("createDB.sql", "r") as f:
        cursor.execute(f.read())
    db_connect.commit()
    cursor.close()
    print("Tables created.")


if __name__ == "__main__":
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

    db_connect.close()
    print("Database connection closed.")


##Add a new customer supplying name, email, phone, and address
def add_customer(name, email, phone, address):
    # TODO
    return


# Add a new order with attributes including customer ID, order date, total amount, product_ID, product category, and product name.
def add_order(customer_id, date, total, prod_id, prod_category, prod_name):
    # TODO
    return


# Add a new delivery with attributes such as order ID, delivery date, and status.
def add_delivery(id, date, status):
    # TODO
    return


# Update the delivery status for an existing delivery, given its delivery ID.
def update_delivery(id):
    # TODO
    return
