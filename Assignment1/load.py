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


if __name__ == "__main__":
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
