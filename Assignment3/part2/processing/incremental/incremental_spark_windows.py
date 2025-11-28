import os
import redis

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as f_sum, expr, coalesce, lit

# ---------- CONFIG ----------
RAW_BASE = "../../data/incremental/raw"
PROCESSED_PATH = "../../data/incremental/processed/orders.csv"
REDIS_KEY = "orders_last_processed_day"
REDIS_HOST = "host.docker.internal"
REDIS_PORT = 6379
# ----------------------------

def get_spark():
    spark = (
        SparkSession.builder
        .appName("incremental_orders_agg")
        .master("local[*]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )
    return spark

def get_last_processed_day(r):
    value = r.get(REDIS_KEY)
    if value is None:
        # Redis was cleared or never set → treat as "process everything"
        return -1
    return int(value)

def get_available_days():
    """
    Look at data/incremental/raw and return a sorted list of int day folder names.
    e.g. ['0','1','2','3'] -> [0,1,2,3]
    """
    if not os.path.isdir(RAW_BASE):
        return []

    days = []
    for name in os.listdir(RAW_BASE):
        full = os.path.join(RAW_BASE, name)
        if os.path.isdir(full) and name.isdigit():
            days.append(int(name))
    return sorted(days)

def main():
    # Connect to Redis
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    available_days = get_available_days()
    if not available_days:
        print("No raw folders found, exiting.")
        return

    last_processed = get_last_processed_day(r)
    print(f"Last processed day from Redis: {last_processed}")

    # New days are those strictly greater than last_processed
    new_days = [d for d in available_days if d > last_processed]

    if not new_days:
        print("No new days to process, exiting.")
        return

    print(f"New days to process: {new_days}")

    # Build list of file paths for Spark
    paths = [os.path.join(RAW_BASE, str(d), "orders_{}.csv".format(d)) for d in new_days]

    spark = get_spark()

    # 1. Read only new days
    df = (
        spark.read
        .option("header", "true")
        .csv(paths)
    )

    # 2. Basic columns
    df_base = (
        df
        .withColumn("day_of_week", col("order_dow").cast("int"))
        .withColumn("hour_of_day", col("order_hour_of_day").cast("int"))
    )

    # 3. Figure out category columns
    base_cols = {"order_id", "order_dow", "order_hour_of_day", "days_since_prior_order"}
    category_cols = [
        c for c in df_base.columns
        if c not in base_cols and c not in {"day_of_week", "hour_of_day"}
    ]

    # 4. Cast category columns to int (null -> 0)
    df_int = df_base.select(
        "day_of_week",
        "hour_of_day",
        *[
            coalesce(col(c).cast("int"), lit(0)).alias(c)
            for c in category_cols
        ]
    )

    # 5. Unpivot wide -> long
    pairs = ", ".join(f"'{c}', `{c}`" for c in category_cols)
    stack_expr = f"stack({len(category_cols)}, {pairs}) as (category, items)"

    df_long = df_int.select(
        "day_of_week",
        "hour_of_day",
        expr(stack_expr)
    )

    # 6. Filter non-zero items
    df_nonzero = df_long.where(col("items") > 0)

    # 7. Aggregate for these new days only
    agg = (
        df_nonzero
        .groupBy("day_of_week", "hour_of_day", "category")
        .agg(f_sum("items").alias("items_count"))
    )

    # 8. Append to processed output
    (
        agg.write
        .mode("append")
        .option("header", "true")
        .csv(PROCESSED_PATH)
    )

    # 9. Update Redis: we've now processed up to max(new_days)
    max_day = max(new_days)
    r.set(REDIS_KEY, str(max_day))
    print(f"Updated Redis: {REDIS_KEY} = {max_day}")

    spark.stop()

if __name__ == "__main__":
    main()
