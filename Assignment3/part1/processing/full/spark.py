import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as f_sum, expr, coalesce, lit

# 🔑 Portable path setup: works wherever the script is run from
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "..", "..", "data")  # relative to processing/full/
INPUT_PATH = os.path.join(DATA_DIR, "raw", "*", "orders_*.csv")
OUTPUT_PATH = os.path.join(DATA_DIR, "processed", "orders.csv")

# Ensure output directory exists (Spark won't create parent dirs on write)
os.makedirs(os.path.join(DATA_DIR, "processed"), exist_ok=True)

# Initialize Spark
spark = (
    SparkSession.builder
    .appName("order_aggregation")
    .master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .getOrCreate()
)

try:
    # 1. Read all raw order files
    df = (
        spark.read
        .option("header", "true")
        .option("recursiveFileLookup", "true")  # Handles */* safely
        .csv(INPUT_PATH)
    )

    print(f" Loaded {df.count()} rows from {len(df.columns)} columns.")

    # 2. Cast & rename time columns
    df_base = (
        df
        .withColumn("day_of_week", col("order_dow").cast("int"))
        .withColumn("hour_of_day", col("order_hour_of_day").cast("int"))
    )

    # 3. Identify category columns (non-base, non-time)
    base_cols = {"order_id", "order_dow", "order_hour_of_day", "days_since_prior_order"}
    category_cols = [
        c for c in df_base.columns
        if c not in base_cols and c not in {"day_of_week", "hour_of_day"}
    ]
    print(f"🔍 Identified {len(category_cols)} category columns.")

    # 4. Cast categories to int, replace null with 0
    df_int = df_base.select(
        "day_of_week",
        "hour_of_day",
        *[
            coalesce(col(c).cast("int"), lit(0)).alias(c)
            for c in category_cols
        ]
    )

    # 5. Unpivot wide → long using stack (efficient native operation)
    if not category_cols:
        raise ValueError("No category columns found — check input schema.")
    
    pairs = ", ".join(f"'{c}', `{c}`" for c in category_cols)
    stack_expr = f"stack({len(category_cols)}, {pairs}) as (category, items)"
    df_long = df_int.select("day_of_week", "hour_of_day", expr(stack_expr))

    # 6. Filter out zero-item entries
    df_nonzero = df_long.where(col("items") > 0)

    # 7. Aggregate: sum(items) per (day, hour, category)
    agg = (
        df_nonzero
        .groupBy("day_of_week", "hour_of_day", "category")
        .agg(f_sum("items").alias("items_count"))
    )

    print("Output schema:")
    agg.printSchema()
    print("Sample (first 5 rows):")
    agg.show(5, truncate=False)

    # 8. Write result — allows multiple part files (as permitted by spec)
    (
        agg.write
        .mode("overwrite")
        .option("header", "true")
        .csv(OUTPUT_PATH)
    )
    print(f"Success! Results written to: {OUTPUT_PATH}")

finally:
    spark.stop()
    print("Spark session closed.")