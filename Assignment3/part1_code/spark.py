from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as f_sum, expr, coalesce, lit
spark = (
    SparkSession.builder
    .appName("order")
    .master("local[*]")  # run Spark in local mode
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .getOrCreate()
)



# 1. Read all 7 raw files  (keep whatever path is working for you)
df = (
    spark.read
    .option("header", "true")
    .csv("../part1/data/raw/*/orders_*.csv")
)

# 2. Cast & rename day/hour columns
df_base = (
    df
    .withColumn("day_of_week", col("order_dow").cast("int"))
    .withColumn("hour_of_day", col("order_hour_of_day").cast("int"))
)

# 3. Work out which columns are categories
base_cols = {"order_id", "order_dow", "order_hour_of_day", "days_since_prior_order"}
category_cols = [
    c for c in df_base.columns
    if c not in base_cols and c not in {"day_of_week", "hour_of_day"}
]

# 4. Make sure category columns are integers (treat null as 0)
df_int = df_base.select(
    "day_of_week",
    "hour_of_day",
    *[
        coalesce(col(c).cast("int"), lit(0)).alias(c)
        for c in category_cols
    ]
)

# 5. Unpivot (wide → long) using stack:
#    stack(N, 'col1', col1, 'col2', col2, ...) AS (category, items)
pairs = ", ".join(f"'{c}', `{c}`" for c in category_cols)
stack_expr = f"stack({len(category_cols)}, {pairs}) as (category, items)"

df_long = df_int.select(
    "day_of_week",
    "hour_of_day",
    expr(stack_expr)
)

# 6. Keep only rows where items > 0
df_nonzero = df_long.where(col("items") > 0)

# 7. Aggregate: total items per (day_of_week, hour_of_day, category)
agg = (
    df_nonzero
    .groupBy("day_of_week", "hour_of_day", "category")
    .agg(f_sum("items").alias("items_count"))
)
agg.printSchema()
agg.show(10, truncate=False)


# 8. Write result to processed path
(
    agg.write
    .mode("overwrite")
    .option("header", "true")
    .csv("../part1/data/processed/orders.csv")
)

spark.stop()
