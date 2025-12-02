from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegressionModel
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql.functions import col, concat_ws
import redis
import os
import json

# ----------------------------------------------------------
# Configuration
# ----------------------------------------------------------
BASE_DIR = os.getenv(
    "PROJECT_BASE_DIR",
    os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
)

DATA_PATH = os.path.join(BASE_DIR, "data", "processed", "orders.csv")
MODEL_PATH = os.path.join(BASE_DIR, "models", "order_prediction_lr_model")
REDIS_HOST = os.getenv("REDIS_CACHE_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_CACHE_PORT", "6379"))

print("[CONFIG] BASE_DIR:", BASE_DIR)
print("[CONFIG] DATA_PATH:", DATA_PATH)
print("[CONFIG] MODEL_PATH:", MODEL_PATH)
print("[CONFIG] Redis:", REDIS_HOST, REDIS_PORT)

# ----------------------------------------------------------
# Spark session
# ----------------------------------------------------------
spark = SparkSession.builder.appName("CachePredictionsPart4").getOrCreate()

# ----------------------------------------------------------
# Redis
# ----------------------------------------------------------
client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
client.ping()

print("Connected to Redis ✔")

# ----------------------------------------------------------
# Load data and model
# ----------------------------------------------------------
df = spark.read.csv(DATA_PATH, header=True, inferSchema=True)
categories = [row.category for row in df.select("category").distinct().collect()]

print("Categories:", categories)

model = LinearRegressionModel.load(MODEL_PATH)
print("Model loaded ✔")

# ----------------------------------------------------------
# Generate all prediction combinations
# ----------------------------------------------------------
combos = []
for day in range(0, 7):
    for hour in range(0, 24):
        for cat in categories:
            combos.append((day, hour, cat))

combo_df = spark.createDataFrame(
    combos, ["day_of_week", "hour_of_day", "category"]
)

# Apply indexing + vectorization
indexer = StringIndexer(
    inputCol="category",
    outputCol="category_indexed",
    handleInvalid="keep"
).fit(combo_df)

combo_df = indexer.transform(combo_df)

assembler = VectorAssembler(
    inputCols=["day_of_week", "hour_of_day", "category_indexed"],
    outputCol="features"
)

combo_df = assembler.transform(combo_df)

# Predict
pred_df = model.transform(combo_df)

pred_df = pred_df.select(
    "day_of_week", "hour_of_day", "category", "prediction"
)

# Add Redis keys
pred_df = pred_df.withColumn(
    "redis_key",
    concat_ws(":", col("day_of_week"), col("hour_of_day"), col("category"))
)

pred_rows = pred_df.collect()

print(f"Storing {len(pred_rows)} predictions in Redis...")

pipe = client.pipeline()

for row in pred_rows:
    key = row.redis_key
    value = {
        "prediction": float(row.prediction),
        "day_of_week": int(row.day_of_week),
        "hour_of_day": int(row.hour_of_day),
        "category": row.category
    }
    pipe.set(key, json.dumps(value))

pipe.execute()

print("Redis caching complete ✔")
spark.stop()
