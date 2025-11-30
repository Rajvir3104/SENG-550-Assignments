from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegressionModel
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql.functions import col, concat_ws
import redis
import os
import json

# Get configuration from environment
BASE_DIR = os.getenv('PROJECT_BASE_DIR', os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
REDIS_HOST = os.getenv('REDIS_CACHE_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_CACHE_PORT', '6379'))

MODEL_PATH = os.path.join(BASE_DIR, "models/order_prediction_lr_model")

print(f"BASE_DIR: {BASE_DIR}")
print(f"Redis: {REDIS_HOST}:{REDIS_PORT}")
print(f"Model path: {MODEL_PATH}")

# Initialize Spark session
spark = SparkSession.builder.appName("CachePredictions").getOrCreate()

# Connect to Redis
try:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    redis_client.ping()
    print("Successfully connected to Redis")
except Exception as e:
    print(f"Failed to connect to Redis: {e}")
    raise

# Load the trained model
print(f"Loading model from: {MODEL_PATH}")
lr_model = LinearRegressionModel.load(MODEL_PATH)

# Generate all combinations of day_of_week, hour_of_day, and category
# Days: 0-6 (Monday=0, Sunday=6)
# Hours: 0-23

# Load a sample of your data to get categories
DATA_PATH = os.path.join(BASE_DIR, "data/processed/orders.csv/part-00000-*.csv")
sample_df = spark.read.csv(DATA_PATH, header=True, inferSchema=True).select("category").distinct()
categories = [row.category for row in sample_df.collect()]
print(f"Categories found: {categories}")

# Create DataFrame with all combinations

combinations = []
for day in range(7):  # 0-6 for days of week
    for hour in range(24):  # 0-23 for hours
        for category in categories:
            combinations.append((day, hour, category))

print(f"Generated {len(combinations)} combinations to predict")

# Create Spark DataFrame from combinations
combinations_df = spark.createDataFrame(combinations, ["day_of_week", "hour_of_day", "category"])

# Apply the same transformations as in training
indexer = StringIndexer(inputCol="category", outputCol="category_indexed", handleInvalid="keep")
indexer_model = indexer.fit(combinations_df)
combinations_df = indexer_model.transform(combinations_df)

assembler = VectorAssembler(
    inputCols=["day_of_week", "hour_of_day", "category_indexed"],
    outputCol="features"
)
combinations_df = assembler.transform(combinations_df)

# Make predictions for all combinations
print("Making predictions...")
predictions_df = lr_model.transform(combinations_df)

# Select relevant columns
predictions_df = predictions_df.select("day_of_week", "hour_of_day", "category", "prediction")

# Create Redis key column
predictions_df = predictions_df.withColumn(
    "redis_key",
    concat_ws(":", col("day_of_week"), col("hour_of_day"), col("category"))
)

print("Storing predictions in Redis...")

# Collect predictions and store in Redis using pipeline for efficiency
predictions_list = predictions_df.select("redis_key", "prediction", "day_of_week", "hour_of_day", "category").collect()

# Use Redis pipeline for batch operations (much faster)
pipe = redis_client.pipeline()
stored_count = 0

for row in predictions_list:
    key = row.redis_key
    value = {
        "prediction": float(row.prediction),
        "day_of_week": int(row.day_of_week),
        "hour_of_day": int(row.hour_of_day),
        "category": row.category
    }
    pipe.set(key, json.dumps(value))
    stored_count += 1
    
    # Execute pipeline in batches of 1000
    if stored_count % 1000 == 0:
        pipe.execute()
        pipe = redis_client.pipeline()
        print(f"Stored {stored_count} predictions...")

# Execute remaining items
pipe.execute()

print(f"Successfully stored {stored_count} predictions in Redis")

# Store metadata about when cache was last updated
metadata = {
    "last_updated": str(spark.sparkContext.getConf().get("spark.app.startTime")),
    "total_predictions": stored_count,
    "categories": categories
}
redis_client.set("cache:metadata", json.dumps(metadata))

print("Cache metadata stored")

# Verify a sample
sample_key = f"0:12:{categories[0]}"
sample_value = redis_client.get(sample_key)
print(f"Sample verification - Key: {sample_key}, Value: {sample_value}")

# Stop Spark session
spark.stop()

print("Cache predictions completed successfully!")