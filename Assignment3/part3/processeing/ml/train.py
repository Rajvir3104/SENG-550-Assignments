from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
import os

# Get base directory from environment variable
# Falls back to calculating from current file location if not set
BASE_DIR = os.getenv('PROJECT_BASE_DIR', os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

print(f"BASE_DIR: {BASE_DIR}")

# Build paths dynamically
DATA_PATH = os.path.join(BASE_DIR, "data/processed/orders.csv/part-00000-f91ad07e-5cb7-499e-8063-a2be9a278550-c000.csv")
MODEL_PATH = os.path.join(BASE_DIR, "models/order_prediction_lr_model")

print(f"Data path: {DATA_PATH}")
print(f"Model path: {MODEL_PATH}")

# Initialize Spark session
spark = SparkSession.builder.appName("OrderPrediction").getOrCreate()

# Load processed data
print(f"Loading data from: {DATA_PATH}")
df = spark.read.csv(DATA_PATH, header=True, inferSchema=True)

print(f"Loaded {df.count()} rows")

# Select relevant columns
df = df.select("day_of_week", "hour_of_day", "category", "items_count")

# Encode categorical variable 'category'
indexer = StringIndexer(inputCol="category", outputCol="category_indexed")
df = indexer.fit(df).transform(df)

# Assemble features into a single vector
assembler = VectorAssembler(
    inputCols=["day_of_week", "hour_of_day", "category_indexed"],
    outputCol="features"
)
df = assembler.transform(df)

# Prepare final dataset for training
final_data = df.select(col("features"), col("items_count").alias("label"))

# Split data into training and test sets
train_data, test_data = final_data.randomSplit([0.8, 0.2], seed=42)

# Initialize and train Linear Regression model
lr = LinearRegression(featuresCol="features", labelCol="label")
lr_model = lr.fit(train_data)

# Evaluate model on test data
test_results = lr_model.evaluate(test_data)
print(f"RMSE on test data: {test_results.rootMeanSquaredError}")
print(f"R2 on test data: {test_results.r2}")

# Stop Spark session
spark.stop()