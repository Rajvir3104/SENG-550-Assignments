# train a predictive model that estimates the number of orders per category per day and hour using data foudn in data/processed/orders.csv
# spark ML linear 
# Input features: day_of_week, hour_of_day, category (encoded as needed).
# Target: total_count
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression

# Initialize Spark session
spark = SparkSession.builder.appName("OrderPrediction").getOrCreate()
# Load processed data
data_path = "data/processed/orders.csv/part-00000-*.csv"

df = spark.read.csv(data_path, header=True, inferSchema=True)
# Select relevant columns
df = df.select("day_of_week", "hour_of_day", "category", "total_count")

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
final_data = df.select(col("features"), col("total_count").alias("label"))
# Split data into training and test sets
train_data, test_data = final_data.randomSplit([0.8, 0.2], seed=42)
# Initialize and train Linear Regression model
lr = LinearRegression(featuresCol="features", labelCol="label")
lr_model = lr.fit(train_data)
# Evaluate model on test data
test_results = lr_model.evaluate(test_data)
print(f"RMSE on test data: {test_results.rootMeanSquaredError}")
print(f"R2 on test data: {test_results.r2}")
# Save the trained model
model_path = "models/order_prediction_lr_model"
lr_model.save(model_path)
# Stop Spark session
spark.stop()
