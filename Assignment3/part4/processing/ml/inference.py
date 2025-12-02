from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegressionModel
from pyspark.sql.functions import col

# takes in day_of_week, hour_of_day, and outputs predicted number of items using trained model
def predict_items(day_of_week, hour_of_day, category):

    # Initialize Spark session
    spark = SparkSession.builder.appName("OrderPredictionInference").getOrCreate()

    # Load the trained model
    MODEL_PATH = 'models/order_prediction_lr_model'
    lr_model = LinearRegressionModel.load(MODEL_PATH)

    # Prepare input data
    input_data = spark.createDataFrame(
        [(day_of_week, hour_of_day, category)],
        ["day_of_week", "hour_of_day", "category"]
    )

    # Encode categorical variable 'category'
    indexer = StringIndexer(inputCol="category", outputCol="category_indexed")
    input_data = indexer.fit(input_data).transform(input_data)

    # Assemble features into a single vector
    assembler = VectorAssembler(
        inputCols=["day_of_week", "hour_of_day", "category_indexed"],
        outputCol="features"
    )
    input_data = assembler.transform(input_data)

    # Make prediction
    predictions = lr_model.transform(input_data)
    predicted_items = predictions.select(col("prediction")).collect()[0][0]
    print(f"Predicted items: {predicted_items}")

    # Stop Spark session
    spark.stop()

    return predicted_items

if __name__ == "__main__":
    # Example usage 
    day_of_week = 6 
    hour_of_day = 20  
    category = "snacks" 

    predicted_count = predict_items(day_of_week, hour_of_day, category)
    print(f"Predicted number of items for category '{category}' on day {day_of_week} at hour {hour_of_day}: {predicted_count}")