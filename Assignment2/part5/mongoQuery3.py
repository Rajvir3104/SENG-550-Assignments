from pymongo import MongoClient

# Connect to MongoDB server
client = MongoClient('mongodb://localhost:27017/')
db = client['sales_db']
collection = db['orders_summary']

# Total difference between price and amount for every order
returned = collection.aggregate([
    {
        "$group": {
            "_id": {
                "order_id": "$order_id",
                "product_name": "$product.name"
            },
            "difference": {
                "$sum": {
                    "$subtract": ["$product.price", "$amount"]
                }
            }
        }
    },
    {
        "$project": {
            "order_id": "$_id.order_id",
            "product_name": "$_id.product_name",
            "difference": 1,
            "_id": 0
        }
    },
    {
        "$sort": {
            "order_id": 1
        }
    }
])

print("Order ID, Product Name, Total Difference:")
results = list(returned)
for doc in results:
    print(doc)
print("Total difference between price and amount:")
results = list(returned)
for doc in results:
    print(doc)
    print("\n")