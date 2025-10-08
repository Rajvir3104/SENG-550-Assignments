from pymongo import MongoClient

# Connect to MongoDB server
client = MongoClient('mongodb://localhost:27017/')
db = client['sales_db']
collection = db['orders_summary']

# Number of cities each customer has lived in
returned = collection.aggregate([
    {
        "$group": {
            "_id": "$customer.name",
            "city_set": {"$addToSet": "$customer.city"}
        }
    },
    {
        "$project": {
            "name": "$_id",
            "city_count": {"$size": "$city_set"},
            "_id": 0
        }
    }
])

print("Number of cities each customer has lived in:")
results = list(returned)
for doc in results:
    print(doc)