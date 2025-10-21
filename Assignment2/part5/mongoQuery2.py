from pymongo import MongoClient

# Connect to MongoDB server
client = MongoClient('mongodb://localhost:27017/')
db = client['sales_db']
collection = db['orders_summary']

# Total amount sold per city
returned = collection.aggregate(
    [
        {
            "$group": {
                "_id": "$customer.city",  # group by city
                "total_amount_sold": {"$sum": "$amount"},  # sum the "amount" field
            }
        },
        # project only desired fields, city as id and total amount sold. also remove _id field
        {"$project": {"city": "$_id", "total_amount_sold": 1, "_id": 0}},
    ]
)

print("Total amount sold per city:")
results = list(returned)
for doc in results:
    print(doc)
