from pymongo import MongoClient

# Connect to MongoDB server
client = MongoClient('mongodb://localhost:27017/')
db = client['sales_db']
collection = db['orders_summary']

# get all orders for a specific customer by customer.id = 1
returned = collection.find({"customer.customer_id": 1})
print("All orders for customer with id 1:")
results = list(returned)
for doc in results:
    print(doc)
    print("\n")