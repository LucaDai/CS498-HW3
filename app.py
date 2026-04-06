from flask import Flask, request, jsonify
from pymongo import MongoClient, WriteConcern, ReadPreference
import os
app = Flask(__name__)
MONGO_URI = "mongodb+srv://lab6user:CS498@cluster0.pv9c50r.mongodb.net/?appName=Cluster0"
client = MongoClient(MONGO_URI)

db = client["ev_db"]
vehicles = db["vehicles"]

# 1. Fast but Unsafe Write
@app.route("/insert-fast", methods=["POST"])
def insert_fast():
    data = request.get_json()
    collection = vehicles.with_options(write_concern=WriteConcern(w = 1))
    result = collection.insert_one(data)
    return jsonify({"inserted_id": str(result.inserted_id)})

# 2. Highly Durable Write
@app.route("/insert-safe", methods=["POST"])
def insert_safe():
    data = request.get_json()
    collection = vehicles.with_options(write_concern=WriteConcern(w = "majority"))
    result = collection.insert_one(data)
    return jsonify({"inserted_id": str(result.inserted_id)})

# 3. Strongly Consistent Read
@app.route("/count-tesla-primary", methods=["GET"])
def count_tesla_primary():
    collection = vehicles.with_options(read_preference=ReadPreference.PRIMARY)
    count = collection.count_documents({"Make" : "TESLA"})
    return jsonify({"count": count})

# 4. Eventually Consistent Analytical Read
@app.route("/count-bmw-secondary", methods=["GET"])
def count_bmw_secondary():
    collection = vehicles.with_options(read_preference=ReadPreference.SECONDARY_PREFERRED)
    count = collection.count_documents({"Make" : "BMW"})
    return jsonify({"count": count})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
