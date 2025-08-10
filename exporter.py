import csv
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

# === Connect to MongoDB ===
mongo = MongoClient(os.getenv("MONGO_URI"))
db = mongo.ojtconnect  # Replace with your DB name
collection = db.company_profiles  # Replace with your collection name

# === Define Output CSV File ===
output_file = "company_profiles.csv"

# === Fetch Documents ===
documents = list(collection.find())

# === Export to CSV ===
if documents:
    # Extract fieldnames from the first document
    fieldnames = sorted(set().union(*(doc.keys() for doc in documents)))

    with open(output_file, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for doc in documents:
            # Convert ObjectId to string and handle nested fields if needed
            doc = {k: str(v) if isinstance(v, (dict, list)) else v for k, v in doc.items()}
            writer.writerow(doc)

    print(f"✅ Exported {len(documents)} documents to {output_file}")
else:
    print("⚠️ No documents found in the collection.")
