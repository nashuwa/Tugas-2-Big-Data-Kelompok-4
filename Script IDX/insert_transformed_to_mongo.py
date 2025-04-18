# pip install pymongo

import json
import os
from pymongo import MongoClient

# Koneksi ke MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["idx_tugas2"]
collection = db["data_terstruktur"]

# Path file hasil transformasi
json_path = os.path.abspath("transformed_financial_data.json")

if not os.path.exists(json_path):
    print("File hasil transformasi tidak ditemukan.")
    exit()

# Masukkan JSON ke MongoDB
try:
    with open(json_path, "r", encoding="utf-8") as file:
        data = json.load(file)
        if isinstance(data, list):
            collection.insert_many(data)
            print(f"{len(data)} dokumen berhasil dimasukkan ke MongoDB.")
        else:
            print("Format data bukan list, gagal dimasukkan.")
except Exception as e:
    print(f"ERROR saat memasukkan data: {e}")

print(f"Total dokumen di MongoDB: {collection.count_documents({})}")
print("Proses selesai.")
