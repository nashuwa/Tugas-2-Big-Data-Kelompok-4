# pip install pymongo

import json
import os
from pymongo import MongoClient

# Koneksi ke MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["idx_tugas2"]
collection = db["laporan_tahunan"]

# Folder JSON
json_folder = os.path.abspath("downloads")
json_files = [f for f in os.listdir(json_folder) if f.endswith(".json")]

if not json_files:
    print("Tidak ada file JSON untuk diproses.")
    exit()

# Masukkan JSON ke MongoDB
for json_file in json_files:
    json_path = os.path.join(json_folder, json_file)

    try:
        with open(json_path, "r", encoding="utf-8") as file:
            data = json.load(file)
            collection.insert_one(data)
            print(f"Data dari {json_file} berhasil dimasukkan ke MongoDB.")
    except Exception as e:
        print(f"ERROR saat memasukkan {json_file}: {e}")

print(f"Total dokumen di MongoDB: {collection.count_documents({})}")
print("Proses selesai.")