# spark_transform_json.py
# pip install pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import os
import json

# Inisialisasi Spark Session
spark = SparkSession.builder \
    .appName("Transformasi Data Keuangan IDX") \
    .getOrCreate()

print("Memulai transformasi data JSON dengan Apache Spark...")

# Folder yang berisi file JSON
json_folder = os.path.abspath("downloads")
json_files = [f for f in os.listdir(json_folder) if f.endswith(".json")]

if not json_files:
    print("Tidak ada file JSON untuk diproses.")
    exit()

all_data = []

# Baca dan gabungkan semua file JSON
for json_file in json_files:
    path = os.path.join(json_folder, json_file)
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if "laporan_keuangan" in data:
                lk = data["laporan_keuangan"]

                # Mapping ke nama atribut pendek dan bahasa Indonesia
                mapped = {
                    "nama": data.get("emiten", "Unknown"),

                    # Laba Rugi
                    "pendapatan": lk.get("SalesAndRevenue") or lk.get("SalesAndRevenueMoreThan10Percent"),
                    "laba_kotor": lk.get("GrossProfit"),
                    "laba_bersih": lk.get("ProfitLoss") or lk.get("ProfitLossAttributableToParentEntity"),
                    "laba_sebelum_pajak": lk.get("ProfitLossBeforeIncomeTax"),

                    # Neraca
                    "kas": lk.get("CashAndCashEquivalents"),
                    "aset": lk.get("Assets"),
                    "ekuitas": lk.get("Equity") or lk.get("EquityAttributableToEquityOwnersOfParentEntity"),
                    "pinjaman_pendek": lk.get("ShortTermLoans"),
                    "pinjaman_panjang": lk.get("LongTermBankLoans"),

                    # Arus Kas
                    "arus_operasi": lk.get("NetCashFlowsReceivedFromUsedInOperatingActivities"),
                    "arus_investasi": lk.get("NetCashFlowsReceivedFromUsedInInvestingActivities"),
                    "arus_pendanaan": lk.get("NetCashFlowsReceivedFromUsedInFinancingActivities"),

                    # Metadata
                    "source_file": json_file
                }

                all_data.append(mapped)
    except Exception as e:
        print(f"ERROR membaca {json_file}: {e}")

if not all_data:
    print("Data kosong setelah parsing JSON.")
    exit()

# Tentukan schema
schema = StructType([
    StructField("nama", StringType(), True),
    StructField("pendapatan", StringType(), True),
    StructField("laba_kotor", StringType(), True),
    StructField("laba_bersih", StringType(), True),
    StructField("laba_sebelum_pajak", StringType(), True),
    StructField("kas", StringType(), True),
    StructField("aset", StringType(), True),
    StructField("ekuitas", StringType(), True),
    StructField("pinjaman_pendek", StringType(), True),
    StructField("pinjaman_panjang", StringType(), True),
    StructField("arus_operasi", StringType(), True),
    StructField("arus_investasi", StringType(), True),
    StructField("arus_pendanaan", StringType(), True),
    StructField("source_file", StringType(), True),
])

# Buat DataFrame dari data JSON
df = spark.createDataFrame(all_data, schema=schema)

# Fungsi konversi string ke float yang aman
def to_float_safe(column):
    return when(col(column).rlike(r"^[0-9.\-]+$"), col(column).cast("float")).otherwise(None)

# Transformasi numerik dan hitung rasio
transformed_df = df \
    .withColumn("pendapatan", to_float_safe("pendapatan")) \
    .withColumn("laba_kotor", to_float_safe("laba_kotor")) \
    .withColumn("laba_bersih", to_float_safe("laba_bersih")) \
    .withColumn("laba_sebelum_pajak", to_float_safe("laba_sebelum_pajak")) \
    .withColumn("kas", to_float_safe("kas")) \
    .withColumn("aset", to_float_safe("aset")) \
    .withColumn("ekuitas", to_float_safe("ekuitas")) \
    .withColumn("pinjaman_pendek", to_float_safe("pinjaman_pendek")) \
    .withColumn("pinjaman_panjang", to_float_safe("pinjaman_panjang")) \
    .withColumn("arus_operasi", to_float_safe("arus_operasi")) \
    .withColumn("arus_investasi", to_float_safe("arus_investasi")) \
    .withColumn("arus_pendanaan", to_float_safe("arus_pendanaan")) \
    .withColumn("margin_laba", when(col("pendapatan") > 0, col("laba_bersih") / col("pendapatan"))) \
    .withColumn("rasio_ekuitas_aset", when(col("aset") > 0, col("ekuitas") / col("aset")))

# Tampilkan ringkasan hasil transformasi
print("\nHasil transformasi ringkas:")
transformed_df.select(
    "nama", "pendapatan", "laba_bersih", "margin_laba", "aset", "ekuitas", "rasio_ekuitas_aset"
).orderBy(col("margin_laba").desc()).show(truncate=False)

# Simpan ke file JSON
output_path = os.path.abspath("transformed_financial_data.json")
transformed_df.toPandas().to_json(output_path, orient="records", indent=4, force_ascii=False)

print(f"\nTransformasi selesai dan disimpan ke: {output_path}")
spark.stop()
