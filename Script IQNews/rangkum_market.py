import re
import torch
from pymongo import MongoClient
from transformers import pipeline, AutoTokenizer, AutoModelForSeq2SeqLM

# Setup model lokal (dilakukan sekali saja di awal)
model_name = "facebook/bart-large-cnn"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForSeq2SeqLM.from_pretrained(model_name)
summarizer = pipeline("summarization", model=model, tokenizer=tokenizer, device=0 if torch.cuda.is_available() else -1)

# Fungsi membersihkan teks
def clean_text(text):
    text = re.sub(r"\(.*?\)", "", text)
    text = re.sub(r"IQPlus,|\"|“|”|‘|’|\(end\)", "", text)
    return text.strip()

# Fungsi membagi teks menjadi chunk berdasarkan karakter (lebih efisien)
def chunk_text_by_char(text, max_chars=1000):
    sentences = text.split('. ')
    chunks = []
    current_chunk = ""

    for sentence in sentences:
        if len(current_chunk) + len(sentence) < max_chars:
            current_chunk += sentence + ". "
        else:
            chunks.append(current_chunk.strip())
            current_chunk = sentence + ". "
    if current_chunk:
        chunks.append(current_chunk.strip())
    return chunks

# Fungsi meringkas teks
def summarize_text(text):
    cleaned = clean_text(text)

    if len(cleaned) < 30:
        print("⚠️ Teks terlalu pendek, tidak diringkas.")
        return cleaned

    chunks = chunk_text_by_char(cleaned)
    summaries = []

    for chunk in chunks:
        try:
            summary = summarizer(chunk, max_length=250, min_length=50, do_sample=False)[0]['summary_text']
            summaries.append(summary)
        except Exception as e:
            print(f"⚠️ Error saat meringkas chunk: {e}")
            continue

    return " ".join(summaries)

# Koneksi ke MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["news_db"]
collection = db["articles2"]

# Batch process artikel dengan _id > last_id
batch_size = 10
last_id = None

while True:
    query = {"_id": {"$gt": last_id}} if last_id else {}
    batch = list(collection.find(query).sort("_id", 1).limit(batch_size))

    if not batch:
        print("✅ Semua artikel sudah diproses.")
        break

    for doc in batch:
        doc_id = doc["_id"]
        original_text = doc.get("konten", "")
        existing_summary = doc.get("ringkasan", None)
        last_id = doc_id  # update untuk batch selanjutnya

        if existing_summary and existing_summary.strip():
            print(f"🔁 Lewati ID {doc_id} karena sudah ada ringkasan.")
            continue

        if not original_text or not original_text.strip():
            collection.update_one({"_id": doc_id}, {"$set": {"ringkasan": None}})
            print(f"⚠️ Kosong/null pada ID: {doc_id}, disimpan None.")
            continue

        print(f"\n📄 Memproses ID: {doc_id}")
        summary = summarize_text(original_text)
        collection.update_one({"_id": doc_id}, {"$set": {"ringkasan": summary}})
        if summary:
            print(f"✅ Ringkasan disimpan untuk ID: {doc_id}")
        else:
            print(f"⚠️ Ringkasan gagal, disimpan None.")
