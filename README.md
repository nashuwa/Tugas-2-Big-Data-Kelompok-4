# Tugas-2-Big-Data-Kelompok-4

Repository ini berisi kumpulan skrip otomatisasi yang dirancang untuk melakukan pengambilan (scraping), transformasi, dan penyimpanan data dari berbagai sumber keuangan, seperti Bursa Efek Indonesia (IDX), Yahoo Finance (yfinance), dan berita pasar modal dari IQPlus. Fokus utama dari tugas ini adalah melakukan transformasi data hasil scraping menjadi format terstruktur, khususnya dengan memanfaatkan Apache Spark untuk pemrosesan skala besar dan MongoDB untuk penyimpanan data.

## Struktur Folder
Berikut adalah penjelasan mengenai struktur folder dalam repository ini:

### Script IDX
Berisi skrip untuk mengambil dan mentransformasi data laporan keuangan perusahaan dari Bursa Efek Indonesia (IDX).
- `scrape_idx.py` : Mengambil data laporan keuangan berbentuk XML dari situs IDX.
- `insert_to_mongodb.py` : Menyimpan data mentah hasil scraping ke dalam MongoDB.
- `insert_transformed_to_mongo.py` : Menyimpan data hasil transformasi ke MongoDB setelah diproses oleh Apache Spark.
- `spark_transform_direct.py` : Melakukan transformasi data dari XML menjadi data terstruktur menggunakan Apache Spark. Data yang diambil meliputi: revenue, gross profit, operating profit, net profit, cash, total asset, short term borrowing, long term borrowing, total equity, cash dari operasi, investasi, dan pendanaan.
- `transformed_financial_data.json` : Contoh hasil transformasi data keuangan yang telah disimpan dalam format JSON.
- `webdriver/` : Folder berisi `msedgedriver.exe`, digunakan untuk web scraping.

### Script IQNews
Berisi skrip untuk merangkum berita pasar modal dari IQPlus.
- `rangkum_market.py` : Merangkum berita pasar menggunakan HuggingFace Transformer
- `rangkum_stock.py` : Merangkum berita saham menggunakan HuggingFace Transformer

### Script yfinance
Berisi skrip untuk mengambil dan mengolah data harga saham dari Yahoo Finance.
- `plot_stock_data.py` : Menyediakan visualisasi data harga saham hasil agregasi.
- `stock_to_spark.py` : Mengambil data historis saham dari yfinance dan mengagregasinya ke dalam periode harian, bulanan, tahunan (1, 3, 5 tahun), dengan pemrosesan menggunakan Apache Spark. Hasilnya dapat dengan cepat digunakan untuk plotting via API.
- `tickers.xlsx` : File Excel yang berisi daftar ticker saham yang digunakan sebagai input.

## Fitur Utama
- **Pengambilan Data**: Menggunakan API (yfinance), web scraping (IDX, IQPlus), dan LLM untuk memperoleh data relevan dari berbagai sumber.
- **Transformasi Data**: Menggunakan Apache Spark untuk memproses data dalam jumlah besar menjadi bentuk terstruktur yang siap dianalisis.
- **Ringkasan Berita**: Menggunakan pendekatan NLP/LLM untuk merangkum berita saham dan pasar modal menjadi informasi singkat yang mudah dipahami.
- **Penyimpanan Data**: Data yang telah dikumpulkan dan diproses disimpan ke dalam MongoDB untuk kemudahan akses dan integrasi analisis lanjutan.
- **Visualisasi Data**: Menyediakan skrip untuk melakukan plotting data harga saham berdasarkan hasil agregasi periode waktu tertentu.
