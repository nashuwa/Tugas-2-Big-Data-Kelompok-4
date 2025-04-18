import pandas as pd
import yfinance as yf
import time
from datetime import datetime
import math
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, first, max as spark_max, min as spark_min, last, sum as spark_sum
import os

# --- Initialize Spark Session ---
print("Initializing Apache Spark...")
spark = SparkSession.builder \
    .appName("StockDataProcessor") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/stock_data") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2") \
    .getOrCreate()

print("✅ Spark session established")

# --- Load ticker dari Excel ---
print("📋 Loading tickers from Excel...")
try:
    # Use pandas to read Excel and convert to Spark DataFrame
    tickers_df = pd.read_excel("tickers.xlsx")
    tickers = [t + ".JK" for t in tickers_df["Ticker"].tolist()]
    print(f"✅ Loaded {len(tickers)} tickers")
except Exception as e:
    print(f"❌ Error loading tickers: {e}")
    # Fallback to a small list for testing
    tickers = ["BBRI.JK", "BBCA.JK", "TLKM.JK", "ASII.JK", "BMRI.JK"]
    print(f"🔄 Using fallback list of {len(tickers)} tickers")

def resample_data_spark(spark_df, ticker, timeframe):
    """
    Resample data to different timeframes using Spark SQL
    """
    # Register the DataFrame as a temporary view
    spark_df.createOrReplaceTempView("stock_data")
    
    # Add ticker column if not exists
    if "Ticker" not in spark_df.columns:
        spark_df = spark_df.withColumn("Ticker", lit(ticker))
    
    # Check if Adj Close exists
    has_adj_close = "Adj Close" in spark_df.columns
    adj_close_clause = ", last(Adj Close) as Adj Close" if has_adj_close else ""
    
    if timeframe == "daily":
        # Daily data remains the same
        return spark_df
    
    elif timeframe == "weekly":
        # Weekly resampling - using Spark SQL for date-based aggregation
        weekly_query = f"""
        SELECT 
            date_trunc('week', Date) as Date,
            first(Open) as Open,
            max(High) as High,
            min(Low) as Low,
            last(Close) as Close{adj_close_clause},
            sum(Volume) as Volume,
            first(Ticker) as Ticker
        FROM stock_data
        GROUP BY date_trunc('week', Date)
        ORDER BY Date
        """
        return spark.sql(weekly_query)
    
    elif timeframe == "monthly":
        # Monthly resampling
        monthly_query = f"""
        SELECT 
            date_trunc('month', Date) as Date,
            first(Open) as Open,
            max(High) as High,
            min(Low) as Low,
            last(Close) as Close{adj_close_clause},
            sum(Volume) as Volume,
            first(Ticker) as Ticker
        FROM stock_data
        GROUP BY date_trunc('month', Date)
        ORDER BY Date
        """
        return spark.sql(monthly_query)
    
    elif timeframe == "yearly":
        # Yearly resampling
        yearly_query = f"""
        SELECT 
            date_trunc('year', Date) as Date,
            first(Open) as Open,
            max(High) as High,
            min(Low) as Low,
            last(Close) as Close{adj_close_clause},
            sum(Volume) as Volume,
            first(Ticker) as Ticker
        FROM stock_data
        GROUP BY date_trunc('year', Date)
        ORDER BY Date
        """
        return spark.sql(yearly_query)
    
    elif timeframe in ["3year", "5year"]:
        # First generate yearly data
        yearly_query = f"""
        SELECT 
            date_trunc('year', Date) as Date,
            year(Date) as year_num,
            first(Open) as Open,
            max(High) as High,
            min(Low) as Low,
            last(Close) as Close{adj_close_clause},
            sum(Volume) as Volume,
            first(Ticker) as Ticker
        FROM stock_data
        GROUP BY date_trunc('year', Date), year(Date)
        ORDER BY Date
        """
        
        # Create temporary view of yearly data
        spark.sql(yearly_query).createOrReplaceTempView("yearly_data")
        
        # Get minimum year for grouping reference
        min_year_row = spark.sql("SELECT MIN(year_num) as min_year FROM yearly_data").collect()
        min_year = min_year_row[0]['min_year']
        
        # Group size based on timeframe
        group_size = 3 if timeframe == "3year" else 5
        
        # Create the multi-year grouping query
        multi_year_query = f"""
        SELECT 
            FLOOR((year_num - {min_year}) / {group_size}) as group_id,
            MIN(year_num) as start_year,
            MAX(year_num) as end_year,
            FIRST(Open) as Open,
            MAX(High) as High,
            MIN(Low) as Low,
            LAST(Close) as Close,
            SUM(Volume) as Volume,
            FIRST(Ticker) as Ticker,
            MAX(Date) as Date
            {', LAST(Adj Close) as Adj Close' if has_adj_close else ''}
        FROM yearly_data
        GROUP BY FLOOR((year_num - {min_year}) / {group_size})
        ORDER BY group_id
        """
        
        # Execute multi-year query
        multi_year_df = spark.sql(multi_year_query)
        
        # Format the Date to be the end of the last year in the period
        # And create a more descriptive period label
        period_df = multi_year_df.selectExpr(
            "Date",
            "Open", 
            "High",
            "Low",
            "Close",
            f"{'Adj Close,' if has_adj_close else ''}"
            "Volume",
            "Ticker",
            "CONCAT(start_year, '-', end_year) as period"
        )
        
        # Return result without the grouping columns
        return period_df.select(
            "Date", 
            "Open", 
            "High", 
            "Low", 
            "Close", 
            *(["Adj Close"] if has_adj_close else []),
            "Volume", 
            "Ticker",
            "period"
        )
    
    return None

def save_to_mongodb(spark_df, collection_name, ticker, timeframe):
    """
    Save Spark DataFrame to MongoDB using the Spark MongoDB connector
    """
    if spark_df is None or spark_df.count() == 0:
        print(f"⚠ No {timeframe} data to save for {ticker}")
        return 0
    
    # Add metadata fields for MongoDB
    df_with_metadata = spark_df.withColumn("timeframe", lit(timeframe)) \
                               .withColumn("inserted_at", lit(datetime.now()))
    
    # Prepare columns for MongoDB (based on what's available)
    columns_to_select = [
        col("Ticker").alias("ticker"),
        col("Date").alias("date"),
        col("timeframe"),
        col("Open").alias("open"),
        col("High").alias("high"),
        col("Low").alias("low"),
        col("Close").alias("close"),
        col("Volume").alias("volume"),
        col("inserted_at")
    ]
    
    # Add adj_close only if it exists in the DataFrame
    if "Adj Close" in spark_df.columns:
        columns_to_select.append(col("Adj Close").alias("adj_close"))
    
    df_to_save = df_with_metadata.select(*columns_to_select)
    
    # Write to MongoDB
    try:
        # Use the MongoDB Spark connector to write data
        df_to_save.write \
                 .format("mongo") \
                 .option("collection", collection_name) \
                 .mode("append") \
                 .save()
        
        record_count = df_to_save.count()
        print(f"💾 Saved {record_count} {timeframe} records for {ticker}")
        return record_count
    except Exception as e:
        print(f"❌ Failed to save {timeframe} data for {ticker} to MongoDB: {e}")
        return 0

# --- Konfigurasi parallelism ---
BATCH_SIZE = 5  # Jumlah ticker per batch
total_batches = math.ceil(len(tickers) / BATCH_SIZE)

# Counter untuk tracking
timeframes = ["daily", "weekly", "monthly", "yearly", "3year", "5year"]
total_documents = {timeframe: 0 for timeframe in timeframes}
successful_tickers = 0

# --- Proses ticker per batch ---
for batch_idx in range(total_batches):
    start_idx = batch_idx * BATCH_SIZE
    end_idx = min((batch_idx + 1) * BATCH_SIZE, len(tickers))
    batch_tickers = tickers[start_idx:end_idx]
    
    print(f"\n🔄 Processing Batch {batch_idx + 1}/{total_batches} ({start_idx + 1}-{end_idx} of {len(tickers)} tickers)")
    
    for ticker in batch_tickers:
        ticker_success = False
        print(f"\n📥 Fetching data: {ticker}")
        
        for attempt in range(3):  # Coba maksimum 3 kali
            try:
                # Download data using yfinance
                pd_df = yf.download(ticker, period="5y")
                if pd_df.empty:
                    print(f"⚠ Empty data for {ticker}, possibly not available on Yahoo Finance.")
                    break
                
                # Show column names for debugging
                print(f"🔍 Original columns: {pd_df.columns}")
                
                # Handle multiindex columns if they exist
                if isinstance(pd_df.columns, pd.MultiIndex):
                    print("🔄 Flattening MultiIndex columns")
                    # Convert multiindex columns to flat columns
                    pd_df.columns = [' '.join(col).strip() for col in pd_df.columns.values]
                
                # Reset index to make Date a column
                pd_df = pd_df.reset_index()
                
                # Rename columns to standard names
                column_mapping = {
                    'Open AALI.JK': 'Open',
                    'High AALI.JK': 'High',
                    'Low AALI.JK': 'Low',
                    'Close AALI.JK': 'Close',
                    'Volume AALI.JK': 'Volume',
                    'Adj Close AALI.JK': 'Adj Close'
                }
                
                # Dynamically replace ticker name in columns
                actual_mapping = {}
                for old_name in pd_df.columns:
                    if ticker in old_name:
                        new_name = old_name.replace(f' {ticker}', '')
                        actual_mapping[old_name] = new_name
                
                # Rename columns if they match our pattern
                pd_df = pd_df.rename(columns=actual_mapping)
                
                # Add ticker column
                pd_df["Ticker"] = ticker
                
                # Print final columns
                print(f"🔍 Final columns: {pd_df.columns.tolist()}")
                
                # Create Spark DataFrame (let Spark infer schema)
                spark_df = spark.createDataFrame(pd_df)
                
                # Debug information
                print(f"📊 Found {spark_df.count()} days of data for {ticker}")
                
                # Process data for each timeframe
                print("🔄 Resampling data to different timeframes using Spark...")
                ticker_docs_counts = {}
                
                for timeframe in timeframes:
                    # Resample to the specific timeframe
                    resampled_df = resample_data_spark(spark_df, ticker, timeframe)
                    
                    # Save to MongoDB
                    collection_name = f"{timeframe}_prices"
                    count = save_to_mongodb(resampled_df, collection_name, ticker, timeframe)
                    ticker_docs_counts[timeframe] = count
                    total_documents[timeframe] += count
                
                # Check if we successfully inserted any data
                if sum(ticker_docs_counts.values()) > 0:
                    ticker_success = True
                    successful_tickers += 1
                    print(f"✅ Successfully processed {ticker} data for all timeframes")
                    for tf, count in ticker_docs_counts.items():
                        if count > 0:
                            print(f"   - {tf.capitalize()}: {count} records")
                else:
                    print(f"⚠ No data was saved for {ticker}")
                
                break  # Exit retry loop
                
            except Exception as e:
                print(f"❌ Failed to process {ticker} (attempt {attempt+1}): {e}")
                if attempt < 2:  # Only sleep if we're going to retry
                    time.sleep(2)  # Wait 2 seconds before retrying
    
    print(f"\n📈 Batch {batch_idx + 1} summary: Processed {len(batch_tickers)} tickers")

# --- Final summary ---
print("\n====== OPERATION COMPLETE ======")
print(f"📊 Total tickers processed: {successful_tickers}/{len(tickers)}")
for timeframe in timeframes:
    print(f"📊 {timeframe.capitalize()} records saved to MongoDB: {total_documents[timeframe]}")

# Stop Spark session
spark.stop()
print("\n✨ Program finished! ✨")