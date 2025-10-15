import yfinance as yf
import pandas as pd
import os
import glob
import textwrap
from datetime import datetime

# Optional libraries, imported safely.
try:
    from google.cloud import storage
except ImportError:
    storage = None

try:
    import matplotlib.pyplot as plt
    import seaborn as sns
except ImportError:
    plt = None
    sns = None

# --- Script Configuration ---
LOCAL_DATA_LAKE_FOLDER = "stock_data_lake"
GCS_BUCKET_NAME = "your-gcs-bucket-name" # <-- EDIT THIS FOR GCP OPERATIONS

def get_user_choice(prompt, options):
    """Prompts the user for a choice and validates it."""
    while True:
        choice = input(prompt).lower().strip()
        if choice in options:
            return choice
        else:
            print(f"Invalid choice. Please select one of {options}.")

def get_analysis_parameters():
    """Gets ticker and period from the user."""
    print("\n--- Asset & Period Selection ---")
    print("Formats: NSE -> RELIANCE.NS | BSE -> 500325.BO | MF -> AXISBLUECHIP.NS")
    ticker = input("Enter ticker symbol: ").upper().strip()

    period_options = {'1': '7d', '2': '1mo', '3': '1y', '4': '3y', '5': '5y', '6': 'max'}
    period_map_display = {'1': 'Weekly (7d)', '2': 'Monthly (1mo)', '3': 'Yearly (1y)', '4': '3 Years', '5': '5 Years', '6': 'All Time'}
    
    print("\nSelect time period:")
    for key, value in period_map_display.items():
        print(f"  {key}: {value}")

    choice = get_user_choice("Enter choice (1-6): ", period_options.keys())
    return ticker, period_options[choice]

def plot_stock_performance(df, ticker, period):
    """Generates and displays a plot of stock performance."""
    if not plt or not sns:
        print("\nWARNING: Plotting libraries not installed (matplotlib, seaborn). Cannot generate graph.")
        print("--- Data for {ticker} ---\n", df.tail())
        return

    print("\nGenerating plot...")
    df['Date'] = pd.to_datetime(df['Date'])
    df.set_index('Date', inplace=True)
    sns.set(style='darkgrid')
    plt.figure(figsize=(14, 7))
    plt.plot(df.index, df['Close'], label='Close Price', color='navy')
    plt.title(f'Performance for {ticker} (Period: {period.upper()})', fontsize=16)
    plt.xlabel('Date', fontsize=12)
    plt.ylabel('Price (INR)', fontsize=12)
    plt.legend()
    plt.grid(True)
    plt.show()

def get_gcs_bucket(bucket_name):
    """Initializes and returns a GCS bucket object, handling errors."""
    if not storage:
        print("\nFATAL: 'google-cloud-storage' not installed. Run 'pip install google-cloud-storage'.")
        return None
    try:
        return storage.Client().bucket(bucket_name)
    except Exception:
        print("\nFATAL: Could not initialize GCS client. Ensure GOOGLE_APPLICATION_CREDENTIALS is set.")
        return None

def save_data_locally(df, ticker, file_date):
    """Saves data to a partitioned local folder."""
    path = os.path.join(LOCAL_DATA_LAKE_FOLDER, f"ticker={ticker}", f"date={file_date}.csv")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)

def upload_data_to_gcs(df, ticker, file_date, bucket):
    """Uploads data as a CSV to a partitioned GCS path."""
    blob = bucket.blob(f"ticker={ticker}/date={file_date}.csv")
    blob.upload_from_string(df.to_csv(index=False), 'text/csv')

def fetch_and_store_data(ticker, period, storage_target, gcs_bucket=None):
    """Fetches and stores stock data."""
    print(f"\n[1] Fetching data for '{ticker}' | Target: {storage_target.upper()}")
    try:
        stock_data = yf.download(ticker, period=period, interval="1d")
        if stock_data.empty:
            print(f"ERROR: No data found for '{ticker}'. Please check the symbol.")
            return None
        
        print(f"-> Fetched {len(stock_data)} data points.")
        for index, row in stock_data.iterrows():
            day_df = pd.DataFrame([row]).reset_index()
            day_df.rename(columns={'index': 'Date'}, inplace=True)
            day_df['Ticker'] = ticker
            file_date = day_df['Date'].iloc[0].strftime('%Y-%m-%d')
            if storage_target == 'local':
                save_data_locally(day_df, ticker, file_date)
            elif storage_target == 'gcp' and gcs_bucket:
                upload_data_to_gcs(day_df, ticker, file_date, gcs_bucket)
        
        print("-> Data ingestion complete.")
        return stock_data.reset_index()
    except Exception as e:
        print(f"An error occurred during data fetch: {e}")
        return None

def process_data_locally(fetched_data, ticker, period):
    """Processes data locally and generates a plot."""
    print(f"\n[2] Processing data locally for '{ticker}'...")
    if fetched_data is None or fetched_data.empty:
        print("-> No data to process.")
        return
    plot_stock_performance(fetched_data, ticker, period)

def provide_databricks_instructions(ticker):
    """Prints instructions and PySpark code for Databricks."""
    db_code = f'''
# PySpark Code for Databricks Analysis

# 1. Configure GCS Access in your Databricks Cluster Spark config.
#    (See project README for details on setting up the service account)
#
#    spark.hadoop.google.cloud.auth.service.account.enable true
#    spark.hadoop.fs.gs.auth.service.account.json.keyfile /dbfs/path/to/key.json

# 2. Create a widget to select the ticker.
dbutils.widgets.text("ticker_symbol", "{ticker}", "Enter Ticker")

# 3. Read the data from GCS.
gcs_path = "gs://{GCS_BUCKET_NAME}/"
df = (spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(gcs_path))
df.createOrReplaceTempView("stock_prices")

# 4. Run analysis. (Example: Display historical price)
display(spark.sql(f"""
    SELECT Date, Close
    FROM stock_prices
    WHERE Ticker = '{{dbutils.widgets.get("ticker_symbol")}}'
    ORDER BY Date
"""))
'''
    print("\n[2] Data is ready for Databricks analysis.")
    print("-> Paste the following PySpark code into a Databricks notebook cell:")
    print("-" * 70)
    print(textwrap.dedent(db_code))
    print("-" * 70)

def main():
    """Main function to orchestrate the pipeline."""
    storage_target = get_user_choice("Select storage target (local/gcp): ", ['local', 'gcp'])
    processing_target = get_user_choice("Select processing target (local/databricks): ", ['local', 'databricks'])
    ticker, period = get_analysis_parameters()

    if not ticker:
        print("No ticker provided. Exiting.")
        return
    if storage_target == 'gcp' and GCS_BUCKET_NAME == "your-gcs-bucket-name":
         print("\nERROR: Please update the GCS_BUCKET_NAME in the script before using GCP.")
         return

    gcs_bucket = get_gcs_bucket(GCS_BUCKET_NAME) if storage_target == 'gcp' else None
    if storage_target == 'gcp' and not gcs_bucket:
        return

    fetched_data = fetch_and_store_data(ticker, period, storage_target, gcs_bucket)

    if processing_target == 'local':
        process_data_locally(fetched_data, ticker, period)
    elif processing_target == 'databricks':
        provide_databricks_instructions(ticker)

if __name__ == "__main__":
    main()
