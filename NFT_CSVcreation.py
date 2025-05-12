import requests
import pandas as pd
import json
from datetime import datetime
import time
import matplotlib.pyplot as plt
import seaborn as sns
from tqdm import tqdm  # Add progress bar
import os

# Using OpenSea official Seaport subgraph
API_ENDPOINT = "https://api.studio.thegraph.com/query/111194/nft-trading-analysis/v0.0.1"

# Known NFT collection mappings
NFT_COLLECTIONS = {
    "0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d": "Bored Ape Yacht Club",
    "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb": "CryptoPunks",
    "0x60e4d786628fea6478f785a6d7e704777c86a7c6": "Mutant Ape Yacht Club",
    "0x34d85c9cdeb23fa97cb08333b511ac86e1c4e258": "Otherdeed for Otherside",
    "0x23581767a106ae21c074b2276d25e5c3e136a68b": "Moonbirds",
    # More known collections can be added here
}

def fetch_nft_sales(first=100, skip=0):
    """Fetch NFT sales data from The Graph API"""
    query = """
    {
      orderFulfilleds(first: %d, skip: %d, orderBy: blockTimestamp, orderDirection: desc) {
        id
        offerer
        recipient
        zone
        orderHash
        offer
        consideration
        blockNumber
        transactionHash
        blockTimestamp
      }
    }
    """ % (first, skip)
    
    try:
        response = requests.post(
            API_ENDPOINT,
            json={"query": query},
            timeout=30  # Increase timeout
        )
        
        if response.status_code != 200:
            print(f"Request failed: {response.status_code}")
            print(response.text)
            return []
        
        data = response.json()
        
        if "errors" in data:
            print("Query error:")
            print(json.dumps(data["errors"], indent=2))
            return []
        
        return data.get("data", {}).get("orderFulfilleds", [])
    
    except Exception as e:
        print(f"Error during request: {e}")
        time.sleep(5)  # Wait after error
        return []

def parse_sale_data(sale):
    """Parse raw sale data, extract basic information"""
    try:
        # Extract basic information
        transaction_hash = sale.get("transactionHash", "")
        offerer = sale.get("offerer", "")  # Seller
        recipient = sale.get("recipient", "")  # Buyer
        zone = sale.get("zone", "")
        order_hash = sale.get("orderHash", "")
        block_timestamp = int(sale.get("blockTimestamp", "0"))
        block_number = int(sale.get("blockNumber", "0"))
        
        # Time information processing
        date_time = datetime.fromtimestamp(block_timestamp)
        date_str = date_time.strftime('%Y-%m-%d')
        time_str = date_time.strftime('%H:%M:%S')
        day_of_week = date_time.weekday()  # 0=Monday, 6=Sunday
        weekday_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        day_name = weekday_names[day_of_week]
        hour = date_time.hour
        is_weekend = day_of_week >= 5  # 5=Saturday, 6=Sunday
        
        # Return processed record
        return {
            "transaction_hash": transaction_hash,
            "order_hash": order_hash,
            "seller": offerer,
            "buyer": recipient,
            "zone": zone,
            "timestamp": block_timestamp,
            "date": date_str,
            "time": time_str,
            "day_of_week": day_of_week,
            "day_name": day_name,
            "hour": hour,
            "is_weekend": is_weekend,
            "block_number": block_number,
        }
    
    except Exception as e:
        print(f"Error processing transaction data: {e}")
        return None

def collect_and_process_nft_sales(total_records=5000, batch_size=100, save_interval=1000):
    """
    Collect and process NFT sales data, saving results periodically
    
    Parameters:
    total_records (int): Total number of records to collect
    batch_size (int): Number of records to fetch per batch
    save_interval (int): How often to save records
    """
    all_sales = []
    processed_count = 0
    error_count = 0
    max_errors = 5  # Maximum consecutive error count
    
    # Create progress bar
    progress_bar = tqdm(total=total_records, desc="Collecting NFT transaction data")
    
    # If temp file exists, load previous progress
    temp_file = "temp_nft_transactions.csv"
    if os.path.exists(temp_file):
        print(f"Found temp file, loading previous progress...")
        temp_df = pd.read_csv(temp_file)
        all_sales = temp_df.to_dict('records')
        processed_count = len(all_sales)
        progress_bar.update(processed_count)
        print(f"Loaded {processed_count} records")
    
    try:
        for skip in range(processed_count, total_records, batch_size):
            try:
                # Get a batch of data
                batch = fetch_nft_sales(batch_size, skip)
                
                if not batch:
                    error_count += 1
                    if error_count >= max_errors:
                        print(f"Failed to fetch data {max_errors} consecutive times, stopping")
                        break
                    continue
                
                error_count = 0  # Reset error count
                
                # Process this batch
                new_records = 0
                for sale in batch:
                    processed_sale = parse_sale_data(sale)
                    if processed_sale:
                        all_sales.append(processed_sale)
                        new_records += 1
                
                processed_count += new_records
                progress_bar.update(new_records)
                
                # Save progress periodically
                if processed_count % save_interval == 0 or processed_count >= total_records:
                    temp_sales_df = pd.DataFrame(all_sales)
                    temp_sales_df.to_csv(temp_file, index=False)
                    print(f"\nTemporarily saved {processed_count} records to {temp_file}")
                
                # Avoid frequent requests
                if skip + batch_size < total_records and batch:
                    time.sleep(1)  # Reduce wait time for efficiency
            
            except Exception as e:
                print(f"\nError during batch processing (skip={skip}): {e}")
                time.sleep(5)  # Wait longer after error
    
    finally:
        progress_bar.close()
    
    # Convert to DataFrame
    if all_sales:
        sales_df = pd.DataFrame(all_sales)
        return sales_df
    else:
        print("No NFT transaction data found")
        return pd.DataFrame()

def analyze_data(sales_df):
    """Analyze transaction data and generate statistics and visualizations"""
    if sales_df.empty:
        print("No data available for analysis")
        return
    
    print("\nStarting transaction data analysis...")
    
    # 1. Basic information
    print(f"Total transactions: {len(sales_df)}")
    print(f"Date range: {sales_df['date'].min()} to {sales_df['date'].max()}")
    print(f"Unique buyers: {sales_df['buyer'].nunique()}")
    print(f"Unique sellers: {sales_df['seller'].nunique()}")
    
    # 2. Time analysis
    # By hour
    hourly_stats = sales_df.groupby("hour").size()
    print("\nTransactions by hour:")
    print(hourly_stats)
    
    # By day of week
    weekday_stats = sales_df.groupby("day_name").size()
    print("\nTransactions by day of week:")
    print(weekday_stats)
    
    # 3. Address analysis
    # Most active buyers
    top_buyers = sales_df["buyer"].value_counts().head(10)
    print("\nTop 10 most active buyers:")
    print(top_buyers)
    
    # Most active sellers
    top_sellers = sales_df["seller"].value_counts().head(10)
    print("\nTop 10 most active sellers:")
    print(top_sellers)
    
    # 4. Create visualizations
    plt.figure(figsize=(15, 12))
    
    # Time distribution
    plt.subplot(2, 2, 1)
    sales_df.groupby("hour").size().plot(kind="bar")
    plt.title("NFT Transaction Distribution by Hour")
    plt.xlabel("Hour of Day")
    plt.ylabel("Number of Transactions")
    
    # Day of week distribution
    plt.subplot(2, 2, 2)
    # Sort by day of week
    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    weekday_counts = sales_df.groupby("day_name").size().reindex(day_order)
    weekday_counts.plot(kind="bar")
    plt.title("NFT Transaction Distribution by Day of Week")
    plt.xticks(rotation=45)
    plt.ylabel("Number of Transactions")
    
    # Date trend
    plt.subplot(2, 2, 3)
    sales_df["date"] = pd.to_datetime(sales_df["date"])
    daily_counts = sales_df.groupby(sales_df["date"].dt.date).size()
    daily_counts.plot()
    plt.title("Daily NFT Transaction Volume")
    plt.xlabel("Date")
    plt.ylabel("Number of Transactions")
    
    # Buyer/seller analysis
    plt.subplot(2, 2, 4)
    transactions_per_address = pd.DataFrame({
        "Buyers": sales_df["buyer"].value_counts().value_counts(),
        "Sellers": sales_df["seller"].value_counts().value_counts()
    })
    transactions_per_address.plot(kind="bar")
    plt.title("Transaction Count Distribution")
    plt.xlabel("Number of Transactions per Address")
    plt.ylabel("Number of Addresses")
    plt.yscale("log")
    
    plt.tight_layout()
    plt.savefig("nft_transaction_analysis.png")
    print("\nVisualization saved to nft_transaction_analysis.png")
    
    return (hourly_stats, weekday_stats, top_buyers, top_sellers)

def prepare_data_for_llm(sales_df, sample_size=1000):
    """Prepare data sample for LLM analysis"""
    # Sort by timestamp
    sales_df = sales_df.sort_values("timestamp", ascending=False)
    
    # Add transaction index
    sales_df["transaction_index"] = range(1, len(sales_df) + 1)
    
    # Select subset of sales data
    if len(sales_df) > sample_size:
        # Sampling strategy: prioritize recent transactions, then randomly sample the rest
        recent_records = min(sample_size // 2, len(sales_df))
        recent_df = sales_df.head(recent_records)
        older_df = sales_df.iloc[recent_records:].sample(sample_size - recent_records)
        llm_sales = pd.concat([recent_df, older_df])
    else:
        llm_sales = sales_df
    
    # Select important columns
    llm_sales = llm_sales[[
        'transaction_index', 'transaction_hash', 'seller', 'buyer', 
        'date', 'time', 'day_name', 'hour', 'is_weekend'
    ]]
    
    # Save as CSV for LLM analysis
    llm_sales.to_csv("llm_nft_transactions_sample.csv", index=False)
    print(f"Saved {len(llm_sales)} sample transactions to llm_nft_transactions_sample.csv (for LLM analysis)")
    
    # Prepare statistical summaries for LLM
    hourly_stats = sales_df.groupby("hour").size().reset_index(name="transactions")
    hourly_stats.to_csv("llm_hourly_stats.csv", index=False)
    
    weekday_stats = sales_df.groupby(["day_of_week", "day_name"]).size().reset_index(name="transactions")
    weekday_stats = weekday_stats.sort_values("day_of_week")
    weekday_stats.to_csv("llm_weekday_stats.csv", index=False)
    
    # Buyer analysis
    buyer_stats = sales_df.groupby("buyer").size().reset_index(name="transactions")
    buyer_stats = buyer_stats.sort_values("transactions", ascending=False)
    buyer_stats.head(100).to_csv("llm_top_buyers.csv", index=False)
    
    # Seller analysis
    seller_stats = sales_df.groupby("seller").size().reset_index(name="transactions")
    seller_stats = seller_stats.sort_values("transactions", ascending=False)
    seller_stats.head(100).to_csv("llm_top_sellers.csv", index=False)
    
    # Date statistics
    date_stats = sales_df.groupby("date").size().reset_index(name="transactions")
    date_stats["date"] = pd.to_datetime(date_stats["date"])
    date_stats = date_stats.sort_values("date")
    date_stats.to_csv("llm_daily_transactions.csv", index=False)
    
    print("All LLM analysis data preparation complete")

def main():
    print("Starting NFT transaction data collection...")
    
    # Collect more data - start with 5000, can increase as needed
    sales_df = collect_and_process_nft_sales(total_records=5000, batch_size=100, save_interval=500)
    
    if sales_df.empty:
        print("No NFT transaction data found, exiting program")
        return
    
    # Save raw data
    sales_df.to_csv("nft_transactions.csv", index=False)
    print(f"Saved {len(sales_df)} NFT transaction records to nft_transactions.csv")
    
    # Analyze data and generate visualizations
    analyze_data(sales_df)
    
    # Prepare data for LLM analysis
    prepare_data_for_llm(sales_df, sample_size=1000)
    
    print("Data processing complete!")
    
    # Delete temporary file
    if os.path.exists("temp_nft_transactions.csv"):
        os.remove("temp_nft_transactions.csv")
        print("Temporary file deleted")

if __name__ == "__main__":
    main()