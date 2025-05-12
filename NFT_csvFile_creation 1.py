import requests
import pandas as pd
import json
from datetime import datetime
import time
import matplotlib.pyplot as plt
import seaborn as sns
from tqdm import tqdm
import os

# OpenSea Seaport subgraph endpoint
API_ENDPOINT = "https://api.studio.thegraph.com/query/111194/nft-trading-analysis/version/latest"

# Dictionary of known NFT collections
NFT_COLLECTIONS = {
    "0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d": "Bored Ape Yacht Club",
    "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb": "CryptoPunks",
    "0x60e4d786628fea6478f785a6d7e704777c86a7c6": "Mutant Ape Yacht Club",
    "0x34d85c9cdeb23fa97cb08333b511ac86e1c4e258": "Otherdeed for Otherside",
    "0x23581767a106ae21c074b2276d25e5c3e136a68b": "Moonbirds",
}

def fetch_nft_sales(first=100, skip=0):
    """
    Fetch NFT sales data from The Graph API
    
    Args:
        first: Number of records to fetch
        skip: Number of records to skip
    Returns:
        List of sales data
    """
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

    response = requests.post(
        API_ENDPOINT,
        json={"query": query},
        timeout=30
    )

    if response.status_code != 200:
        print(f"Request failed with status code: {response.status_code}")
        return []

    data = response.json()
    return data.get("data", {}).get("orderFulfilleds", [])


def parse_sale_data(sale):
    """
    Parse raw sale data and extract key information
    
    Args:
        sale: Raw sale data from GraphQL
        
    Returns:
        Dictionary with processed sale information
    """
    transaction_hash = sale.get("transactionHash", "")
    offerer = sale.get("offerer", "")  # Seller
    recipient = sale.get("recipient", "")  # Buyer
    zone = sale.get("zone", "")
    order_hash = sale.get("orderHash", "")
    block_timestamp = int(sale.get("blockTimestamp", "0"))
    block_number = int(sale.get("blockNumber", "0"))

    # Process timestamp into useful date/time components
    date_time = datetime.fromtimestamp(block_timestamp)
    date_str = date_time.strftime('%Y-%m-%d')
    time_str = date_time.strftime('%H:%M:%S')
    day_of_week = date_time.weekday()  # 0=Monday, 6=Sunday
    weekday_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    day_name = weekday_names[day_of_week]
    hour = date_time.hour
    is_weekend = day_of_week >= 5  # 5=Saturday, 6=Sunday

    # Create record with extracted data
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


def collect_and_process_nft_sales(total_records=5000, batch_size=100, save_interval=1000):
    """
    Collect and process NFT sales data from The Graph
    Args:
        total_records: Total number of records to collect
        batch_size: Number of records to fetch per batch
        save_interval: How often to save progress
        
    Returns:
        DataFrame with processed sales data
    """
    all_sales = []
    processed_count = 0
    
    # Create progress bar
    progress_bar = tqdm(total=total_records, desc="Collecting NFT sales data")

    # Load previous progress if available
    temp_file = "temp_nft_transactions.csv"
    if os.path.exists(temp_file):
        print(f"Found temp file with previous progress")
        temp_df = pd.read_csv(temp_file)
        all_sales = temp_df.to_dict('records')
        processed_count = len(all_sales)
        progress_bar.update(processed_count)
        print(f"Loaded {processed_count} previously collected records")

    # Fetch data in batches
    for skip in range(processed_count, total_records, batch_size):
        # Get a batch of data
        batch = fetch_nft_sales(batch_size, skip)
        
        if not batch:
            print(f"No more data available or query failed at skip={skip}")
            break
        
        # Process each sale in the batch
        new_records = 0
        for sale in batch:
            processed_sale = parse_sale_data(sale)
            all_sales.append(processed_sale)
            new_records += 1
        
        processed_count += new_records
        progress_bar.update(new_records)
        
        # Save progress periodically
        if processed_count % save_interval == 0 or processed_count >= total_records:
            temp_sales_df = pd.DataFrame(all_sales)
            temp_sales_df.to_csv(temp_file, index=False)
            print(f"\nSaved {processed_count} records to {temp_file}")
        
        # Short pause between batches
        time.sleep(1)
    
    progress_bar.close()
    
    # Convert to DataFrame
    sales_df = pd.DataFrame(all_sales)
    return sales_df


def analyze_data(sales_df):
    """
    Analyze NFT sales data and generate statistics
    Args:
        sales_df: DataFrame with sales data
    Returns:
        Tuple of key statistics
    """
    if sales_df.empty:
        print("No data available for analysis")
        return
    
    print("\n--- NFT Sales Analysis ---")
    
    # Basic statistics
    print(f"Total transactions: {len(sales_df)}")
    print(f"Date range: {sales_df['date'].min()} to {sales_df['date'].max()}")
    print(f"Unique buyers: {sales_df['buyer'].nunique()}")
    print(f"Unique sellers: {sales_df['seller'].nunique()}")
    
    # Time analysis
    hourly_stats = sales_df.groupby("hour").size()
    print("\nTransactions by hour:")
    print(hourly_stats)
    
    weekday_stats = sales_df.groupby("day_name").size()
    print("\nTransactions by day of week:")
    print(weekday_stats)
    
    # Address analysis
    top_buyers = sales_df["buyer"].value_counts().head(10)
    print("\nTop 10 most active buyers:")
    print(top_buyers)
    
    top_sellers = sales_df["seller"].value_counts().head(10)
    print("\nTop 10 most active sellers:")
    print(top_sellers)
    
    # Create visualizations
    plt.figure(figsize=(15, 12))
    
    # Hourly distribution
    plt.subplot(2, 2, 1)
    sales_df.groupby("hour").size().plot(kind="bar")
    plt.title("NFT Sales by Hour of Day")
    plt.xlabel("Hour")
    plt.ylabel("Number of Sales")
    
    # Day of week distribution
    plt.subplot(2, 2, 2)
    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    weekday_counts = sales_df.groupby("day_name").size().reindex(day_order)
    weekday_counts.plot(kind="bar")
    plt.title("NFT Sales by Day of Week")
    plt.xticks(rotation=45)
    plt.ylabel("Number of Sales")
    
    # Daily trend
    plt.subplot(2, 2, 3)
    sales_df["date"] = pd.to_datetime(sales_df["date"])
    daily_counts = sales_df.groupby(sales_df["date"].dt.date).size()
    daily_counts.plot()
    plt.title("Daily NFT Sales Volume")
    plt.xlabel("Date")
    plt.ylabel("Number of Sales")
    
    # Buyer/seller distribution
    plt.subplot(2, 2, 4)
    transactions_per_address = pd.DataFrame({
        "Buyers": sales_df["buyer"].value_counts().value_counts(),
        "Sellers": sales_df["seller"].value_counts().value_counts()
    })
    transactions_per_address.plot(kind="bar")
    plt.title("Transactions per Address")
    plt.xlabel("Number of Transactions")
    plt.ylabel("Number of Addresses")
    plt.yscale("log")
    
    plt.tight_layout()
    plt.savefig("nft_sales_analysis.png")
    
    return (hourly_stats, weekday_stats, top_buyers, top_sellers)


def prepare_data_for_llm(sales_df, sample_size=1000):
    """
    Prepare data for analysis with a Large Language Model
    
    Args:
        sales_df: DataFrame with sales data
        sample_size: Number of records to include in the sample
    """
    # Sort by timestamp (most recent first)
    sales_df = sales_df.sort_values("timestamp", ascending=False)
    
    # Add transaction index
    sales_df["transaction_index"] = range(1, len(sales_df) + 1)
    
    # Create a representative sample
    if len(sales_df) > sample_size:
        # Get half most recent, half random from the rest
        recent_count = sample_size // 2
        recent_df = sales_df.head(recent_count)
        older_df = sales_df.iloc[recent_count:].sample(sample_size - recent_count)
        llm_sales = pd.concat([recent_df, older_df])
    else:
        llm_sales = sales_df
    
    # Select relevant columns
    llm_sales = llm_sales[[
        'transaction_index', 'transaction_hash', 'seller', 'buyer',
        'date', 'time', 'day_name', 'hour', 'is_weekend'
    ]]
    
    # Save transaction sample
    llm_sales.to_csv("llm_nft_transactions_sample.csv", index=False)
    print(f"Saved {len(llm_sales)} sample transactions for LLM analysis")
    
    # Create and save summary statistics
    # Hourly distribution
    hourly_stats = sales_df.groupby("hour").size().reset_index(name="transactions")
    hourly_stats.to_csv("llm_hourly_stats.csv", index=False)
    
    # Weekday distribution
    weekday_stats = sales_df.groupby(["day_of_week", "day_name"]).size().reset_index(name="transactions")
    weekday_stats = weekday_stats.sort_values("day_of_week")
    weekday_stats.to_csv("llm_weekday_stats.csv", index=False)
    
    # Top buyers
    buyer_stats = sales_df.groupby("buyer").size().reset_index(name="transactions")
    buyer_stats = buyer_stats.sort_values("transactions", ascending=False)
    buyer_stats.head(100).to_csv("llm_top_buyers.csv", index=False)
    
    # Top sellers
    seller_stats = sales_df.groupby("seller").size().reset_index(name="transactions")
    seller_stats = seller_stats.sort_values("transactions", ascending=False)
    seller_stats.head(100).to_csv("llm_top_sellers.csv", index=False)
    
    # Daily transaction volume
    date_stats = sales_df.groupby("date").size().reset_index(name="transactions")
    date_stats["date"] = pd.to_datetime(date_stats["date"])
    date_stats = date_stats.sort_values("date")
    date_stats.to_csv("llm_daily_transactions.csv", index=False)


def main():

    
    # Collect sales data
    print("\nStep 1: Collecting NFT sales data from The Graph")
    sales_df = collect_and_process_nft_sales(total_records=5000, batch_size=100, save_interval=500)
    
    # Save complete dataset
    sales_df.to_csv("nft_transactions.csv", index=False)
    print(f"Saved {len(sales_df)} NFT sales records to nft_transactions.csv")
    
    # Analyze the data
    print("\nStep 2: Analyzing NFT sales patterns")
    analyze_data(sales_df)
    
    # Prepare data for LLM analysis
    print("\nStep 3: Preparing data for LLM analysis")
    prepare_data_for_llm(sales_df, sample_size=1000)
    
    
    # Clean up temp file
    if os.path.exists("temp_nft_transactions.csv"):
        os.remove("temp_nft_transactions.csv")
        print("Removed temporary file")


if __name__ == "__main__":
    main()