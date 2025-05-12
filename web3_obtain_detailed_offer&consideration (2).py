from web3 import Web3
import json
import pandas as pd
import requests
from datetime import datetime



# Popular NFT collection addresses and their names(coulc be defined by ourselves)
NFT_COLLECTIONS = {}


def get_collection_name(token_address):
    """Look up the human-readable name for an NFT collection contract address"""
    address_lower = token_address.lower()
    
    # Check our local collection dictionary first
    if address_lower in NFT_COLLECTIONS:
        return NFT_COLLECTIONS[address_lower]
    
    # Try to get the contract name from Etherscan
    api_key = "WVHKCRZKBCJY35AT4QP7Y9386B18Q864QD"  # Your Etherscan API key
    url = f"https://api.etherscan.io/api?module=contract&action=getsourcecode&address={token_address}&apikey={api_key}"
    
    response = requests.get(url, timeout=5)
    data = response.json()
    
    
    # If we couldn't identify the collection, return the address
    return token_address


def get_item_type_name(item_type):
    """Convert numeric item type to readable name"""
    item_types = {
        0: "NATIVE",      # ETH
        1: "ERC20",       # Fungible token
        2: "ERC721",      # Non-fungible token (NFT)
    }
    return item_types.get(item_type, f"UNKNOWN ({item_type})")


def decode_transaction(tx_hash, w3, seaport_contract):
    """Extract NFT transaction details from an Ethereum transaction"""
    # Get basic transaction data
    tx = w3.eth.get_transaction(tx_hash)
    receipt = w3.eth.get_transaction_receipt(tx_hash)
    block = w3.eth.get_block(receipt.blockNumber)

    # Basic transaction info
    transaction_info = {
        "hash": tx_hash,
        "from": tx["from"],
        "to": tx.to,
        "block_number": receipt.blockNumber,
        "timestamp": block.timestamp,
        "datetime": datetime.fromtimestamp(block.timestamp).strftime('%Y-%m-%d %H:%M:%S'),
        "gas_used": receipt.gasUsed,
        "gas_price": w3.from_wei(tx.gasPrice, 'gwei'),
        "tx_fee_eth": w3.from_wei(tx.gasPrice * receipt.gasUsed, 'ether'),
    }

    # Look for NFT trade events (OrderFulfilled) in etherum 
    order_fulfilled_events = []
    for log in receipt.logs:
        if log.address.lower() == seaport_contract.address.lower():
            try:
                parsed_log = seaport_contract.events.OrderFulfilled().process_log(log)
                order_fulfilled_events.append(parsed_log)
            except:
                # Not an OrderFulfilled event
                pass

    # Process the NFT trade event
    event_data = order_fulfilled_events[0].args
    transaction_info["has_order_fulfilled"] = True

    # Add trade participants from NFT
    transaction_info.update({
        "order_hash": event_data.orderHash.hex(),
        "offerer": event_data.offerer,        # seller
        "zone": event_data.zone,
        "recipient": event_data.recipient,    # buyer
    })

    # Process what the seller offered (typically NFTs)
    nft_items = []
    for item in event_data.offer:
        item_type = item.itemType
        item_info = {
            "item_type": item_type,
            "item_type_name": get_item_type_name(item_type),
            "token_address": item.token,
            "token_id": item.identifier,
            "amount": item.amount
        }

        # Add extra info for NFT items
        if item_type in [2, 3, 4, 5]:  # ERC721 or ERC1155 types
            item_info["is_nft"] = True
            item_info["collection_name"] = get_collection_name(item.token)
            item_info["opensea_link"] = f"https://opensea.io/assets/{item.token}/{item.identifier}"
        else:
            item_info["is_nft"] = False

        nft_items.append(item_info)

    # Process what the buyer paid (typically ETH)
    payment_items = []
    total_eth_value = 0

    for item in event_data.consideration:
        item_type = item.itemType
        item_info = {
            "item_type": item_type,
            "item_type_name": get_item_type_name(item_type),
            "token_address": item.token,
            "token_id": item.identifier,
            "amount": item.amount,
            "recipient": item.recipient
        }

        # Calculate ETH value for native or ERC20 tokens
        if item_type in [0, 1]:  # NATIVE or ERC20
            amount_eth = float(w3.from_wei(item.amount, 'ether'))
            item_info["amount_eth"] = amount_eth
            total_eth_value += amount_eth

        payment_items.append(item_info)

    # Add price information
    transaction_info["total_price_eth"] = total_eth_value

    # Add detailed item info
    transaction_info["nft_items"] = nft_items
    transaction_info["payment_items"] = payment_items

    return transaction_info


def display_transaction_details(transaction_info):
    """Print transaction details in a human-readable format"""
    print("\nTransaction Details:")
    print("="*60)
    print(f"Transaction Hash: {transaction_info['hash']}")
    print(f"Block Number: {transaction_info['block_number']}")
    print(f"Time: {transaction_info['datetime']}")
    print(f"From: {transaction_info['from']}")
    print(f"To: {transaction_info['to']}")
    print(f"Gas Used: {transaction_info['gas_used']}")
    print(f"Gas Price: {transaction_info['gas_price']} Gwei")
    print(f"Transaction Fee: {transaction_info['tx_fee_eth']} ETH")

    # Display NFT trade details
    print("\nNFT Trade Details:")
    print("-"*60)
    print(f"Seller: {transaction_info['offerer']}")
    print(f"Buyer: {transaction_info['recipient']}")

    # Show price
    if "total_price_eth" in transaction_info:
        print(f"\nTrade Price: {transaction_info['total_price_eth']} ETH")



def process_transaction_batch(tx_hashes, output_file=None):
    """Process a list of transaction hashes to analyze NFT trades"""
    # Connect to Ethereum
    INFURA_KEY = "dfea7c97a1254b9d8e742c8d212e5ca1"  # Replace with your API key
    w3 = Web3(Web3.HTTPProvider(f"https://mainnet.infura.io/v3/{INFURA_KEY}"))
        
    print(f"Connected to Ethereum network (chainId: {w3.eth.chain_id})")
    
    # Initialize Seaport contract
    seaport_address = "0x00000000000000ADc04C56Bf30aC9d3c0aAF14dC"
    seaport_contract = w3.eth.contract(address=seaport_address, abi=json.loads(SEAPORT_ABI))
    
    # Process each transaction
    all_transactions = []
    for i, tx_hash in enumerate(tx_hashes, 1):
        print(f"[{i}/{len(tx_hashes)}] Processing transaction: {tx_hash}")
        
        # Decode transaction
        tx_info = decode_transaction(tx_hash, w3, seaport_contract)
        all_transactions.append(tx_info)
        
        # Show results
        display_transaction_details(tx_info)
    
    # Extract key data for DataFrame
    simplified_data = []
    for tx in all_transactions:
        record = {
            "hash": tx.get("hash", ""),
            "block_number": tx.get("block_number", ""),
            "timestamp": tx.get("timestamp", ""),
            "datetime": tx.get("datetime", ""),
            "from_address": tx.get("from", ""),
            "to_address": tx.get("to", ""),
            "gas_used": tx.get("gas_used", ""),
            "gas_price_gwei": tx.get("gas_price", ""),
            "tx_fee_eth": tx.get("tx_fee_eth", ""),
            "has_nft_trade": tx.get("has_order_fulfilled", False),
            "seller": tx.get("offerer", ""),
            "buyer": tx.get("recipient", ""),
            "price_eth": tx.get("total_price_eth", ""),
            "nft_token_address": tx.get("nft_token_address", ""),
            "nft_token_id": tx.get("nft_token_id", ""),
            "nft_collection": tx.get("nft_collection", ""),
            "nft_type": tx.get("nft_type", ""),
        }
        simplified_data.append(record)
    
    # Create DataFrame
    df = pd.DataFrame(simplified_data)
    
    # Save to CSV if requested
    if output_file:
        df.to_csv(output_file, index=False)
        print(f"Data saved to {output_file}")
    
    return df


def main():
    """Main function to demonstrate NFT transaction analysis"""
    # Example transactions to analyze
    tx_hashes = [
        "0xa9dfe4c8ddd3f263af79278866586a244b534eaf83e9b3e2aa830f3aabbaeebf",
        # Add more transactions here
    ]
    
    # Where to save the results
    output_file = "nft_transactions.csv"
    
    # Process the transactions
    df = process_transaction_batch(tx_hashes, output_file)
    
    # Show statistics
    if df is not None:
        nft_trades = df[df["has_nft_trade"] == True]
        
        print("\nData Analysis:")
        print(f"Total transactions: {len(df)}")
        print(f"NFT trades: {len(nft_trades)}")
        
        if not nft_trades.empty:
            print(f"Average price: {nft_trades['price_eth'].mean():.4f} ETH")
            print(f"Highest price: {nft_trades['price_eth'].max():.4f} ETH")
            
            # Collection statistics
            print("\nCollection Statistics:")
            collection_counts = nft_trades['nft_collection'].value_counts().head(5)
            for collection, count in collection_counts.items():
                print(f"  {collection}: {count} transactions")


if __name__ == "__main__":
    main()