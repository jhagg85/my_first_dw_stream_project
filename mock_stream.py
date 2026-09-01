# File: /Users/josephalbertgarganera/m1_lakehouse/my_first_dw_stream_project/mock_stream.py
import time
import json
import uuid
import random
import os
from datetime import datetime
from azure.storage.filedatalake import DataLakeServiceClient


# 🚨 ENTERPRISE SECURITY CONTEXT - UPDATE WITH YOUR AZURE SETTINGS
ACCOUNT_NAME = "stenterprisedwstream"  # Your exact Azure storage account name
ACCOUNT_KEY = os.getenv("AZURE_STORAGE_KEY")  # Found under "Access keys" in your Azure Portal storage view
FILE_SYSTEM = "landing" # The target container we built in Milestone 1

def initialize_lake_client(account_name, account_key):
    """Programmatically establishes an authenticated secure socket link into Azure ADLS Gen2."""
    account_url = f"https://{account_name}.dfs.core.windows.net"
    return DataLakeServiceClient(account_url=account_url, credential=account_key)

def generate_transaction_payload():
    """Simulates an enterprise application event stream containing data anomalies, duplicates, and multiple currencies."""
    currencies = ["USD", "EUR", "PHP"]
    # Introduce an occasional duplicate ID to simulate internet stream retries for the Silver layer to fix
    tx_id = "FIXED_DUP_77777" if random.random() < 0.05 else str(uuid.uuid4())
    
    return {
        "transaction_id": tx_id,
        "customer_id": f"CUST_{random.randint(1001, 1010)}", # Generates a narrow pool of 10 recurring customers
        "amount": round(random.uniform(5.0, 1500.0), 2),
        "currency": random.choice(currencies),
        "event_time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    }

def main():
    print("=" * 60)
    print("ENTERPRISE REAL-TIME TRANSACTION STREAM SIMULATOR ACTIVE")
    print(f"Target Destination: Azure ADLS Gen2 // {ACCOUNT_NAME}/{FILE_SYSTEM}/transactions/")
    print("Press Ctrl+C to terminate the stream safely.")
    print("=" * 60)
    
    try:
        service_client = initialize_lake_client(ACCOUNT_NAME, ACCOUNT_KEY)
        file_system_client = service_client.get_file_system_client(file_system=FILE_SYSTEM)
        
        while True:
            # 1. Construct payload
            payload = generate_transaction_payload()
            json_data = json.dumps(payload)
            
            # 2. Assign unique streaming file paths inside the cloud directory tree
            unique_timestamp = int(time.time_ns())
            file_name = f"transactions/tx_log_{unique_timestamp}.json"
            
            # 3. Stream payload directly up to the Azure cloud landing layer
            file_client = file_system_client.get_file_client(file_name)
            file_client.upload_data(json_data, overwrite=True)
            
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Streamed ➔ {payload['transaction_id']} | {payload['currency']} {payload['amount']}")
            
            # 4. Throttling throttle: pause for 2 seconds to safeguard your free tier resource consumption limits
            time.sleep(2)
            
    except KeyboardInterrupt:
        print("\n[INFO] Streaming script gracefully halted by user operator.")
    except Exception as e:
        print(f"\n[CRITICAL ERROR] Stream network failure: {str(e)}")

if __name__ == "__main__":
    main()

