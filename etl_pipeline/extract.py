import os
import json
import logging
from dotenv import load_dotenv
from client import EcomAPIClient

BASE_URL = "http://localhost:8000/api/v1" 
ENDPOINTS = [
    "products",
    "customers",
    "sellers", 
    "orders",            
    "orders/reviews/",   
    "orders/payments/",  
    "orders/items/"      
]

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_credentials():
    load_dotenv()
    return os.getenv("USER_NAME"), os.getenv("PASSWORD")

def save_to_file(endpoint_name, data):
    directory = "data/raw"
    os.makedirs(directory, exist_ok=True)
    
    clean_name = endpoint_name.strip("/")
    safe_filename = clean_name.replace("/", "_")
    
    # Result: "orders/reviews/" -> "orders_reviews.json"
    filepath = f"{directory}/{safe_filename}.json"
    
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f"   -> Saved to {filepath}")
    except IOError as e:
        logger.error(f"   -> Failed to save file: {e}")

def main():
    try:
        user, password = load_credentials()
        client = EcomAPIClient(BASE_URL, user, password)
        logger.info(f"--- Starting Extraction from {BASE_URL} ---")

        for endpoint in ENDPOINTS:
            logger.info(f"\n[+] Fetching: {endpoint}...")
            
            response = client.fetch_data(endpoint)

            if response.status_code == 200:
                json_data = response.json()
                
                # Handle "data" wrapper if present
                count = 0
                if isinstance(json_data, dict) and 'data' in json_data:
                    count = len(json_data['data'])
                elif isinstance(json_data, list):
                    count = len(json_data)
                
                logger.info(f"Status: 200 OK | Records Found: {count}")
                save_to_file(endpoint, json_data)
            
            else:
                logger.warning(f"    Error: Status {response.status_code}")
                logger.warning(f"    Message: {response.text[:100]}")
                if response.status_code == 404:
                    logger.warning("(Hint: Check trailing slashes in the ENDPOINTS list)")

    except Exception as e:
        logger.critical(f"Unexpected Error: {e}")

if __name__ == "__main__":
    main()