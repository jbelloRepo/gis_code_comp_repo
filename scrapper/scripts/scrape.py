import time
from arcgis_utils import fetch_all_datasets
from db_utils import store_data_in_db

FETCH_INTERVAL = 3600  # Time interval in seconds (e.g., 1 hour)

def run_scraper():
    while True:
        print("Fetching all GIS datasets...")
        all_data = fetch_all_datasets()
        if all_data:
            print("Fetched all data successfully. Storing in database...")
            for city, datasets in all_data.items():
                for dataset_type, data in datasets.items():
                    store_data_in_db(city, dataset_type, data)
            print("Data stored successfully.")
        else:
            print("Failed to fetch data or no data available.")
        
        print(f"Sleeping for {FETCH_INTERVAL} seconds...")
        time.sleep(FETCH_INTERVAL)

if __name__ == "__main__":
    run_scraper()
