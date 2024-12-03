import psycopg2
import json
import os

def load_db_config():
    """Load database configuration from JSON file."""
    config_path = os.path.join(os.path.dirname(__file__), "db_config.json")
    with open(config_path, "r") as file:
        config = json.load(file)
    return config["database"]

def get_db_connection():
    """Create and return a database connection using configuration from JSON."""
    db_settings = load_db_config()
    return psycopg2.connect(**db_settings)