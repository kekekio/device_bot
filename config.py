# config.py
import json
import os

def load_config(config_file="config.json"):
    """Load configuration from JSON file"""
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Config file {config_file} not found. Please create it based on config.json.example")
    
    with open(config_file, 'r') as f:
        config = json.load(f)
    
    return config

# Load configuration
CONFIG = load_config()

# Export configuration variables
HOMESERVER = CONFIG["homeserver"]
USERNAME = CONFIG["username"]
PASSWORD = CONFIG["password"]
COMMAND_PREFIX = CONFIG.get("command_prefix", "!")

DATA_FILE = CONFIG.get("data_file", "devices.json")
SUBSCRIPTIONS_FILE = CONFIG.get("subscriptions_file", "subscriptions.json")
SUBSCRIPTION_ROOMS_FILE = CONFIG.get("subscription_rooms_file", "subscription_rooms.json")
SYNC_TOKEN_FILE = CONFIG.get("sync_token_file", "sync_token.txt")

SYNC_TIMEOUT = CONFIG.get("sync_timeout", 30000)
LOCALPART_DOMAIN = CONFIG.get("localpart_domain", "localhost")
