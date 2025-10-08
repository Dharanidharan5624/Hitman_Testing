import mysql.connector
import configparser
import os
import sys
import traceback

# Add the script's parent directory to sys.path for module imports
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

try:
    from HE_error_logs import log_error_to_db
except ImportError:
    def log_error_to_db(file_name, error_description, created_by="system", env="dev"):
        print(f"[ERROR LOGGER FAILED] {error_description}")

_config = None

def load_config():
    global _config
    if _config:
        return _config

    # Dynamically get the path to config.ini relative to the script's directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Navigate up one level from \script to \powerbuilder, then to \config\config.ini
    config_path = os.path.join(script_dir, "..", "config", "config.ini")
    config_path = os.path.abspath(config_path)  # Resolve to absolute path

    if not os.path.exists(config_path):
        msg = f"Config file not found: {config_path}"
        print(msg)
        log_error_to_db("HE_database_connect.py", msg)
        sys.exit(1)

    config = configparser.ConfigParser()
    config.read(config_path)

    if 'database' not in config:
        msg = "Missing [database] section in config.ini"
        print(msg)
        log_error_to_db("HE_database_connect.py", msg)
        sys.exit(1)

    _config = config
    return config

def get_connection(env='dev'):
    config = load_config()
    db = config['database']

    env_key = {
        'dev': 'HE_DB_DEV',
        'test': 'HE_DB_TEST',
        'prod': 'HE_DB_PROD'
    }.get(env)

    if not env_key or env_key not in db:
        msg = f"Invalid environment: {env}"
        print(msg)
        log_error_to_db("HE_database_connect.py", msg)
        sys.exit(1)

    try:
        conn = mysql.connector.connect(
            host=db['HE_HOSTNAME'],
            port=int(db['HE_PORT']),
            user=db['HE_DB_USERNAME'],
            password=db['HE_DB_PASSWORD'],
            database=db[env_key]
        )
        print(f"[INFO] Connected to {env} database successfully.")
        return conn

    except mysql.connector.Error as err:
        trace = traceback.format_exc()
        print(f"[ERROR] DB Connection failed: {err}")
        log_error_to_db("HE_database_connect.py", trace, created_by="DB_CONNECT", env=env)
        sys.exit(1)

def main():
    # Automatically attempt to connect to the database (default to 'dev' environment)
    conn = get_connection(env='dev')
    try:
        # Example: Perform a simple query to verify connection
        cursor = conn.cursor()
        cursor.execute("SELECT DATABASE()")
        db_name = cursor.fetchone()[0]
        print(f"[INFO] Currently connected to database: {db_name}")
        cursor.close()
    except mysql.connector.Error as err:
        trace = traceback.format_exc()
        print(f"[ERROR] Query failed: {err}")
        log_error_to_db("HE_database_connect.py", trace, created_by="DB_CONNECT", env='dev')
    finally:
        conn.close()
        print("[INFO] Database connection closed.")

if __name__ == "__main__":
    main()