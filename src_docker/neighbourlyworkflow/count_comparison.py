import os
import sys
import pymysql
from google.cloud import bigquery
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder
import paramiko
import socket
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common.bigquery.bigquery import create_bigquery_client
from common.logging.logger import logger, log_start,  log_end

# Load environment variables
load_dotenv()

# Configure logger
logger = logger(
    "count_comparison",
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "log"),
)

# Global variables
project_id = os.environ.get("BIGQUERY_PROJECT_ID")
dataset_id = os.environ.get("BIGQUERY_DATASET_ID")
base64_credentials = os.environ.get("GOOGLE_CLOUD_CRED_BASE64")

# SSH jump host details
jump_host = os.environ.get("NBLY_JUMP_HOST")
jump_port = int(os.environ.get("NBLY_JUMP_PORT", 22))
jump_username = os.environ.get("NBLY_JUMP_USER")
PRIVATE_KEY_PATH = "/Users/amber.wang/.ssh/id_rsa"

# MySQL server details
mysql_host = os.environ.get("NBLY_MYSQL_HOST")
mysql_port = int(os.environ.get("NBLY_MYSQL_PORT", 3306))
mysql_user = os.environ.get("NBLY_MYSQL_USER")
mysql_password = os.environ.get("NBLY_MYSQL_USER_PW")
mysql_db = os.environ.get("NBLY_DB_NAME")

# Define the list of tables to be compared
tables_to_compare = [
    'paf_address',
    'notification_preference',
    'user_paf_address',
    'user',
    'neighbourhood'
]

def find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        return s.getsockname()[1]

def establish_ssh_tunnel(jump_host, jump_port, jump_username, ssh_pkey, mysql_host, mysql_port):
    local_port = find_free_port()
    max_attempts = 5
    for attempt in range(max_attempts):
        try:
            tunnel = SSHTunnelForwarder(
                (jump_host, jump_port),
                ssh_username=jump_username,
                ssh_pkey=ssh_pkey,
                remote_bind_address=(mysql_host, mysql_port),
                local_bind_address=("127.0.0.1", local_port),
            )
            tunnel.start()
            logger.info(f"SSH tunnel established to jump host: {tunnel.local_bind_address}")
            return tunnel, local_port
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
            if attempt == max_attempts - 1:
                raise
            local_port = find_free_port()
    
    raise Exception("Failed to establish SSH tunnel after multiple attempts")

def compare_table_counts(mysql_connection, bigquery_client, tables, project_id, dataset_id):
    mysql_cursor = mysql_connection.cursor()
    comparison_results = []

    for table_name in tables:
        # Get MySQL count
        mysql_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        mysql_count = mysql_cursor.fetchone()[0]

        # Get BigQuery count
        bq_table_name = f"neighbourly_{table_name}"
        full_table_id = f"{project_id}.{dataset_id}.{bq_table_name}"
        query = f"SELECT COUNT(*) as count FROM `{full_table_id}`"
        query_job = bigquery_client.query(query)
        results = query_job.result()
        bigquery_count = next(results).count

        comparison_results.append({
            'table_name': table_name,
            'mysql_count': mysql_count,
            'bigquery_count': bigquery_count,
            'difference': mysql_count - bigquery_count
        })

    return comparison_results

def log_comparison_results(comparison_results):
    logger.info("Table Count Comparison Results:")
    for result in comparison_results:
        logger.info(f"Table: {result['table_name']}")
        logger.info(f"  MySQL count: {result['mysql_count']}")
        logger.info(f"  BigQuery count: {result['bigquery_count']}")
        logger.info(f"  Difference: {result['difference']}")
        logger.info("-----------------------------")

def main():
    log_start(logger)
    bigquery_client = None
    ssh_tunnel = None
    mysql_connection = None

    try:
        # Initialize BigQuery client
        bigquery_client = create_bigquery_client(base64_credentials)
        if not bigquery_client:
            logger.error("Failed to create BigQuery client.")
            sys.exit(1)

        # Check if PRIVATE_KEY_PATH is set
        if not PRIVATE_KEY_PATH:
            logger.error("Private Key Path is not set. Please check your .env file.")
            sys.exit(1)

        # Load the private key
        try:
            ssh_pkey = paramiko.RSAKey(filename=PRIVATE_KEY_PATH)
            logger.info("Private key loaded successfully.")
        except Exception as e:
            logger.error(f"Failed to load the private key: {e}")
            sys.exit(1)

        # Establish SSH tunnel
        logger.info(f"Attempting to establish SSH tunnel to {jump_host}:{jump_port}")
        try:
            ssh_tunnel, local_port = establish_ssh_tunnel(
                jump_host, jump_port, jump_username, ssh_pkey, mysql_host, mysql_port
            )
        except Exception as e:
            logger.error(f"Failed to establish SSH tunnel: {str(e)}")
            sys.exit(1)

        # Establish MySQL connection
        mysql_connection = pymysql.connect(
            host="127.0.0.1",
            user=mysql_user,
            password=mysql_password,
            database=mysql_db,
            port=local_port,
            connect_timeout=10,
        )
        logger.info("Connected to MySQL database")

        # Perform table count comparison
        comparison_results = compare_table_counts(mysql_connection, bigquery_client, tables_to_compare, project_id, dataset_id)
        log_comparison_results(comparison_results)

    except Exception as e:
        logger.error(f"An unexpected error occurred in the main function: {e}")
    finally:
        # Clean up resources
        if mysql_connection:
            mysql_connection.close()
            logger.info("MySQL connection closed.")
        if ssh_tunnel:
            ssh_tunnel.stop()
            logger.info("SSH tunnel closed.")
        if bigquery_client:
            bigquery_client.close()
            logger.info("BigQuery client closed.")
            log_end(logger)

if __name__ == "__main__":
    main()