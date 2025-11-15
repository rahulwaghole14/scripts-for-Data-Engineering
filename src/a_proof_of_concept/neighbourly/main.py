"""
connect to neighbourly db through ssh tunnel
"""

import os
import sys
import logging
import paramiko
import pymysql
from sshtunnel import SSHTunnelForwarder


# SSH jump host details
jump_host = os.environ.get("NBLY_JUMP_HOST")
jump_port = os.environ.get("NBLY_JUMP_PORT")
jump_username = os.environ.get("NBLY_JUMP_USER")
PRIVATE_KEY_PATH = "./keyfile.pem"

# MySQL server details
mysql_host = os.environ.get("NBLY_MYSQL_HOST")
mysql_port = os.environ.get("NBLY_MYSQL_PORT")
mysql_user = os.environ.get("NBLY_MYSQL_USER")
mysql_password = os.environ.get("NBLY_MYSQL_USER_PW")
mysql_db = os.environ.get("NBLY_DB_NAME")

try:
    # Load the Ed25519 private key
    ssh_pkey = paramiko.RSAKey(filename=PRIVATE_KEY_PATH, password=None)
    # ssh_pkey = paramiko.Ed25519Key(filename=PRIVATE_KEY_PATH, password=None)
    logging.info("Private key loaded successfully.")
except paramiko.SSHException as e:
    logging.error("Failed to load the Ed25519 key: %s", e)
    sys.exit(1)

local_bind_address = ("127.0.0.1", 3307)  # Local port forwarding to 3307

# Create an SSH tunnel
try:
    with SSHTunnelForwarder(
        (jump_host, 22),
        ssh_username=jump_username,
        ssh_pkey=ssh_pkey,
        remote_bind_address=(mysql_host, mysql_port),
        local_bind_address=local_bind_address,  # Local port forwarding to 3307
    ) as tunnel:

        print(
            "SSH tunnel established to jump host:", tunnel.local_bind_address
        )

        # Connect to the MySQL database through the SSH tunnel
        try:
            connection = pymysql.connect(
                host=local_bind_address[0],
                user=mysql_user,
                password=mysql_password,
                database=mysql_db,
                port=local_bind_address[1],
                connect_timeout=10,  # debug
            )
            print("Connected to MySQL database.")

            try:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT * from paf_address limit 10;")
                    result = cursor.fetchone()
                    print("Connected to database: %s", result)
            except pymysql.MySQLError as e:
                print("Failed to execute query: %s", e)
            finally:
                connection.close()
                print("MySQL connection closed.")
        except pymysql.MySQLError as e:
            print("Failed to connect to MySQL database: %s", e)
except Exception as e:
    print("Failed to establish SSH tunnel or connect to MySQL: %s", e)
