import requests
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.

def create_connection():
    # Redshift connection details
    redshift_config = {
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT'),
    }
    
    # Set up the Redshift connection
    connection = psycopg2.connect(
        host=redshift_config['host'],
        dbname=redshift_config['dbname'],
        user=redshift_config['user'],
        password=redshift_config['password'],
        port=redshift_config['port']
    )

    return connection