import os
import psycopg2
from dotenv import load_dotenv

load_dotenv() 

def get_db_conn():
    """
    Get PostgreSQL database connection
    :return: psycopg2 connection object
    """
    return psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT')
    )
