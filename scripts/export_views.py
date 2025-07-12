import logging
import os
from datetime import datetime

import pandas as pd
import psycopg2

from utils.config import VIEW_WHITELIST,TABLE_PREFIX
from utils.logger import setup_logger
from db import get_db_conn

setup_logger("logs/export_views.log", logging.INFO)

SELECT_QUERY = """
    SELECT table_schema, table_name
    FROM information_schema.views
    WHERE table_schema NOT IN ('pg_catalog', 'information_schema');
    """

PROJECT_PATH = os.environ["PROJECT_PATH"]
EXPORT_DIR = os.path.join(PROJECT_PATH, f"export/{TABLE_PREFIX}_view_export")

def get_all_views(cursor):
    cursor.execute(SELECT_QUERY)
    return cursor.fetchall()

def export_views_to_csv():
    os.makedirs(EXPORT_DIR, exist_ok=True)
    conn = get_db_conn()
    cursor = conn.cursor()

    views = VIEW_WHITELIST

    for schema, view in views:
        full_view_name = f'"{schema}"."{view}"'
        try:
            df = pd.read_sql_query(f"SELECT * FROM {full_view_name}", conn)
            filename = f"{view}.csv"
            filepath = os.path.join(EXPORT_DIR, filename)
            df.to_csv(filepath, index=False)
            print(f"✅ Exported view {full_view_name} to {filepath}")
        except pd.errors.DatabaseError as e:
            print(f"⚠️ Skipped {full_view_name}: {e}")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    export_views_to_csv()
