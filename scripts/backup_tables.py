import csv
import logging
import os
import subprocess
import sys

import psycopg2

from db import get_db_conn
from utils.logger import setup_logger

PROJECT_PATH = os.environ["PROJECT_PATH"]

setup_logger("logs/backup_tables.log", logging.INFO)

SELECT_QUERY = """
    SELECT tablename FROM pg_tables WHERE schemaname = 'public';
    """

def backup_table_csv(cur, table_name, backup_dir):
    """ Back up a single table to a CSV file.
    :param conn: psycopg2 connection object
    :param table_name: str, name of the table to back up
    :param backup_dir: str, directory to save the backup file
    """
    backup_filename = f"{table_name}_backup.csv"
    backup_path = os.path.join(backup_dir, backup_filename)

    with open(backup_path, 'w', newline='', encoding='utf-8') as f:
        cur.execute(f"SELECT * FROM {table_name}")
        writer = csv.writer(f)
        writer.writerow([desc[0] for desc in cur.description])  # Write header row
        for row in cur:
            writer.writerow(row)
    logging.info("✅ CSV backup completed for table `%s`: %s", table_name, backup_path)

def backup_table_pg_dump(db_conf, table_name, backup_dir):
    """ Back up a single table using pg_dump.
    :param db_conf: dict, database configuration parameters
    :param table_name: str, name of the table to back up
    :param backup_dir: str, directory to save the backup file
    """
    backup_path = os.path.join(backup_dir, f"{table_name}_backup.sql")

    cmd = [
        "pg_dump",
        "-U", db_conf.get("user"),
        "-d", db_conf.get("dbname"),
        "-h", db_conf.get("host", "localhost"),
        "-p", str(db_conf.get("port", 5432)),
        "-t", table_name,
        "-f", backup_path
    ]

    try:
        subprocess.run(cmd, check=True, env={**os.environ, "PGPASSWORD": db_conf.get("password")})
        logging.info("✅ SQL dump completed for `%s`: %s", table_name, backup_path)
    except subprocess.CalledProcessError as e:
        logging.error("❌ pg_dump failed for `%s`: %s", table_name, e)


def main():
    backup_dir = os.path.join(PROJECT_PATH, "backup")

    if not os.path.exists(backup_dir):
        os.makedirs(backup_dir)
        logging.info("📁 Created backup directory: %s", backup_dir)

    conn = get_db_conn()
    try:
        db_conf = {
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT")
    }
    except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
        logging.error("❌ Database connection failed: %s", e)
        sys.exit(1)

    with conn.cursor() as cursor:
        cursor.execute(SELECT_QUERY)
        tables = [row[0] for row in cursor.fetchall()]

        for table in tables:
            try:
                backup_table_csv(cursor, table, backup_dir)
                backup_table_pg_dump(db_conf, table, backup_dir)
            except (OSError,RuntimeError, psycopg2.Error) as e:
                logging.error("❌ Database error while backing up table `%s`: %s", table, e)

    conn.close()
    logging.info("🎉 All backups done.")

if __name__ == "__main__":
    main()
