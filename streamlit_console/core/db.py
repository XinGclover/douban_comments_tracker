import pandas as pd
from db import get_db_conn

def select_df(sql: str, params: tuple = ()) -> pd.DataFrame:
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d.name if hasattr(d, "name") else d[0] for d in cur.description]
            rows = cur.fetchall()
        return pd.DataFrame(rows, columns=cols)
    finally:
        conn.close()

def test_connection() -> tuple[bool, str]:
    try:
        conn = get_db_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
        conn.close()
        return True, "OK"
    except Exception as e:
        return False, repr(e)
