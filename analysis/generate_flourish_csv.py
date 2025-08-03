import pandas as pd
from db import get_db_conn
import os


conn = get_db_conn()


views_config = {
    "view_zhaoxuelu_comments_rating_percentage_with_time": {
        "drama_col": "rating",
        "date_col": "insert_time",
        "value_col": "rating_percent"
    },
    "view_cumulative_top20_dramas_per_day": {
        "drama_col": "drama_name",
        "date_col": "comment_day",
        "value_col": "cumulative_count"
    },
    "view_suspense_daily": {
        "drama_col": "name",
        "date_col": "day",
        "value_col": "count_sum"
    }
}

output_dir = "flourish_outputs"
os.makedirs(output_dir, exist_ok=True)

for view_name, cols in views_config.items():
    print(f"🔄 Processing Views：{view_name}")

    try:
        query = f"""
        SELECT
            {cols['drama_col']} AS drama,
            {cols['date_col']} AS date,
            {cols['value_col']} AS value
        FROM {view_name}
        """
        df = pd.read_sql(query, conn)

        # Pivot to a wide table: rows are drama_name, columns are comment_day
        wide_df = df.pivot(index="drama", columns="date", values="value")
        # Replace missing values with 0 and sort the date column
        wide_df = wide_df.fillna(0).sort_index(axis=1)
        # Optional: Convert column names to strings (Flourish compatible)
        wide_df.columns = wide_df.columns.astype(str)

        # Save as CSV, with the first column as drama_name
        output_path = os.path.join(output_dir, f"{view_name}.csv")
        wide_df.reset_index().to_csv(output_path, index=False)
        print(f"✅ CSV generated: {output_path}")

    except Exception as e:
        print(f"❌ Processing failed {view_name}: {e}")


conn.close()
print("\n🎉 All views processed successfully!")
