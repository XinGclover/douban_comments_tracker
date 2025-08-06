import pandas as pd
from db import get_db_conn
import os


conn = get_db_conn()


views_config = {
    "view_zhaoxuelu_comments_rating_percentage_daily": {
        "label_col": "rating",
        "date_col": "comment_date",
        "value_col": "cumulative_percent"
    },
     "view_continuous_top20_dramas_per_day": {
        "label_col": "drama_name",
        "date_col": "comment_day",
        "value_col": "cumulative_count"
    },
    "view_cumulative_group_topic_per_day": {
        "label_col": "group_name",
        "date_col": "topic_day",
        "value_col": "cumulative_topic_count"
    }
}

output_dir = "flourish_outputs"
os.makedirs(output_dir, exist_ok=True)

for view_name, cols in views_config.items():
    print(f"🔄 Processing Views：{view_name}")

    try:
        query = f"""
        SELECT
            {cols['label_col']} AS label,
            {cols['date_col']} AS date,
            {cols['value_col']} AS value
        FROM {view_name}
        """
        df = pd.read_sql(query, conn)

        # Pivot to a wide table: rows are label, columns are date
        wide_df = df.pivot(index="label", columns="date", values="value")

        missing = wide_df.isna().sum().sort_values(ascending=False)

        # Replace missing values with 0 and sort the date column
        wide_df = wide_df.fillna(0).sort_index(axis=1)
        # Optional: Convert column names to strings (Flourish compatible)
        wide_df.columns = wide_df.columns.astype(str)

        # Save as CSV, with the first column as label
        output_path = os.path.join(output_dir, f"{view_name}.csv")
        wide_df.reset_index().to_csv(output_path, index=False)
        print(f"✅ CSV generated: {output_path}")

    except Exception as e:
        print(f"❌ Processing failed {view_name}: {e}")


conn.close()
print("\n🎉 All views processed successfully!")
