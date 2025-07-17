import pandas as pd
from db import get_db_conn
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation


sql = """SELECT insert_time, rating, rating_percent
         FROM view_zhaoxuelu_comments_rating_percentage_with_time
         """
def fetch_data():
    conn = get_db_conn()
    df = pd.read_sql_query(sql, conn)
    conn.close()
    # Make sure the time column is of datetime type
    df['insert_time'] = pd.to_datetime(df['insert_time'])
    return df

def prepare_data(df):
    # Group by time and get the rating and rating_percent at each time point
    grouped = df.groupby('insert_time')
    times = sorted(df['insert_time'].unique())
    return times, grouped


def animate_pie_chart():
    df = fetch_data()
    times, grouped = prepare_data(df)

    fig, ax = plt.subplots(figsize=(8,8))

    def update(frame):
        ax.clear()
        current_time = times[frame]
        group = grouped.get_group(current_time)

        labels = group['rating'].astype(str)
        sizes = group['rating_percent']

        ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90)
        ax.set_title(f'Rating distribution\nTime: {current_time.strftime("%Y-%m-%d %H:%M:%S")}', fontsize=14)

    ani = FuncAnimation(fig, update, frames=len(times), interval=1000)

    # Save as mp4 video (need to install ffmpeg)
    ani.save('rating_pie_animation.mp4', writer='ffmpeg')
    print("Animation saved to rating_pie_animation.mp4")

if __name__ == "__main__":
    animate_pie_chart()