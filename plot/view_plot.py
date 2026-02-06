import pandas as pd
import matplotlib.pyplot as plt
from db import get_db_conn

conn = get_db_conn()

df = pd.read_sql("SELECT * FROM view_zhaoxuelu_heat_iqiyi_with_shanghai_time", conn)

plt.plot(df['insert_time_shanghai'], df['heat_info'])
plt.xticks(rotation=45)
plt.title("Heat Info Over Time")
plt.show()
