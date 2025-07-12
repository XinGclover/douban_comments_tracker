import os

from baidubce.auth.bce_credentials import BceCredentials
from baidubce.bce_client_configuration import BceClientConfiguration
from baidubce.services.bos.bos_client import BosClient
from dotenv import load_dotenv

load_dotenv()

BAIDU_AK = os.getenv("BAIDU_AK")
BAIDU_SK = os.getenv("BAIDU_SK")
BUCKET_NAME = "zhaoxuelu-dashboard-bucket"
BOS_BASE_PATH = 'shujuanyimeng'
LOCAL_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'export', 'shujuanyimeng_view_export'))

config = BceClientConfiguration(
    credentials=BceCredentials(BAIDU_AK, BAIDU_SK),
    endpoint='http://bj.bcebos.com'
)
client = BosClient(config)

def upload_all_csv():
    if not os.path.exists(LOCAL_DIR):
        print(f"❌ The local directory does not exist: {LOCAL_DIR}")
        return

    files = [f for f in os.listdir(LOCAL_DIR) if f.lower().endswith('.csv')]
    if not files:
        print(f"❌ CSV file not found in directory: {LOCAL_DIR}")
        return

    for filename in files:
        local_file_path = os.path.join(LOCAL_DIR, filename)
        bos_file_path = f"{BOS_BASE_PATH}/{filename}"
        try:
            client.put_object_from_file(BUCKET_NAME, bos_file_path, local_file_path)
            print(f"✅ Upload Successfully: {bos_file_path}")
        except Exception as e:
            print(f"❌ Upload failed: {bos_file_path}, error: {e}")

if __name__ == '__main__':
    upload_all_csv()
    print(BAIDU_AK,"BK", BAIDU_SK)