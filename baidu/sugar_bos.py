import os
from baidubce.services.bos.bos_client import BosClient
import requests
import logging


logger = logging.getLogger()
logger.setLevel(logging.INFO)


BOS_AK = os.getenv("BOS_AK")
BOS_SK = os.getenv("BOS_SK")
BUCKET_NAME = os.getenv("BUCKET_NAME", "zhaoxuelu-dashboard-bucket")
SUGAR_API = os.getenv("SUGAR_API")
SUGAR_TOKEN = os.getenv("SUGAR_TOKEN")


CSV_FILES = [
    "view_high_rating_dramas_source.csv",
    "view_weibo_stats.csv",
    "view_zhaoxuelu_comments_count_with_shanghai_time.csv",
    "view_zhaoxuelu_comments_distribution_long.csv",
    "view_zhaoxuelu_comments_rating_percentage.csv",
    "view_zhaoxuelu_comments_rating_percentage_with_time.csv",
    "view_zhaoxuelu_daily_top_words.csv",
    "view_zhaoxuelu_heat_iqiyi_with_shanghai_time.csv",
    "view_zhaoxuelu_iqiyiheat_timeline.csv",
    "view_zhaoxuelu_top_words.csv"
]


def handler(event, context):
    # Initialize BOS client
    bos_client = BosClient(
        access_key_id=BOS_AK,
        secret_access_key=BOS_SK
    )

    results = []

    for file_key in CSV_FILES:
        file_name = os.path.basename(file_key)
        local_path = f"/tmp/{file_name}"

        try:
            # 1. Download CSV file from BOS
            logger.info(f"Processing File: {file_key}")
            bos_client.get_object_to_file(BUCKET_NAME, file_key, local_path)
            logger.info(f"File downloaded successfully: {file_key}")

            # 2. Upload to Sugar
            with open(local_path, "rb") as f:
                files = {"file": (file_name, f)}
                headers = {
                    "Authorization": f"Bearer {SUGAR_TOKEN}",
                    "User-Agent": "BOS-Sugar-Sync/1.0"
                }

                response = requests.post(
                    SUGAR_API,
                    headers=headers,
                    files=files,
                    timeout=30
                )

                # Clean up temporary files
                if os.path.exists(local_path):
                    os.remove(local_path)

                if response.status_code == 200:
                    logger.info(f"Sync successful: {file_key}")
                    results.append({
                        "file": file_key,
                        "status": "success",
                        "message": response.text
                    })
                else:
                    logger.error(f"Sync failed: {file_key}, Status Code: {response.status_code}, Response: {response.text}")
                    results.append({
                        "file": file_key,
                        "status": "error",
                        "message": f"API returned error: {response.status_code} - {response.text}"
                    })

        except Exception as e:
            logger.error(f"Processing file {file_key} failed: {str(e)}", exc_info=True)
            results.append({
                "file": file_key,
                "status": "error",
                "message": str(e)
            })
            continue

    # Return summary results
    return {
        "status": "complete",
        "success_count": len([r for r in results if r["status"] == "success"]),
        "failed_count": len([r for r in results if r["status"] == "error"]),
        "details": results
    }