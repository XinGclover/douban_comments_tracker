import os

APP_NAME = "douban_console"

# Whitelist of allowed views (to prevent injection attacks)
ALLOWED_VIEWS = [
    "v_posts_ai",
    "v_user_stats",
    "v_low_rating_batches",
    "v_topic_summary",
]

DEFAULT_LIMIT = 200
MAX_LIMIT = 2000

# Optional: Used in the UI to display the current connection target (without password)
DB_TARGET = f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"