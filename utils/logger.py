import logging
import os

def setup_logger(log_file='logs/scraper.log', level=logging.INFO):
    """Configure the global logging settings."""
    os.makedirs(os.path.dirname(log_file), exist_ok=True) 
    print(f"📝 Logging to {os.path.abspath(log_file)}")
    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(levelname)s] %(message)s',
        encoding='utf-8',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
 