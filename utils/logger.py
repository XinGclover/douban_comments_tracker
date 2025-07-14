import logging
import os

''' def setup_logger(log_file='logs/scraper.log', level=logging.INFO):
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
    ) '''



def setup_logger(log_file, level=logging.INFO):
    """Set log output to files and console to avoid the limitations of basicConfig"""
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    logger = logging.getLogger()  # Get root logger
    logger.setLevel(level)

    # Avoid adding FileHandler repeatedly (especially when called multiple times)
    if not any(isinstance(h, logging.FileHandler) and h.baseFilename == os.path.abspath(log_file) for h in logger.handlers):
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
        logger.addHandler(file_handler)

    # Avoid adding StreamHandler repeatedly
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
        logger.addHandler(stream_handler)

    return logger

