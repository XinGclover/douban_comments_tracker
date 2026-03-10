from pathlib import Path
import re


LOG_DIR = Path("logs")

def extract_task_name(cmd):
    if isinstance(cmd, list):
        name = cmd[-1]
    else:
        name = cmd.split()[-1]

    if "." in name:
        name = name.split(".")[-1]

    return name


def get_log_file(cmd):
    name = extract_task_name(cmd)
    log_file = LOG_DIR / f"{name}.log"

    if log_file.exists():
        return log_file

    return None


def tail_log(log_file: Path, n: int = 5):
    if not log_file or not log_file.exists():
        return ["Log file not found"]

    with open(log_file) as f:
        lines = f.readlines()

    return lines[-n:]