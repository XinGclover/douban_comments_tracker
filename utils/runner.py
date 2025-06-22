import logging
from datetime import datetime, timedelta
from utils.common import safe_sleep


def run_forever(main_loop_func):
    """ Continuously runs the main loop function, handling exceptions and sleeping between cycles.
    :param main_loop_func: function to run in the loop
    """ 
    while True:
        try:
            logging.info("🚀 Starting a new fetch cycle...")
            main_loop_func()
            safe_sleep(90, 120)
        except ValueError as e:  # Replace with a specific exception type
            logging.error("🔥 Unexpected crash in outer loop: %s", e)
            safe_sleep(60, 90)


def run_for_rounds(main_loop_func, n_rounds=10):
    """ Runs the main loop function for a specified number of rounds, handling exceptions and sleeping between rounds.
    :param main_loop_func: function to run in each round
    :param n_rounds: int, number of rounds to run
    """ 
    for round_num in range(1, n_rounds + 1):
        try:
            logging.info("🚀 [Round %d/%d] Starting fetch...", round_num, n_rounds)
            main_loop_func()
            safe_sleep(90, 120)
        except ValueError as e:  # Replace with a specific exception type
            logging.error("🔥 Round %d failed: %s", round_num, e)
            safe_sleep(60, 90)

    logging.info("🛑 All rounds complete, exiting.")


def run_for_duration(main_loop_func, minutes=5):
    """ Runs the main loop function for a specified duration, handling exceptions and sleeping between cycles.
    :param main_loop_func: function to run in each cycle
    :param hours: int, duration in hours to run the loop
    """
    end_time = datetime.now() + timedelta(minutes=minutes) 

    while datetime.now() < end_time:
        try:
            logging.info("🚀 Starting a new fetch cycle...")
            main_loop_func()
            safe_sleep(90, 120)
        except ValueError as e:  # Replace with a specific exception type
            logging.error("🔥 Unexpected crash: %s", e)
            safe_sleep(60, 90)

    logging.info("🛑 Time limit reached, exiting.")
