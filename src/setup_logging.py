import os
from dotenv import find_dotenv, load_dotenv
from prefect import get_run_logger
from functools import wraps

class SetupLogging:
    def __init__(self):
        self.env_file = find_dotenv()
        load_dotenv(self.env_file)

    def __call__(self, cls):
        @wraps(cls)
        def wrapper(*args, **kwargs):
            logger = get_run_logger()
            logger.info("Logging setup complete using Prefect's logger.")
            return cls(*args, **kwargs)
        return wrapper