import os
import logging
import logging.config

from datetime import datetime
from dotenv import find_dotenv, load_dotenv
from functools import wraps

class SetupLogging:
    def __init__(self, log_dir: str, config_dir: str):
        self.log_dir = log_dir
        self.config_dir = config_dir
        self.env_file = find_dotenv()
        load_dotenv(self.env_file)
        self.log_configs = {"dev": "logging.dev.ini", "prod": "logging.prod.ini"}
        self.config = self.log_configs.get(os.getenv("ENV", "dev"), "logging.dev.ini")
        self.config_path = os.path.join(self.config_dir, self.config)
        self.timestamp = datetime.now().strftime("%Y%m%d-%H:%M:%S")

    def __call__(self, cls):
        @wraps(cls)
        def wrapper(*args, **kwargs):
            try:
                logging.config.fileConfig(
                    self.config_path,
                    disable_existing_loggers=False,
                    defaults={"logfilename": f"{self.log_dir}/{self.timestamp}.log"},
                )
                logging.info(f"Logging setup complete using config: {self.config_path}")
            except Exception as e:
                logging.basicConfig(level=logging.INFO)
                logging.error(f"Failed to load logging config file: {e}")
            return cls(*args, **kwargs)
        return wrapper
