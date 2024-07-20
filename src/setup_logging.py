import logging
import logging.config
import os
from datetime import datetime
from dotenv import find_dotenv, load_dotenv

def setup_logging(LOG_DIR):
    """Load logging configuration"""
    env_file = find_dotenv()
    load_dotenv()
    log_configs = {"dev": "logging.dev.ini", "prod": "logging.prod.ini"}
    config = log_configs.get(os.environ["ENV"], "logging.dev.ini")
    config_path = "/".join([LOG_DIR, config])
    timestamp = datetime.now().strftime("%Y%m%d-%H:%M:%S")
    logging.config.fileConfig(
        config_path,
        disable_existing_loggers=False,
        defaults={"logfilename": f"{LOG_DIR}/{timestamp}.log"},
    )