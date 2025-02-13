# bootstrap.py
import sys
import os
from dotenv import load_dotenv

def bootstrap():
    try:        
        # Load environment variables from .env file
        load_dotenv()

        from daas_py_config import config
        from daas_py_common import logging_config

        # logging_config.logger.debug((config.get_configs().as_dict()))
        
        # You can call other initialization code here if necessary

        logging_config.logger.info("Bootstrap complete")

        return logging_config.logger, config
    except Exception as e:
        print(f"Error during bootstrap: {e}")
        return None, None