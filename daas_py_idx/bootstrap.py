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

        logger_prepend = "daas_py_idx"
        
        if os.getenv("DOMAIN"):
            DOMAIN = os.getenv("DOMAIN").upper().strip().replace("'", "")
        else:
            DOMAIN = "UNKNOWN"

        for handler in logging_config.logger.handlers:
            old_format = handler.formatter._fmt  # Get the current format
            if not (old_format.startswith(logger_prepend)):
                new_format = f"{logger_prepend}-{DOMAIN} - {old_format}"  # Prepend text
                handler.setFormatter(logging_config.logging.Formatter(new_format))
            
        # logging_config.logger.debug((config.get_configs().as_dict()))
        
        # You can call other initialization code here if necessary

        logging_config.logger.info("Bootstrap complete")

        return logging_config.logger, config
    except Exception as e:
        print(f"Error during bootstrap: {e}")
        return None, None
    