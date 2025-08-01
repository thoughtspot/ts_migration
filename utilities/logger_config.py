import os
import logging

# Global variable for the logger
logger = None

def get_logger():
    global logger
    if logger is None:
        raise Exception("Logger not initialized. Call setup_logger() first.")
    return logger

def setup_logger(log_folder: str):
    global logger
    if logger is None:
        if not os.path.exists(log_folder):
            os.makedirs(log_folder)

        log_file_path = os.path.join(log_folder, 'migrator_info.log')
        error_log_file_path = os.path.join(log_folder, 'migrator_error.log')

        # Configure the logger
        logger = logging.getLogger('migrator_logger')
        logger.setLevel(logging.INFO)

        # Handlers
        info_handler = logging.FileHandler(log_file_path)
        info_handler.setLevel(logging.INFO)

        error_handler = logging.FileHandler(error_log_file_path)
        error_handler.setLevel(logging.ERROR) 

        console_handler = logging.StreamHandler()

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - Line: %(lineno)d - %(message)s')
        info_handler.setFormatter(formatter)
        error_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Adding handlers
        logger.addHandler(info_handler)
        logger.addHandler(error_handler)
        logger.addHandler(console_handler)

    return logger
