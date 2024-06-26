# Logger class
import logging
import sys
from src.utils.global_constants import Paths
from src.utils.util_functions import show_message


class Logger:
    """
    Class for creating and managing logs
    """
    def __init__(self, logger_name: str):
        """
        Constructor for the Logger class
        :param logger_name: the name of the logger
        """
        self.logger_name = logger_name

        # Logging messages
        # Errors
        self.db_conn_err = "A database connection error occurred\n"
        self.truncation_err = "Error occurred during truncation of {0}.{1}\n"
        self.insert_err = "Error occurred during INSERT operation of {0}.{1}\n"

        # Info messages
        self.end_program = "Program completed successfully. {}"
        self.no_data_upload = "No new data to load to {}\n"
        self.trunc_success = "Table {0}.{1} has been TRUNCATED successfully.\n{2}\n"
        self.insert_success = "INSERT operation on {0}.{1} completed successfully\n{2}\n"

    def logger_creator(self, logger_type='ERROR', logger_name=None, logging_path=None):
        """
        Method creates a logger object
        :param logger_type: the type of the logger (e.g. error, info, etc.)
        :param logger_name: If defined (not None) use this parameter to change the logger name
        :type logger_name: str
        :param logging_path: The location for the log file (if defined)
        :type logging_path: str
        :return: the logger object
        :rtype: logging.Logger
        """
        if logging_path:
            logger_path = logging_path
        elif logger_type == 'ERROR':
            logger_path = Paths.ERRORS_LOG.value
        elif logger_type == 'INFO':
            logger_path = Paths.INFO_LOG.value
        else:
            print("\nThat is not a correct logging type name.\n")
            show_message()
            sys.exit(1)
            
        # Create logger
        if logger_name is not None:  # Change Logger name
            self.logger_name = logger_name

        logger = logging.getLogger(self.logger_name)
        logger.setLevel(logger_type)

        # Configure handlers
        formatter = logging.Formatter('\n%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        # For Console display
        logger_console_handler = logging.StreamHandler()
        logger_console_handler.setFormatter(formatter)
        logger.addHandler(logger_console_handler)

        # For saving to Log file
        logger_file_handler = logging.FileHandler(logger_path)
        logger_file_handler.setFormatter(formatter)
        logger.addHandler(logger_file_handler)

        return logger
