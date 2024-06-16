# Abstract class for creating other ETL classes
from abc import ABC, abstractmethod
from src.utils.logger import Logger


class ETL(ABC):
    """
    Abstract class for creating other Extraction, Load, Transform classes
    """
    def __init__(self, **connections_files):
        """Constructor with optional keyword arguments for database connectivity or files. If using files, they
        typically are of type csv, excel, parquet, hdf5."""
        self.connections_files = connections_files
        self.log = Logger(logger_name="ETL Operation")
        self.logger_inf = self.log.logger_creator('INFO')

    @abstractmethod
    def extract_data(self):
        pass

    @abstractmethod
    def transform_data(self):
        pass

    @abstractmethod
    def load_data(self):
        pass
