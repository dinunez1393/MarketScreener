# Abstract class for creating other CRUD classes
import sys
from abc import ABC, abstractmethod
from datetime import datetime as dt
from mysql.connector import Error
from src.utils.logger import Logger
from src.utils.util_functions import show_message
from src.utils.db_conn import DataBaseConn


class Table_crud(ABC):
    """
    Abstract class for creating other CRUD classes
    """

    def __init__(self, connection: DataBaseConn, table_name: str, db_name: str):
        """
        Constructor for Table_crud
        :param connection: Connection object (not DB connection itself). Actual DB conns (normal or async) must be
        opened first prior to using the object's connection on MySQL queries.
        :param table_name: The name of a table in the database
        :param db_name: The name of the database where the table resides
        """
        self.log = Logger(logger_name="MySQL Queries_ERROR")
        self.logger_err = self.log.logger_creator()
        self.logger_inf = self.log.logger_creator('INFO', logger_name="MySQL Queries_INFO")
        self.connection = connection
        self.table_name = table_name
        self.db_name = db_name

    @abstractmethod
    def insert_to_table(self):
        pass

    @abstractmethod
    def select_from_table(self):
        pass

    @abstractmethod
    def update_table(self):
        pass

    @abstractmethod
    def delete_from_table(self):
        pass

    def truncate_table(self):
        """
        Method to truncate the table
        """
        # Define the TRUNCATE query with the table name directly
        truncate_query = f"TRUNCATE TABLE {self.db_name}.{self.table_name};"
        query_start = dt.now()

        # Execute the TRUNCATE query
        with self.connection.open_db_cursor(self.logger_err,
                                            self.log.truncation_err.format(self.db_name, self.table_name)) as cursor:
            cursor.execute(truncate_query)

            # Commit the transaction
            self.connection.db_conn.commit()
            self.logger_inf.info(self.log.trunc_success.format(self.db_name, self.table_name,
                                                               f"T: {dt.now() - query_start}"))
