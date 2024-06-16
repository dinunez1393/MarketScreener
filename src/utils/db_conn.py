# Database connection class
import mysql.connector
import sys
import aiomysql
import asyncio
from datetime import datetime as dt
from mysql.connector import Error
from contextlib import contextmanager
from src.utils.logger import Logger
from src.utils.util_functions import show_message


class DataBaseConn:
    """
    Class for creating DB connection objects
    """

    def __init__(self, host='localhost', user='diegonunez', password='el13deJuli024$', database='finan_invest',
                 port=3306):
        """
        Class constructor with the MySQL connection parameters
        :param host: the name of the server (host)
        :param user: the name of the user from the host
        :param password: the password of the user
        :param database: the name of the database
        :param port: the port number of the MySQL host
        """
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.db_conn = None
        self.async_dbConn = None
        self.async_dbPool = None
        self.log = Logger(logger_name='DB_Conn_Error')
        self.logger = self.log.logger_creator()

    def open_db_conn(self):
        """
        Method to make the connection to a MySQL DB
        """
        conn_start = dt.now()
        try:
            self.db_conn = mysql.connector.connect(host=self.host,
                                                   user=self.user,
                                                   password=self.password,
                                                   database=self.database)
        except Error as e:
            print(repr(e))
            self.logger.error(self.log.db_conn_err, exc_info=True)
            show_message()
            sys.exit(1)
        else:
            print(f"DB Connection established successfully: host: {self.host}; user: {self.user}\n"
                  f"T: {dt.now() - conn_start}\n\n")

    @contextmanager
    def open_db_cursor(self):
        """
        Method opens the DB cursor. It can be used as context manager. It will only work if db_conn is already
        established. This context manager was customized because MySQL Connector library does not provide one.
        :return: The DB cursor
        :rtype: mysql.connector.abstracts.MySQLCursorAbstract
        """
        cursor = None
        try:
            cursor = self.db_conn.cursor()
            yield cursor
        except Error as e:
            print(repr(e))
            self.logger.error(self.log.db_conn_err, exc_info=True)
            show_message()
            sys.exit(1)
        finally:
            if cursor:
                cursor.close()

    async def open_async_dbConn(self, loop):
        """
        Method to make asynchronous connection to a MySQL DB.
        :param loop: The running event loop for the async DB connection
        """
        conn_start = dt.now()

        try:
            self.async_dbConn = await aiomysql.connect(host=self.host,
                                                       user=self.user,
                                                       password=self.password,
                                                       db=self.database,
                                                       port=self.port,
                                                       loop=loop)
        except Exception as e:
            print(repr(e))
            self.logger.error(self.log.db_conn_err, exc_info=True)
            show_message()
            sys.exit(1)
        else:
            print(f"DB ASYNC Connection established successfully: host: {self.host}; user: {self.user}\n"
                  f"T: {dt.now() - conn_start}\n\n")

    async def open_async_dbPool(self):
        """
        Method to create an asynchronous pool to a MySQL DB.
        """
        conn_start = dt.now()

        try:
            self.async_dbPool = await aiomysql.create_pool(host=self.host,
                                                           user=self.user,
                                                           password=self.password,
                                                           db=self.database,
                                                           port=self.port,
                                                           minsize=1,
                                                           maxsize=10)
        except Exception as e:
            print(repr(e))
            self.logger.error(self.log.db_conn_err, exc_info=True)
            show_message()
            sys.exit(1)
        else:
            print(f"DB ASYNC pool established successfully: host: {self.host}; user: {self.user}\n"
                  f"T: {dt.now() - conn_start}\n\n")

    def close_all_dbConns(self):
        """Method closes all the DB connections from this object instance. Including async"""
        try:
            if self.db_conn is not None and self.db_conn.is_connected():
                self.db_conn.close()
                print("\nDB Connection closed successfully\n\n")
        except Error as e:
            print(repr(e))
            print("\nDB Connection is already closed\n")
        asyncio.run(self.__helper_close_all_async_dbConns())

    async def __helper_close_all_async_dbConns(self):
        """
        Helper method for closing all the ASYNC DB connections
        """
        try:
            if self.async_dbConn:
                self.async_dbConn.close()
                print("\nASYNC DB connection closed successfully\n")
            if self.async_dbPool:
                self.async_dbPool.close()
                await self.async_dbPool.wait_closed()
                print("\nASYNC DB pool closed successfully\n")
        except Error as e:
            print(repr(e))
            print("\nASYNC DB Connection or Pool is already closed\n")
