# CRUD class for the nyse_symbols table
import sys
from mysql.connector import Error
from tqdm import tqdm
from datetime import datetime as dt
from src.CRUD.table_crud import Table_crud
from src.utils.db_conn import DataBaseConn
from src.utils.util_functions import show_message


class MarketSymbols_CRUD(Table_crud):
    """
    Class for performing CRUD operations of the Market Symbols data (NYSE & NASDAQ)
    """

    def __init__(self, connection: DataBaseConn, table_name: str, db_name: str, market_symbols_data=None):
        """
        Constructor for MarketSymbols_CRUD
        :param connection: Connection object (not DB connection itself). Actual DB conns (normal or async) must be
        opened first prior to using the object's connection on MySQL queries.
        :param table_name: The name of a table in the database
        :param db_name: The name of the database where the table resides
        :param market_symbols_data: The data for the market symbols
        """
        super().__init__(connection, table_name, db_name)
        self.market_symbols_data = market_symbols_data

    def insert_to_table(self):
        """
        Method to insert Market Symbols data to DB
        """
        insert_op_start = dt.now()
        query = f"""
            INSERT INTO {self.db_name}.{self.table_name} (
                symbol, stock_name, LastPrice, LastVolume, IPO_year, industry, sector, exchange, Extraction_Timestamp
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        try:
            with self.connection.open_db_cursor() as cursor:
                for market_syms in tqdm(self.market_symbols_data, total=len(self.market_symbols_data),
                                        desc=f"INSERTing Market Symbols data to {self.db_name}.{self.table_name}"):
                    cursor.executemany(query, market_syms)
        except Error as e:
            print(repr(e))
            self.connection.db_conn.rollback()
            self.logger_err.error(self.log.insert_err.format(self.db_name, self.table_name), exc_info=True)
            show_message()
            sys.exit(1)
        else:
            # Commit the transaction
            self.connection.db_conn.commit()
            self.logger_inf.info(self.log.insert_success.format(self.db_name, self.table_name,
                                                                f"T: {dt.now() - insert_op_start}"))

    def select_from_table(self):
        pass

    def update_table(self):
        pass

    def delete_from_table(self):
        pass
