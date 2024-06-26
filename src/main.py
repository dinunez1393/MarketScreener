import sys
from datetime import datetime as dt
from pathlib import Path

# Add the src package to the Python path
sys.path.append(rf"{Path(__file__).parent.parent}")
from src.utils.db_conn import DataBaseConn
from src.model.market_symbols_etl import MarketSymbols_ETL
from src.utils.logger import Logger
from src.utils.alerts import AlertType
from src.utils.util_functions import show_message
from src.utils.util_functions2 import memit
from src.utils.global_constants import *


if __name__ == '__main__':
    program_start = dt.now()
    log = Logger(logger_name="Main Program")
    logger = log.logger_creator('INFO')

    # Establish DB connections
    conn = DataBaseConn()
    conn.open_db_conn()

    # Perform ETL of Market Symbols
    marketSyms_etl = MarketSymbols_ETL(nyse=Paths.NYSE_CSV.value,
                                       nasdaq=Paths.NASDAQ_CSV.value,
                                       conn=conn,
                                       table_name=DB_Tables.MARKET_SYMS_tbl.value,
                                       db_name=DB_Tables.FINAN_INVEST_db.value)
    marketSyms_etl.extract_data()
    marketSyms_etl.transform_data()
    marketSyms_etl.load_data()

    # TODO: Screener using Yahoo Finance

    # Close DB connections
    conn.close_all_dbConns()

    # End
    logger.info(log.end_program.format(f"T: {dt.now() - program_start}\n\n"))
    show_message(AlertType.SUCCESS)
