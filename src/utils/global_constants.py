from enum import Enum
from pathlib import Path
import os

PROJECT_FOLDER = Path(__file__).parent.parent.parent


class Paths(Enum):
    """
    Enum class has the paths of files used in this script
    """

    INFO_LOG = rf"{PROJECT_FOLDER}{os.sep}Logs{os.sep}info.log"
    ERRORS_LOG = rf"{PROJECT_FOLDER}{os.sep}Logs{os.sep}errors.log"
    MAIN = rf"{PROJECT_FOLDER}{os.sep}src{os.sep}main.py"
    SRC_FOLDER = rf"{PROJECT_FOLDER}{os.sep}src{os.sep}"
    NASDAQ_CSV = rf"{PROJECT_FOLDER}{os.sep}Files{os.sep}nasdaq_stocks.csv"
    NYSE_CSV = rf"{PROJECT_FOLDER}{os.sep}Files{os.sep}nyse_stocks.csv"


class DB_Tables(Enum):
    """
    Enum class has the names of databases and tables
    """
    # Databases
    FINAN_INVEST_db = "finan_invest"

    # finan_invest DB tables
    MARKET_SYMS_tbl = "market_symbols"
