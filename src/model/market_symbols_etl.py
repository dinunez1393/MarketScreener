# ETL class for the NYSE Symbols table
import pandas as pd
from datetime import datetime as dt
from tqdm import tqdm
from src.model.etl import ETL
from src.CRUD.market_symbols_crud import MarketSymbols_CRUD
from src.utils.util_functions import iterable_splitter, datetime_from_py_to_sql, remove_dollar_sign


class MarketSymbols_ETL(ETL):
    """
    Class for manipulating the data (ETL) of the NYSE Symbols table in MySQL
    """
    def __init__(self, **connections_files):
        """
        Class constructor with keyword arguments for the file names and database connection.
        :param connections_files: Keyword arguments: DB Connection, files
        """
        super().__init__(**connections_files)
        self.stocks_df = None
        self.stocks_tuples = []

    def extract_data(self):
        """
        Method to extract the NYSE and NASDAQ stocks data
        """
        task_start = dt.now()
        na_values = ['N/A', 'null', '', 'n/a', 'NULL', 'Null']

        print("Extracting the market data from NYSE and NASDAQ\n")
        nyse_df = pd.read_csv(self.connections_files['nyse'],
                              converters={'Last Sale': remove_dollar_sign},
                              na_values=na_values,
                              keep_default_na=False)
        nyse_df['exchange'] = 'NYSE'

        nasdaq_df = pd.read_csv(self.connections_files['nasdaq'],
                                converters={'Last Sale': remove_dollar_sign},
                                na_values=na_values,
                                keep_default_na=False)
        nasdaq_df['exchange'] = 'NASDAQ'

        self.stocks_df = pd.concat([nyse_df, nasdaq_df], ignore_index=True)
        print(f"Market data extraction complete. T: {dt.now() - task_start}")

    def transform_data(self):
        """
        Method to transform the market symbols data (NYSE and NASDAQ) to get it ready for loading to DB
        """
        columns_order = ['Symbol', 'Name', 'Last Sale', 'Volume', 'IPO Year', 'Country',
                         'Industry', 'Sector', 'exchange']
        chunk_size = 5_000

        # Reindex stocks DF columns (it drops some unneeded columns)
        self.stocks_df = self.stocks_df.reindex(columns=columns_order)

        # Slice Stock name to maximum 65 chars
        self.stocks_df['Name'] = self.stocks_df['Name'].str.slice(0, 65)

        # Round stock last price
        self.stocks_df['Last Sale'] = self.stocks_df['Last Sale'].round(2)

        # Convert IPO year column from float to integer, then to string
        self.stocks_df['IPO Year'] = self.stocks_df['IPO Year'].astype('Int64')
        self.stocks_df['IPO Year'] = self.stocks_df['IPO Year'].astype(str)

        # Convert any np.nan to blank
        self.stocks_df = self.stocks_df.fillna('')

        # Convert blanks and string 'nan' to None
        self.stocks_df = self.stocks_df.map(lambda x: None if x == '' else x)
        self.stocks_df = self.stocks_df.map(lambda x: None if x == 'nan' or x == '<NA>' else x)

        # Assign ETL timestamp
        self.stocks_df['etl_time'] = datetime_from_py_to_sql(dt.now())

        # Chunk stocks dataframe
        chunked_stocks_df = iterable_splitter(self.stocks_df.copy(), chunk_size=chunk_size)

        # Convert each stocks DF in the list of chunked stocks DF into tuples
        for df_item in tqdm(chunked_stocks_df, total=len(chunked_stocks_df), desc="Converting Stocks DF to tuples"):
            df_tuples = [tuple(row) for _, row in df_item.iterrows()]
            self.stocks_tuples.append(df_tuples.copy())

    def load_data(self):
        """
        Method uses a MarketSymbols_crud object to insert the cleaned Market Symbols data
        """
        if len(self.stocks_tuples) > 0:
            marketSyms_crud = MarketSymbols_CRUD(self.connections_files['conn'],
                                                 self.connections_files['table_name'],
                                                 self.connections_files['db_name'],
                                                 self.stocks_tuples)
            marketSyms_crud.truncate_table()  # Clear Market Symbols table first
            marketSyms_crud.insert_to_table()
        else:
            self.logger_inf.info(self.log.no_data_upload.format("Market Symbols table\n\n"))
