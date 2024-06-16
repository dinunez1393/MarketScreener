import sys
from contextlib import contextmanager
import asyncio
import aiomysql
import mysql
from mysql.connector import Error

from src.utils.util_functions import show_message


# Running an Asynchronous MySQL connection
async def run_query(query, loop):
    conn = await aiomysql.connect(
        host='your_host',
        port=3306,
        user='your_user',
        password='your_password',
        db='your_database',
        loop=loop
    )
    async with conn.cursor() as cur:
        await cur.execute(query)
        result = await cur.fetchall()
        print(result)  # Or process the results as needed
    conn.close()


async def main():
    queries = [
        "SELECT * FROM table1",
        "SELECT * FROM table2",
        "SELECT * FROM table3"
    ]
    loop = asyncio.get_running_loop()
    tasks = [run_query(query, loop) for query in queries]
    await asyncio.gather(*tasks)


# Run the event loop
asyncio.run(main())


# Creating async pool for MySQL Connection:
import asyncio
import aiomysql


async def run_query(pool, query):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query)
            result = await cur.fetchall()
            print(result)  # Or process the results as needed


async def main():
    pool = await aiomysql.create_pool(
        host='your_host',
        port=3306,
        user='your_user',
        password='your_password',
        db='your_database',
        minsize=1,
        maxsize=10
    )

    queries = [
        "SELECT * FROM table1",
        "SELECT * FROM table2",
        "SELECT * FROM table3"
    ]

    tasks = [run_query(pool, query) for query in queries]
    await asyncio.gather(*tasks)

    pool.close()
    await pool.wait_closed()


# Run the event loop
asyncio.run(main())


# DB_Conn creator as context manager:
@contextmanager
def make_db_conn(self):
    """
    Method to make the connection to the MySQL DB. It can be used context manager
    :return: The DB connection
    :rtype: mysql.connector.pooling.PooledMySQLConnection
    """
    db_conn = None
    try:
        db_conn = mysql.connector.connect(host=self.host,
                                          user=self.user,
                                          password=self.password,
                                          database=self.database,
                                          pool_name='db_conns',
                                          pool_size=7)
        yield db_conn
    except Error as e:
        print(repr(e))
        self.logger_err.error(self.log.db_conn_err, exc_info=True)
        show_message()
        sys.exit(1)
    finally:
        if db_conn is not None and db_conn.is_connected():
            db_conn.close()

# Trial block
# import asyncio
#
# connection = DataBaseConn()
# db_conn1 = connection.open_db_conn()
# if db_conn1.is_connected():
#     print("Connection is successful\n", type(db_conn1))
#     db_conn1.close()
#
#
# # DB CONNECTION TEST PASSED
#
# async def async_func(query, async_pool):
#     async with async_pool.acquire() as db_conn:
#         async with db_conn.cursor() as cursor:
#             await cursor.execute(query)
#             result = await cursor.fetchall()
#             print(result)
#             print("Async connection made successfully\n", type(async_pool))
#
#
# async def other_async_func():
#     async_pool = await connection.open_async_dbPool()
#     queries = ["SELECT * FROM finan_invest.nyse_symbols;", "SELECT * FROM finan_invest.nyse_symbols;",
#                "SELECT * FROM finan_invest.nyse_symbols;"]
#     # loop = asyncio.get_running_loop()
#     tasks = [async_func(query, async_pool) for query in queries]
#     await asyncio.gather(*tasks)
#
#     async_pool.close()
#     await async_pool.wait_closed()
#
#
# asyncio.run(other_async_func())
#
# # ASYNC DB CONNECTION TEST PASSED - BOTH Individual Async Connection and Async Pool