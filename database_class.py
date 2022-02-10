import logger_handler
import pyodbc 
import pandas as pd
from urllib.parse import quote_plus
import sqlalchemy

# define parameters
db_host = '.'
db_name = 'Bestseller'
driver = 'DRIVER={ODBC Driver 17 for SQL Server}'
chunk_size = 1

my_logger = logger_handler.get_logger("database class")

# this function used for checking duplicate ids :
# chack ids inserted to database and returns them
def load_ids(table_name,pk_id):
    IDs = []
    try:
        driver ='{SQL Server}'
        dbConnection = pyodbc.connect(f'Driver={driver};Server={db_host};Database={db_name};Trusted_Connection=yes;')
        cursor = dbConnection.cursor()
        my_logger.debug(f"fetch {pk_id} FROM {table_name}")
        cursor.execute(f'SELECT DISTINCT {pk_id} FROM {table_name}')
        IDs = cursor.fetchall()
    except pyodbc.Error as ex:
        my_logger.debug("Exception occurred on load_ids", exc_info=True)
    return IDs

#bulk_insert give table name and dataframe then insert datas into MSSQL database with chunk size 100 (it can be more than 100)
def bulk_insert(lines_collected_temp, table_name):
    dfItem  = pd.DataFrame.from_records(lines_collected_temp)
    
    try:
        conn =  f"{driver};SERVER={db_host};DATABASE={db_name};Trusted_Connection=yes;"
        quoted = quote_plus(conn, safe='', encoding=None, errors=None)
        new_con = 'mssql+pyodbc:///?odbc_connect={}'.format(quoted)
        sqlEngine = sqlalchemy.create_engine(new_con)  
        my_logger.debug(f'connect to mssql server to insert data into {table_name}')
        dbConnection    = sqlEngine.connect()
        frame = dfItem.to_sql(
                    table_name, 
                    dbConnection, 
                    if_exists='append',
                    index=False,
                    chunksize= chunk_size,
                    dtype={'helpful': sqlalchemy.types.JSON,
                    'salesRank': sqlalchemy.types.JSON,
                    'related': sqlalchemy.types.JSON,
                    'categories': sqlalchemy.types.JSON
                    }
            )
        my_logger.debug(str(chunk_size) +" data inserted int database successfuly")
    except:
        my_logger.debug("Exception occurred in bulk_insert", exc_info=True)
