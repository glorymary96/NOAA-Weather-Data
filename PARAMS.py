import datetime as dt
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy import text


WORKING_DIR = './'

FILES_DIR = './Files/'

GET_COUNTRIES = ['US'] # Add countries

DATE_NOW = dt.datetime.now()

CONN_PARAMS = {"server": "localhost","database": "WeatherDB","DSN": "weather","port": 59252,
    "driver": "ODBC Driver 17 for SQL Server",}

TABLE_DEF = {"name": "weather",
    "columns": {"ts": "DATETIME","date": "DATE","type": "VARCHAR(50)","location": "VARCHAR(50)",
        "sub_location": "VARCHAR(50)","sub_sub_location": "VARCHAR(50)","crop": "VARCHAR(50)","value": "FLOAT",}}

CONN = create_engine('mssql+pyodbc://@weather?TrustedConnection=True', connect_args={"timeout":120}).connect()

YEARS = np.arange(dt.datetime.now().year-1, dt.datetime.now().year)

def LOG(txt):
    print(dt.datetime.now().strftime('%Y-%m-%d %H:%M') + ' - ' + txt)

def clean_weather_db():
    LOG('Cleaning the weather database')
    sql_delete_duplicates = """
    DELETE FROM weather WHERE id NOT IN (SELECT MAX(id) AS id FROM weather group by date, type, location, sub_location, sub_sub_location, crop UNION SELECT id FROM weather WHERE date > ts)
    """
    CONN.execute(text(sql_delete_duplicates))
