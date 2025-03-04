import datetime as dt
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy import text


WORKING_DIR = 'C:/Users/hp/PycharmProjects/WeatherData/'

FILES_DIR = 'C:/Users/hp/PycharmProjects/WeatherData/Files/'

GET_COUNTRIES = ['US']

DATE_NOW = dt.datetime.now()

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
