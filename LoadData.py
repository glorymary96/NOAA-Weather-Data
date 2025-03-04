from concurrent import futures
import csv
import datetime as dt
import queue
import pandas as pd
import sqlactions

from PARAMS import CONN, FILES_DIR

MULTI_ROW_INSERT_LIMIT = 1000
WORKERS = 6

TABLE_DEF = {"name": "weather",
    "columns": {"ts": "DATETIME","date": "DATE","type": "VARCHAR(50)","location": "VARCHAR(50)",
        "sub_location": "VARCHAR(50)","sub_sub_location": "VARCHAR(50)","crop": "VARCHAR(50)","value": "FLOAT",}}

CONN_PARAMS = {"server": "localhost","database": "WeatherDB","DSN": "weather","port": 59252,
    "driver": "ODBC Driver 17 for SQL Server",}

def create_csv(list_of_df,csv_name):
    df = pd.concat(list_of_df)
    df['ts'] = df['ts'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df = df[['ts','date','type','location','sub_location','sub_sub_location','crop','value']].groupby(
        ['ts','date','type','location','sub_location','sub_sub_location','crop']).mean().reset_index()

    df.to_csv(csv_name + '.csv',sep='|',encoding='utf-8',index=False)

def read_csv(csv_file):
    with open(csv_file,encoding="utf-8",newline="") as in_file:
        reader = csv.reader(in_file,delimiter="|")
        next(reader)  # Header row

        for row in reader:
            yield row


def process_row(row,batch,table_def,conn_params):
    batch.put(row)
    if batch.full():
        sqlactions.multi_row_insert(batch,table_def,conn_params)

    return batch


def load_csv(csv_file,table_def,conn_params):
    batch = queue.Queue(MULTI_ROW_INSERT_LIMIT)

    with futures.ThreadPoolExecutor(max_workers=WORKERS) as executor:
        todo = []

        for row in read_csv(csv_file):
            future = executor.submit(process_row,row,batch,table_def,conn_params)
            todo.append(future)

        for future in futures.as_completed(todo):
            result = future.result()
    if not result.empty():
        sqlactions.multi_row_insert(result,table_def,conn_params)

