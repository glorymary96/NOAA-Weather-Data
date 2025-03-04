import pyodbc
from pypika import Column, Query, Table


def execute_query(q, conn_params):
    connection = pyodbc.connect(server=conn_params['server'], DSN=conn_params['DSN'],timeout=30)
    cursor = connection.cursor()

    cursor.execute(q)
    connection.commit()
    connection.close()

def make_table(table_def, conn_params):
    table = Table(table_def["name"])
    cols = [Column(k, v) for k, v, in table_def["columns"].items()]

    drop = Query.drop_table(table).if_exists()
    create = Query.create_table(table).columns(*cols)

    execute_query(str(drop) + "\n" + str(create), conn_params)


def multi_row_insert(batch, table_def, conn_params):
    row_expressions = []

    for _ in range(batch.qsize()):
        row_data = tuple(batch.get())
        row_expressions.append(row_data)

    cols = [s[0] for s in table_def["columns"].items()]

    table = Table(table_def["name"], schema = table_def["schema"] if "schema" in table_def.keys() else None)

    insert_into = Query.into(table).columns(cols).insert(*row_expressions)

    execute_query(str(insert_into), conn_params)