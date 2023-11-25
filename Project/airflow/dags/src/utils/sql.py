import pandas as pd
from sqlalchemy import create_engine

def read_sql(name):
    try:
        with open(f'sql/{name}.sql') as s:
            sql = s.readlines()
    except FileNotFoundError:
        print('There is no script with that name')

def load_from_pg(pg_hook, request_name):
    sql = read_sql(request_name)
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)
    colnames = [desc[0] for desc in cursor.description]
    records = cursor.fetchall()
    result = pd.DataFrame(records, columns=colnames)
    connection.close()

def save_to_pg(pg_hook, df, destination):
    engine = create_engine(pg_hook.get_uri())
    df.to_sql(destination, engine, if_exists='append')