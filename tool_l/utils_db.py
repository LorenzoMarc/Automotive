import os
import pandas as pd
from sqlalchemy import types, create_engine, DateTime
import time
from tqdm import tqdm
from pathlib import PurePath
import re
import stored_proc as sp


def extract_name(file):
    name = PurePath(file).stem
    name = name.replace('-', '_')
    name = name.replace(' ', '_')
    name = name.replace('.', '_', 1)
    name = name.replace('(', '')
    name = name.replace(')', '')
    name = name.replace('+', '')
    name = name.split('.')[0]
    line = re.sub(r"-\ \.", "", name).lower()
    return line


def set_conn_parameters(host, port, db, user, psw):
    engine = create_engine(
        'postgresql+psycopg2://' + user.strip() + ':' + psw.strip() + '@' + host.strip() + ':' + port.strip() + '/' + db.strip())
    return engine


## CHECK length DBCs files (or total name file)
def upload_dfs(list_to_upload, engine):
    for dbc in list_to_upload:
        for df, df_name in tqdm(iterable=[dbc], unit='tables', total=1,
                                desc='Uploading data to DB...'):
            #df.fillna('-', inplace=True)
            if df.__len__() == 0:
                pass
            elif '_log' in df_name:
                try:
                    # this will fail if there is a new column
                    df.to_sql(df_name.lower(), con=engine, schema="FcaData", if_exists='append',
                              dtype=types.VARCHAR())
                except:
                    data = pd.read_sql('SELECT * FROM "FcaData".' + df_name.lower(), engine)
                    df2 = pd.concat([data, df])
                    res_row = df2.to_sql(name=df_name.lower(), con=engine, schema="FcaData", if_exists='replace',
                                         dtype=types.VARCHAR())
            else:
                df.to_sql(df_name.lower(), con=engine, schema="FcaData", if_exists='replace', index=True,
                          dtype=types.VARCHAR())

            if ('ro_log' in df_name) and len(df) >= 1:
                # call the stored procedure to calculate the sessions and truncate ro_log table
                done = sp.ro_sessions()
            if ('phev_log' in df_name) and len(df) >= 1:
                done = sp.phev_sessions()
                if not done:
                    print('Error computing PHEV sessions')
            if ('vf_log' in df_name) and len(df) >=1:
                done = sp.vf_sessions()
    return True


def main(csv_file, engine, ecu_path, dbc=None):
    print('Uploading to DB... ')
    start = time.time()
    table_name = csv_file.split('.')[0]
    ecu_name = ecu_path.split('.')[0]
    ecu_df = pd.read_csv(ecu_path, low_memory=False)
    ecu_df.to_sql(ecu_name.lower(), con=engine, schema="FcaData", if_exists='replace', index=True)
    conn = sp.set_connection()
    cursor = conn.cursor()
    cursor.execute('CREATE TABLE IF NOT EXISTS "FcaData".{} ( '
                   '"index" bigint  PRIMARY KEY, '
                   '"CAN_ID" bigint, '
                   '"Code" double precision,'
                   ' "Msg_Name" varchar(255), '
                   '"Signal_name" varchar(255),'
                   ' "Timestamp" timestamp, '
                   '"Value" varchar(255))'.format(table_name.lower()))
                   #'PRIMARY KEY ("CAN_ID", "Timestamp", "Signal_name"))'.format(table_name.lower()))

    cursor.close()
    conn.close()
    if dbc is not None:
        dbc_table_name = dbc.split('.')[0]
        dbc_df = pd.read_csv(dbc, low_memory=False)

        dbc_df.to_sql(dbc_table_name.lower(), con=engine, schema="FcaData", if_exists='replace',
                      dtype={'index': types.Integer(), 'Signal_name': types.String(), 'Start_bit': types.Integer(),
                             'Length_bit': types.Integer(), 'Modality': types.String(), 'Scale': types.Float(),
                             'Offset': types.Float(), 'Min': types.Float(), 'Max': types.Float(),
                             'Unit': types.String(),
                             'Receiver': types.String(), 'Msg_Name': types.String(), 'CAN_ID': types.String(),
                             'Length_Data': types.Integer(), 'Sender': types.String()})

    data = pd.read_csv(csv_file, usecols=['index', 'Msg_Name', 'Timestamp', 'Signal_name', 'Code', 'CAN_ID', 'Value'],
                       dtype={'Value': 'string[pyarrow]', 'Timestamp':'string'}, low_memory=False, index_col='index')

    data.to_sql(table_name.lower(), con=engine, schema="FcaData", if_exists='append', chunksize=20000, index_label='index',
                dtype={'index':types.Integer(), 'Msg_Name': types.String(), 'Timestamp': DateTime(),
                       'Signal_name': types.String(), 'Code': types.String(),
                       'CAN_ID': types.Integer(), 'Value': types.String()})

    print("END UPLOAD in %s" % (time.time() - start))
    print('Data Uploaded to host')
    print('Choose other files or close this application')

    return True


def extract_tf_name(tf_file):
    name = PurePath(tf_file).stem
    name = name.replace('-', '_')
    name = name.replace(' ', '_')
    name = name.replace('.', '_')
    name = name.replace('(', '')
    name = name.replace(')', '')
    line = re.sub(r"-\ \.", "", name)
    return line


def del_temporary_files():
    dir_list = os.listdir('.')

    for item in dir_list:
        if item.endswith(".csv"):
            os.remove(os.path.join('.', item))
