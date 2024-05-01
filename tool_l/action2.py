import pandas as pd
from decimal import Decimal
from tqdm import tqdm
import ast
from utils_db import extract_name, extract_tf_name
pd.options.mode.chained_assignment = None  # default='warn'
'''

ACTION 2

Extract dataType from any CAN Signal retrieved on Action #1
0: Data exactly mapped 
1: Signal length in bit is less in DBC then TF (e.g.: dbc signal->2 bit, tf signal->int8; not hard problem (padding 000000 data 11))
2: Signal length in bit is more in DBC then TF (e.g.: dbc signal->16 bit, tf signal->int8; hard problem (truncated data = losses))
-1: Not mapped value (error/not required check)

'''


def main(dbc_csv, tf_excel, ecu):
    dbc_name = extract_name(dbc_csv)
    tf_name = extract_tf_name(tf_excel)
    dbc_name = dbc_name[:-4]

    if len(dbc_name) > 30:
        try:
            index_end = dbc_name.find("CAN")
            dbc_name = dbc_name[:index_end]
        except:
            dbc_name = dbc_name[:25]

    try:
        init_name = tf_name.index('R')
        tf_name = tf_name[init_name:]
    except:
        tf_name = extract_name(tf_excel)

    dbc_df = pd.read_csv(dbc_csv)
    tf_df = pd.read_excel(tf_excel)
    ecu_df = pd.read_csv(ecu)

    dbc_df['Msg_Sign'] = dbc_df.Msg_Name + "." + dbc_df.Signal_name
    tf_df['Msg_Sign'] = tf_df.Msg_Name + "." + tf_df.Signal_name
    merge = pd.merge(dbc_df, tf_df, on=['Msg_Sign'])

    merge = merge.drop(merge.columns.difference(['architecture','Length_bit', 'Msg_Sign',
                                                 'apiType', 'Min', 'apiKey', 'choices', 'GAPI_struct']),
                       axis=1)
    merge['Min'].fillna(0, inplace=True)
    merge.fillna('-', inplace=True)
    results = []
    len_data = len(merge)
    for row in tqdm(merge.itertuples(), unit=' rows', total=len_data, desc='Checking data types'):
        try:
            if 'ENUM' in row.apiType:
                start = row.apiType.index('{')
                end = row.apiType.index('}')
                string = row.apiType[start + 1:end]
                len_data = len(string.strip().split(','))
                occ_dbc = len(ast.literal_eval(row.choices))
                if len_data < occ_dbc:
                    res = 2
                elif len_data == occ_dbc:
                    res = 0
                else:
                    res = 1
            else:
                if row.apiType == 'double':
                    nAPI_type = 64
                elif row.apiType == '-':
                    nAPI_type = -1
                elif row.apiType == 'float':
                    nAPI_type = 32
                # assumo che tutti i valori all'interno
                # sia tutti dello stesso tipo
                elif 'Struct' in row.apiType:
                    start = row.apiType.index('{')
                    end = row.apiType.index('}')
                    string = row.apiType[start + 1:end]
                    data_struct = string.strip().split('=')
                    type_struct = data_struct[0].strip()
                    if type_struct == 'uint8':
                        nAPI_type = 8
                    elif type_struct == 'uint16':
                        nAPI_type = 16
                    elif type_struct == 'uint32':
                        nAPI_type = 32
                    elif type_struct == 'double':
                        nAPI_type = 64
                    elif type_struct == 'float':
                        nAPI_type = 32

                elif row.Min >= 0:
                    if (row.apiType == 'uint8') or (row.apiType == 'int8'):
                        nAPI_type = 8
                    elif (row.apiType == 'uint16') or (row.apiType == 'int16'):
                        nAPI_type = 16
                    elif (row.apiType == 'uint32') or (row.apiType == 'int32'):
                        nAPI_type = 32

                elif row.Min < 0:
                    if row.apiType == 'int8':
                        nAPI_type = 8
                    elif row.apiType == 'int16':
                        nAPI_type = 16
                    elif row.apiType == 'int32':
                        nAPI_type = 32

                else:
                    nAPI_type = -1

                dec_length_bit = Decimal(row.Length_bit)
                res = -1
                if dec_length_bit == nAPI_type:
                    res = 0
                elif dec_length_bit < nAPI_type:
                    res = 1
                elif dec_length_bit > nAPI_type:
                    res = 2
        except:
            print(row)
            print(row.apiType)

        results.append(res)

    merge['Diff'] = results
    name_final = tf_name + '_tfdbc_' + dbc_name
    merge.to_excel(name_final + '.xlsx')
    ecu_name = dbc_name + '_ECU'
    dbc_name = dbc_name + "_DBC"

    return [(merge, name_final), (dbc_df, dbc_name), (tf_df,  "tf_"+tf_name), (ecu_df,  ecu_name)]
