import can_parser
from time import time
import dbc_parser
from utils_db import extract_name
import pandas as pd

'''
Merge multiple DBCs and parse them with CAN log.
Create and read csv files
'''


def main(can_log, dbcs_file):
    start_time = time()

    dbc_output = ''
    ecu_output = ''
    list_df = []
    list_ecu = []
    # POSTGRESQL server required table name <64 chars
    # if 2 dbcs are requested merge the names
    for dbc in dbcs_file:
        dbc_name = extract_name(dbc)
        if len(dbc_name) > 30:
            try:
                index_end = dbc_name.find("can")
                dbc_name = dbc_name[:index_end]
            except:
                dbc_name = dbc_name[:25]
        dbc_output += dbc_name
        ecu_output += dbc_name
        dbc_path, ecu_path = dbc_parser.main(dbc)
        dbc_df = pd.read_csv(dbc_path, sep=',')
        ecu_df = pd.read_csv(ecu_path, sep=',')
        list_df.append(dbc_df)
        list_ecu.append(ecu_df)
    dbc_output += '_DBC.csv'
    ecu_output += '_ECU.csv'
    bh_c1 = pd.concat(list_df, ignore_index=True)
    ecus = pd.concat(list_ecu, ignore_index=True)
    bh_c1.to_csv(dbc_output, index=False)
    ecus.to_csv(ecu_output, index=False)
    can_path = can_parser.main(can_log, dbcs_file)

    print("---TOTAL TIME: %s seconds ---" % (time() - start_time))
    return can_path, dbc_output, ecu_output
