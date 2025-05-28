import pandas as pd

import utils_db
from utils_db import extract_name
import dbc_parser

'''
Extract specs from features' file
Extract dbc messages and signals 

'''


def main(featureSet, dbcs):
    ff_name = extract_name(featureSet)
    dbc_output = ''
    ecu_output = ''
    list_df = []
    list_ecu = []
    for dbc in dbcs:
        dbc_name = extract_name(dbc)
        if (len(dbcs)) > 1 & (len(dbc_name) > 30):
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
        utils_db.del_temporary_files()

    dbc_output += '_DBC'
    ecu_output += '_ECU'
    bh_c1 = pd.concat(list_df, ignore_index=True)
    ecus = pd.concat(list_ecu, ignore_index=True)
    feature_spec = pd.read_excel(featureSet, sheet_name=0, dtype=str)
    feature_spec = feature_spec.fillna('-')
    feature_spec['Feature'] = feature_spec['Feature'].apply(lambda x: x.replace(" ", ""))
    feature_spec['SubFeature'] = feature_spec['SubFeature'].apply(lambda x: x.replace('"', ""))
    feature_spec['SubFeature'] = feature_spec['SubFeature'].str.strip()
    feature_spec.columns = [c.replace(' ', '_') for c in feature_spec.columns]
    feature_spec['Architecture'] = feature_spec['Architecture'].str.lower()
    feature_spec['Architecture'] = feature_spec['Architecture'].replace(['atlmid', 'atlhigh', 'pnet', 'cusw', 'powernet'],
                                                                        ['ATLANTISMID', 'ATLANTISHIGH', 'POWERNET',
                                                                         'CUSW', 'POWERNET'])
    feature_spec['CAN_signal'] = feature_spec['CAN_signal'].str.replace(':', '.')
    feature_spec[["Msg_Name", "Signal_name"]] = feature_spec['CAN_signal'].str.split('.', 1, expand=True)
    feature_spec = feature_spec.drop(columns='CAN_signal')
    feature_spec['Msg_Name'] = feature_spec['Msg_Name'].str.strip()
    feature_spec['Signal_name'] = feature_spec['Signal_name'].str.strip()
    feature_spec['Value'] = feature_spec['Value'].str.strip()

    feature_spec = feature_spec.drop_duplicates()

    return [(feature_spec, ff_name),  (bh_c1, dbc_output), (ecus, ecu_output)] # , (dfs_choices,dbc_output+'ch')

