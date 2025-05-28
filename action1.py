import ast
import re
from datetime import datetime
import pandas as pd

pd.options.mode.chained_assignment = None  # default='warn'
from tqdm import tqdm
from utils_db import extract_tf_name

'''
ACTION 1

Extract all the CAN signals expected
Extract all the nAPIs mapped with any CAN signals
Extract all the transfer function rule and dataType

:return Transfer Function .xlsx name
'''


def is_feature(row):
    res = pd.notna(row)
    return res


'''
RETURN TF excel file
From original TF extract signals and corresponding nAPI info for each architecture

'''


def main(file):
    data = {'architecture': [], 'Msg_Name': [], 'Signal_name': [], 'priority': [], 'CAN_ch': [], 'SR': [],
            'Features': [], 'apiKey': [], 'apiType': [], 'Transfer': [], 'GAPI_struct': [], 'CR': [],
            'GAPI_field': [], 'GAPI_type': [], 'NOTE': [], 'ftd_type': [], 'ftd_v2c_type': [], 'uom': [], 'lid': [],
            'CR_type':[], 'CR_date': [], 'impacted':[]}
    tf_file_name = extract_tf_name(file)
    tf = pd.read_excel(file, sheet_name=1)
    tf.columns = [col.upper() for col in tf.columns.tolist()]
    cols = tf.columns.tolist()
    ind_last_app = cols.index('GLOBAL API STRUCT')
    list_arch = ['POWERNET', 'ATLANTISMID', 'ATLANTISHIGH', 'CUSW']
    feature_app = cols[:ind_last_app]

    for arch in list_arch:

        data_row = ['GLOBAL API STRUCT', 'GLOBAL API FIELD', 'GLOBAL API TYPE', 'NOTE',
                    'FTD TYPE', 'FTD V2C TYPE', 'UOM', 'LID', 'NETWORKAPI.KEY',
                    'NETWORKAPI.TYPE', arch + '.SIGNAL',   arch + '.TRANSFER',
                    'CRS', 'SR']
        df_feat = tf[feature_app]
        df_row = tf[data_row]
        mask_list = df_feat.apply(lambda row: row[is_feature(row)].index, axis=1)
        df_feat['features'] = mask_list.tolist()
        d = pd.concat([df_row, df_feat['features']], axis=1)
        d = d.fillna('-')
        total_len = len(d)
        for s in tqdm(d.iterrows(), total=total_len, unit=' rows', desc='Calculating data for ' + arch):
            architecture = arch.split('.')[0]
            signals = s[1][arch + '.SIGNAL']
            sr = s[1]['SR']
            api = s[1]['NETWORKAPI.KEY']
            api_type = s[1]['NETWORKAPI.TYPE']
            transfer = s[1][arch + '.TRANSFER']
            g_api_struct = s[1]['GLOBAL API STRUCT']
            pre_feats = s[1]['features']
            cr = s[1]['CRS']
            gaf = s[1]['GLOBAL API FIELD']
            gat = s[1]['GLOBAL API TYPE']
            note = s[1]['NOTE']
            ft = s[1]['FTD TYPE']
            ftd = s[1]['FTD V2C TYPE']
            uo = s[1]['UOM']
            lid = s[1]['LID']

            # -------------CR and Date --------------
            cr_num = '-'
            type_e = '-'
            d = '-'
            impacted = '-'
            try:

                cr_num, types_date = cr.split(':', 1)
                cr_num = cr_num[2:]
                s = types_date.strip()
                s_reverse = s[::-1]
                s_reverse=s_reverse.replace(',', '-', 1)
                s = s_reverse[::-1]
                s = s.replace('=', ':')
                s = re.sub(r':\s?(?![{\[\s])([^,}]+)', r': "\1"', s)  # Add quotes to dict values
                s = re.sub(r'(\w+) :', r'"\1" :', s)  # Add quotes to dict keys

                dict_data = ast.literal_eval(s)  # Evaluate the dictionary

                if dict_data['type']:
                    type_e = dict_data['type']
                if dict_data['date']:
                    d = dict_data['date'].strip()
                    d = datetime.strptime(d, "%b %d- %Y")
                if 'impacted' in dict_data:
                    impacted = dict_data['impacted']

            except:
                pass

            feats = pre_feats.tolist()
            string_feat = ""
            for feat in feats:
                string_feat += feat + ' , '
            try:
                row = signals.split(";")
                for payload in row:
                    p = payload.split(': ', maxsplit=1)
                    priority = p[0][-1:]
                    msgs = p[1][1:-1].split(',')
                    for msg in msgs:
                        ch_msg = msg.split(":")

                        ch = ch_msg[0].strip()
                        if ch == 'C':
                            ch = 'C1'
                        try:
                            msg, sg = ch_msg[1].split('.')
                        except:
                            msg = ch
                            sg = ch
                        data['architecture'].append(architecture)
                        data['CAN_ch'].append(ch.strip())
                        data['priority'].append(priority)
                        data['Msg_Name'].append(msg)
                        data['Signal_name'].append(sg)
                        data['SR'].append(sr)
                        data['Features'].append(string_feat)
                        data['apiKey'].append(api)
                        data['apiType'].append(api_type)
                        data['Transfer'].append(transfer)
                        data['GAPI_struct'].append(g_api_struct)
                        data['CR'].append(cr_num)
                        data['GAPI_field'].append(gaf)
                        data['GAPI_type'].append(gat)
                        data['NOTE'].append(note)
                        data['ftd_type'].append(ft)
                        data['ftd_v2c_type'].append(ftd)
                        data['uom'].append(uo)
                        data['lid'].append(lid)
                        data['CR_type'].append(type_e)
                        data['CR_date'].append(d)
                        data['impacted'].append(impacted)
            except:
                data['architecture'].append(architecture)
                data['CAN_ch'].append('-')
                data['priority'].append('-')
                data['Msg_Name'].append('-')
                data['Signal_name'].append('-')
                data['SR'].append(sr)
                data['Features'].append(string_feat)
                data['apiKey'].append(api)
                data['apiType'].append(api_type)
                data['Transfer'].append(transfer)
                data['GAPI_struct'].append(g_api_struct)
                data['CR'].append(cr_num)
                data['GAPI_field'].append(gaf)
                data['GAPI_type'].append(gat)
                data['NOTE'].append(note)
                data['ftd_type'].append(ft)
                data['ftd_v2c_type'].append(ftd)
                data['uom'].append(uo)
                data['lid'].append(lid)
                data['CR_type'].append(type_e)
                data['CR_date'].append(d)
                data['impacted'].append(impacted)

    df = pd.DataFrame.from_dict(data)
    df.to_excel(tf_file_name + ".xlsx")

    return tf_file_name + ".xlsx"
