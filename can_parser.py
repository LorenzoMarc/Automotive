import cantools
import pandas as pd
from can import Message
from can import ASCReader
from datetime import datetime
from tqdm import tqdm
from time import time
from pathlib import PurePath
from can import BLFReader

'''
Find DBC message and info using CAN_ID in log file
extract all info from dbc and Decode signals 
'''


def main(logfile, dbcfiles):
    start_time = time()
    file = PurePath(logfile).parts[-1:][0]
    file_name = file.split('.')[0]
    ext_file = file.split('.')[1]
    file_name = file_name.replace('-', '_')
    file_name = file_name.replace(' ', '_')
    list_df = []

    # prova a usare bh_c1 df da main_logger e parsare solo 1 volta
    for dbc in dbcfiles:
        if ext_file == 'asc':
            asc_file = ASCReader(logfile)
        else:
            asc_file = BLFReader(logfile)
        timestamp = 0
        db = cantools.db.load_file(dbc)
        index = 0
        decoded_signals = {'index': [], 'Timestamp': [], 'CAN_ID': [], 'Msg_Name': [], 'Signal_name': [], 'Value': [], 'Code': []}
        try:
            # pd.DataFrame(data=map(operator.attrgetter('timestamp', 'data'), log))
            for row in tqdm(iterable=asc_file, unit=' rows', desc='Extracting signal from log...'):
                if ext_file == 'asc':
                    datetime_m = asc_file.date
                    try:
                        date_format = datetime.strptime(datetime_m, "%b %d %H:%M:%S %p %Y")
                        timestamp = datetime.timestamp(date_format)
                    except Exception as e:
                        date_format = datetime.strptime(datetime_m, "%b %d %H:%M:%S.%f %p %Y")
                        timestamp = datetime.timestamp(date_format)

                # trova data iniziale log can
                t = timestamp + row.timestamp
                data_msg = Message(data=row.data)

                data = data_msg.data
                dt_object = datetime.fromtimestamp(t)
                date_time = dt_object.strftime("%Y/%m/%d %H:%M:%S.%f")
                # --- fine ricerca data
                CAN_ID = row.arbitration_id
                try:
                    msg = db.get_message_by_frame_id(CAN_ID)
                    Msg_Name = msg.name

                    decoded = db.decode_message(Msg_Name, data, allow_truncated=True)
                    not_decoded = db.decode_message(Msg_Name, decode_choices=False, data=data, allow_truncated=True)
                    for k, v in not_decoded.items():

                        try:
                            i, dec = str(v).split('.')
                            dec = dec[:3]
                            new_v = i + '.' + dec
                            decoded_signals['Code'].append(new_v)
                        except:
                            decoded_signals['Code'].append(v)

                    for k, v in decoded.items():
                        try:
                            i, dec = str(v).split('.')
                            dec = dec[:3]
                            new_v = i + '.' + dec
                            decoded_signals['Value'].append(new_v)
                        except:
                            decoded_signals['Value'].append(v)
                        decoded_signals['Signal_name'].append(k)
                        decoded_signals['Msg_Name'].append(Msg_Name)
                        decoded_signals['CAN_ID'].append(CAN_ID)
                        decoded_signals['Timestamp'].append(date_time)
                        decoded_signals['index'].append(index + 1)
                except:
                    pass
        except EOFError:
            pass
        df = pd.DataFrame.from_records(decoded_signals, index='index')
        del decoded_signals
        list_df.append(df)

    dfs = pd.concat(list_df, ignore_index=True)

    can_output = 'can_log_' + file_name + '.csv'
    dfs.to_csv(can_output, index=True, index_label='index')
    del df

    print("---END PARSING---  %s" % (time() - start_time))
    return can_output
