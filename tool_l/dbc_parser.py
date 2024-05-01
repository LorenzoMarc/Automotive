import re
import cantools
import os
import pandas as pd
from tqdm import tqdm
from time import time
import utils_db


def main(dbcfile):
    dict_data = {'Signal_name': [],
                 'Start_bit': [],
                 'Length_bit': [],
                 'Modality': [],
                 'Scale': [],
                 'Offset': [],
                 'Min': [],
                 'Max': [],
                 'Unit': [],
                 'Receiver': [],
                 'Msg_Name': [],
                 'CAN_ID': [],
                 'Length_Data': [],
                 'Sender': [],
                 'choices': [],
                 'choices_value': [],
                 'bus': []
                 }
    nodes_data = {'ECU': []}
    file_name = utils_db.extract_name(dbcfile)
    start_time = time()

    path_dbc = os.path.join(dbcfile)
    db = cantools.db.load_file(path_dbc, strict=False)
    bus = db.buses
    if not bus:
        try:
            bus = re.findall('BH', file_name)[0]
        except:
            pass
        try:
            bus = re.findall('C', file_name)[0]
        except:
            pass
    else:
        bus = bus[0].name
    messages = db.messages
    nodes = db.nodes
    for node in nodes:
        name = node.name
        nodes_data['ECU'].append(name)

    len_msg = len(messages)

    for msg in tqdm(messages, total=len_msg):
        sigs = msg.signals
        sender = msg.senders
        dlc = msg.length
        can_id = msg.frame_id
        msg_name = msg.name

        for sig in sigs:
            name = sig.name
            receivers = sig.receivers
            unit = sig.unit
            start = sig.start
            scale = sig.scale
            offset = sig.offset
            byte_order = sig.byte_order
            length_bit = sig.length
            min = sig.minimum
            max = sig.maximum

            try:
                choices = dict(sig.choices)
                choices_value = ",".join(str(v) for k, v in choices.items())
            except:
                choices = {}
                choices_value = []
            #if ("TBM" in sender) | ("TBM" in receivers):

            dict_data['Signal_name'].append(name)
            dict_data['Start_bit'].append(start)
            dict_data['Length_bit'].append(length_bit)
            dict_data['Modality'].append(byte_order)
            dict_data['Scale'].append(scale)
            dict_data['Offset'].append(offset)
            dict_data['Min'].append(min)
            dict_data['Max'].append(max)
            dict_data['Unit'].append(unit)
            dict_data['Receiver'].append(receivers)
            dict_data['Msg_Name'].append(msg_name)
            dict_data['CAN_ID'].append(can_id)
            dict_data['Length_Data'].append(dlc)
            dict_data['Sender'].append(sender)
            dict_data['choices'].append(choices)
            dict_data['choices_value'].append(choices_value)
            dict_data['bus'].append(bus)

    df = pd.DataFrame.from_records(dict_data)
    df_node = pd.DataFrame.from_records(nodes_data)
    del dict_data
    del nodes_data

    ## CHECk if dbc name starts with number to inhibit with chars
    if file_name[0].isnumeric():
        file_name = 'a'+file_name
    dbc_path = file_name + '_DBC.csv'
    ecu_path = file_name + '_ECU.csv'
    df.to_csv(dbc_path, index=False)
    df_node.to_csv(ecu_path, index=False)
    print("---DBC PARSING: %s seconds ---" % (time() - start_time))
    del df
    return dbc_path, ecu_path

