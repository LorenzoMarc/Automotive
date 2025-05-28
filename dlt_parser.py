import re
import pandas as pd
from pydlt import DltFileReader, DltMessage
from tqdm import tqdm
import utils_db


def main(dlt_file_path):
    filename = utils_db.extract_name(dlt_file_path)
    filename = filename + '_dlt'
    # Initialize a DltFile object to parse the .dlt file
    dlt_file = DltFileReader(dlt_file_path)
    # Create empty lists to store parsed data
    timestamps = []
    ecu_ids = []
    app_ids = []
    keys = []
    napis = []
    values = []
    validities = []
    messages = dlt_file.read_messages()
    # Iterate through DLT messages in the file
    for msg in tqdm(iterable=messages, unit=' rows', desc='Extracting signal from log...', total=20):
        if isinstance(msg, DltMessage):
            payload_raw = str(msg.payload)
            if 'NTW_' in payload_raw:
                try:
                    prekey, postkey = payload_raw.split('key:  ')
                    init_napi = re.search(r'<', postkey)
                    key = postkey[0: init_napi.start()]
                    end_napi = re.search(r'>  value', postkey)
                    napi = postkey[init_napi.start(): end_napi.end()]
                    validity = re.search(r'validity:', postkey)
                    value = postkey[end_napi.start(): validity.end()]
                    validity = postkey[validity.end():]
                    init_value = value.index('[')
                    end_value = value.index(']')
                    keys.append(key)
                    napis.append(napi[1:-8])
                    values.append(value[init_value + 2: end_value])
                    validities.append(validity.strip())
                    timestamps.append(msg.str_header.seconds)
                    ecu_ids.append(msg.str_header.ecu_id)
                    app_ids.append(msg.ext_header.application_id)
                except:
                    keys.append('-')
                    napis.append('-')
                    values.append('-1')
                    validities.append('-')
                    timestamps.append(msg.str_header.seconds)
                    ecu_ids.append(msg.str_header.ecu_id)
                    app_ids.append(msg.ext_header.application_id)
            elif 'NTW_' not in payload_raw:
                try:
                    last_data = payload_raw.split('|')[-1:][0]
                    param, value = last_data.split(',')
                    napi = 'NTW_RX_SIGNAL_' + param.split('= ')[1]
                    napis.append(napi)
                    values.append(value.strip())
                    validities.append('-')
                    timestamps.append(msg.str_header.seconds)
                    ecu_ids.append(msg.str_header.ecu_id)
                    app_ids.append(msg.ext_header.application_id)
                except:
                    pass
            else:
                pass

    data = {
        'Timestamp': timestamps,
        'ECU_ID': ecu_ids,
        'App_ID': app_ids,
        'napis': napis,
        'values': values,
        'validities': validities,
    }
    df = pd.DataFrame(data, dtype="string[pyarrow]").fillna('-1')
    df.to_excel('results.xlsx')
    exit()

    return [(df, filename)]
