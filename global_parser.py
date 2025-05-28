import utils_sc
import pandas as pd
from tqdm import tqdm
import re
import os
import json
from datetime import datetime


def convert_timestamp(input_timestamp):
    # Define the input and output timestamp formats
    output_format = "%Y-%m-%d %H:%M:%S.%f"
    try:
        # da greylog
        input_format = "%Y-%m-%dT%H:%M:%S.%f"
        # Convert the input timestamp to a datetime object
        dt = datetime.strptime(input_timestamp, input_format)
    except:
        #  da splunk
        input_format = "%Y/%m/%d-%H:%M:%S.%f"
        dt = datetime.strptime(input_timestamp, input_format)
        # Convert the datetime object to the output format
    output_timestamp = dt.strftime(output_format)
    # Remove the last three digits of the microseconds part
    output_timestamp = output_timestamp[:-3]
    return output_timestamp


def main(path_file, region, greylog_flag):
    ext_file = os.path.splitext(path_file)[1]
    with open(path_file, 'r') as file:
        lines = file.readlines()
        len_file = len(lines)
        data_received = []
        data_ro_received = []
        data_send = []
        data_ro_send = []
        data_cc = []
        prvi_push = []
        prvi_res = []
        data_sqdf = []
        data_kal = {'timestamp': [], 'device_id': [], 'topic': []}
        data_ubi_qos = {'timestamp': [], 'device_id': [], 'topic': [], 'qos': [], 'userName': [],
                        'messageTimestamp': [], 'currentTimestamp': []}
        data_subs = {'timestamp': [], 'device_id': [], 'VIN': [], 'subs_topics': [], 'status': []}
        data_subs_ubi = {'timestamp': [], 'device_id': [], 'VIN': [], 'subs_topics': [], 'status': []}
        data_vf = []
        data_ubi = []
        data_cert = {'timestamp': [], 'device_id': [], 'VIN': [], 'error': []}

        API_list = [('chargeDataPublish', 'feedback'),
                    ('ackMessage', 'feedback'),
                    ('chargeDataRequest', 'command'),
                    ('updateChargeSchedules', 'command'),
                    ('remoteChargingDoorRequest', 'command'),
                    ('remoteChargePlugRequest', 'command'),
                    ('remoteChargePlugResponse', 'feedback'),
                    ('chargePowerPreference', 'command'),
                    ('chargeNowResponse', 'feedback'),
                    ('remoteOperationResponse', 'feedback'),
                    ('remoteChargingDoorResponse', 'feedback'),
                    ('remoteOperationRequest', 'command'),
                    ('chargeNowRequest', 'command'),
                    ('remoteInhibitRequest', 'command'),
                    ('remoteInhibitResponse', 'feedback'),
                    ('provisioningPush', 'command'),
                    ('initiateProvisioningPush', 'command'),
                    ('provisioningResponse', 'feedback'),
                    ('provisioningRequest', 'command'),
                    ('provisioningPushAck', 'feedback'),
                    ('clearPersonalDataResponse', 'feedback'),
                    ('crankAttemptedNotification', 'feedback'),
                    ('vehicleLocationRequest', 'command'),
                    ('vehicleLocationResponse', 'feedback'),
                    ('sqdfPublish', 'feedback'),
                    ('vehicleDataAcquisitionPolicyPublish', 'feedback'),
                    ('vehicleDataAcquisitionPublish', 'feedback')
                    ]

        for line in tqdm(lines, unit='rows', desc='Extracting data..', total=len_file):
            if len(line) == 1:
                pass
            else:
                try:
                    if greylog_flag:
                        # this line for greylog converted logs
                        line = line.replace('""', '"')
                        line = line[1:-1]

                    # if 'The Client' or 'MQTT_CLIENT_INGRESS' in line:
                    #     pass
                    if 'VehicleFinder' in line:
                        timestamp, data_plus_log = line.split("message", 1)
                        timestamp = (timestamp[0:23]).strip()
                        timestamp = convert_timestamp(timestamp)
                        data, log = data_plus_log.split("with ")
                        dict_level = {}
                        if "Sending" in line:
                            direction = '1'
                        else:
                            direction = '0'
                        device = re.search(r'\b(device: )\b', log)
                        sessionId = re.search(r'\b(sessionId : )\b', log)
                        scope = re.search(r'\b(. SCOPE)\b', log)
                        vehicle = re.search(r'\b(for vehicle: )\b', log)

                        device_id = log[device.end():vehicle.start()]
                        VIN = log[vehicle.end():scope.start()]
                        session_id = log[sessionId.end(): device.start()]

                        utils_sc.create_json_data_2(data, API_list, dict_level, device_id, VIN, session_id, timestamp,
                                                    direction, data_vf)

                    if 'Keep alive message' in line:
                        timestamp = line[:23]
                        timestamp = convert_timestamp(timestamp.strip())
                        receveid_in = line.split('received in ')[1]
                        res = []
                        for sub in receveid_in.split('for '):
                            if ':' in sub:
                                res.append(map(str.strip, sub.split(':', 1)))
                        res = dict(res)
                        data_kal['timestamp'].append(timestamp)
                        data_kal['device_id'].append(res['clientId'][:15].replace('"', ''))
                        data_kal['topic'].append('KAL')

                    if 'Sending V2C message' in line:
                        timestamp, data_plus_log = line.split("message", 1)
                        timestamp = (timestamp[:23]).strip()
                        timestamp = convert_timestamp(timestamp)

                        dict_level = {}
                        direction = '1'
                        data, log = data_plus_log.split(" with ")
                        sessionId = re.search(r'(sessionId :)', log)
                        device = re.search(r'\b(to device: )\b', log)
                        vehicle = re.search(r'\b( for vehicle: )\b', log)
                        scope = re.search(r'\b(SCOPE)\b', log)

                        session_id = log[sessionId.end(): device.start()]
                        device_id = log[device.end():vehicle.start() - 1]
                        VIN = log[vehicle.end():scope.start() - 1]
                        utils_sc.create_json_data(data, API_list, dict_level, device_id, VIN,
                                                  session_id, timestamp, data_ubi)

                    if 'received the message on Topic : fcasdp/ADA/UBI/' in line:
                        timestamp = line[:23]
                        # timestamp = convert_timestamp(timestamp.strip())
                        device = re.search('clientId:', line)
                        userName = re.search('userName:', line)
                        qos = re.search('QOS ', line)
                        msgTime = re.search('messageTimestamp from device : ', line)
                        currTs = re.search('currentTimestamp ', line)
                        topic = re.search('fcasdp/ADA/UBI/', line)

                        device_id = line[device.end():device.end() + 16]
                        userName_id = line[userName.end():userName.end() + 16]
                        qos_id = line[qos.end():msgTime.start() - 1]
                        msgTime_id = line[msgTime.end():msgTime.end() + 13]
                        currTs_id = line[currTs.end():currTs.end() + 13]
                        topic_id = line[topic.start():topic.end() + 15]

                        data_ubi_qos['timestamp'].append(timestamp)
                        data_ubi_qos['device_id'].append(device_id)
                        data_ubi_qos['topic'].append(topic_id)
                        data_ubi_qos['qos'].append(qos_id)
                        data_ubi_qos['userName'].append(userName_id)
                        data_ubi_qos['messageTimestamp'].append(msgTime_id)
                        data_ubi_qos['currentTimestamp'].append(currTs_id)

                    if 'Received RO' in line:
                        timestamp, data_plus_log = line.split("message", 1)
                        if ext_file == '.csv':
                            data_plus_log = data_plus_log.replace('""', '"')
                        timestamp = (timestamp[:23]).strip()
                        timestamp = convert_timestamp(timestamp)

                        dict_level = {}
                        direction = '0'  # TBM->SDP
                        data, log = data_plus_log.split("from")
                        device = re.search(r'\b(device: )\b', log)
                        vehicle = re.search(r'\b( for vehicle: )\b', log)
                        sessionId = re.search(r'\b(sessionId)\b', log)
                        scope = re.search(r'\b(SCOPE)\b', log)

                        device_id = log[device.end():vehicle.start()]
                        VIN = log[vehicle.end():sessionId.start()]
                        session_id = log[sessionId.end() + 2: scope.start() - 2]
                        utils_sc.create_json_data_2(data, API_list, dict_level, device_id, VIN, session_id, timestamp,
                                                    direction, data_ro_received)

                    elif 'Sending RemoteOperation' in line:
                        timestamp, data_plus_log = line.split("message", 1)
                        if ext_file == '.csv':
                            data_plus_log = data_plus_log.replace('""', '"')
                        timestamp = (timestamp[:23]).strip()
                        timestamp = convert_timestamp(timestamp.strip())
                        dict_level = {}
                        # # LOG INFO TO ADD
                        direction = '1'  # SDP->TBM

                        data, log = data_plus_log.strip().split(' ', 1)

                        log = log.split('.')[0]

                        device = re.search(r'\b(from device: )\b', log)
                        if device is None:
                            device = re.search(r'\b(device_id=)\b', log)

                        vehicle = re.search(r'\b( for vehicle: )\b', log)
                        if vehicle is None:
                            vehicle = re.search(r'\b(vehicle_id=)\b', log)
                        sessionId = re.search(r'\b(sessionId)\b', log)
                        if sessionId is None:
                            sessionId = re.search(r'\b(sessionId")\b', log)
                        device_id = log[device.end():vehicle.start()]
                        VIN = log[vehicle.end():]
                        session_id = log[sessionId.end() + 2:device.start()]

                        try:
                            utils_sc.create_json_data_2(data, API_list, dict_level, device_id, VIN, session_id,
                                                        timestamp,
                                                        direction, data_ro_send)
                        except:
                            print('Remote operation Sending missing rows')
                            print(line)

                    if 'CommCheck' in line:
                        timestamp, data_plus_log = line.split("message", 1)
                        if ext_file == '.csv':
                            data_plus_log = data_plus_log.replace('""', '"')
                        timestamp = (timestamp[0:23]).strip()
                        timestamp = convert_timestamp(timestamp)
                        # timestamp = timestamp.replace('-', ' ')
                        # timestamp = timestamp.replace('/', '-')
                        # timestamp = timestamp.replace('"', '')
                        dict_level = {}
                        vehicle = re.search(r'( VIN )', line)
                        device_id = ''
                        try:
                            VIN = line[vehicle.end():vehicle.end() + 17]
                        except:
                            print('VIN ERROR \n {}'.format(line))
                        try:
                            header, data = line.split('(hiveMqCachedVehicleId):')
                        except:
                            header, data = line.split(': ', 1)
                        try:
                            json_data, footer = data.split(' SCOPE:')
                        except:
                            print("corrupted lines")
                            print(data)
                        utils_sc.create_json_data(json_data, API_list, dict_level, device_id, VIN,
                                                  '', timestamp, data_cc)

                    # from Topics sub on Greylog (converted)
                    if 'DeviceSubscription' in line:
                        device = re.search('ClientId:', line)
                        vehicle = re.search('vehicleId:', line)
                        service = re.search('service: ', line, 1)
                        timestamp = (line[:23]).strip()
                        timestamp = convert_timestamp(timestamp)
                        device_id = line[device.end():device.end() + 16]
                        VIN = line[vehicle.end():service.start()]
                        services = line[service.start():]
                        services_array = services.split('service: ')
                        for service in services_array[1:]:
                            topic_id = re.search('fcasdp/', service, 1)
                            device_status_id = re.search('deviceStatus:', service, 1)
                            updated = re.search(', updated', service, 1)
                            topics_string = service[topic_id.end():device_status_id.start()-2].rsplit('/', 1)[1]
                            device_status = service[device_status_id.end():updated.start()]

                            data_subs_ubi['timestamp'].append(timestamp.strip())
                            data_subs_ubi['device_id'].append(device_id.strip())
                            data_subs_ubi['VIN'].append(VIN.strip())
                            data_subs_ubi['subs_topics'].append(topics_string)
                            data_subs_ubi['status'].append(device_status.strip())

                    if 'Request subscription' in line:
                        timestamp, data_plus_log = line.split("subscription", 1)
                        if ext_file == '.csv':
                            line = data_plus_log.replace('""', '"')
                        timestamp = (timestamp[:23]).strip()
                        timestamp = convert_timestamp(timestamp)
                        device = re.search('clientId: ', line)
                        vehicle = re.search(', vehicleId: ', line)
                        status = re.search('status: ', line)
                        original_source = re.search('original_source', line)
                        try:
                            device_id = line[device.end():device.end() + 16]
                        except:
                            print("DEVICE")
                            print(line)
                            print(device.end())
                            # print(vehicle.start())
                        topics_re = re.search(', topics: \\[(.*)],', line)
                        try:
                            VIN = line[vehicle.end():topics_re.span()[0]]
                        except:
                            print("VIN in subs and topics")
                            print(vehicle)
                            print(line)
                            print(topics_re.span()[0])
                        try:
                            status = line[status.end():original_source.start()]
                        except:
                            status = line[status.end():-1].strip()
                        topics_re = re.search('topics: \[(.*)],', line)
                        topics = topics_re.group(1)
                        topics = topics.split(',')
                        topics_str = ''
                        for topic in topics:

                            try:
                                top = topic.rsplit('/', 1)[1]
                                topics_str += str(top) + ','
                            except:
                                topics_str += str(top) + ','

                        data_subs['timestamp'].append(timestamp.strip())
                        data_subs['device_id'].append(device_id.strip())
                        data_subs['VIN'].append(VIN.strip())
                        data_subs['subs_topics'].append(topics_str.replace(' ', ''))
                        data_subs['status'].append(status.strip())

                    # if 'DeviceSubscription' in line:
                    #     timestamp, data_plus_log = line.split("INFO", 1)
                    #     if ext_file == '.csv':
                    #         line = data_plus_log.replace('""', '"')
                    #     timestamp = (timestamp[:23]).strip()
                    #     timestamp = convert_timestamp(timestamp)
                    #     device = re.search('ClientId: ', line)
                    #     vehicle = re.search('vehicleId: ', line)
                    #     status = re.search('deviceStatus: ', line)
                    #     update = re.search(', updated', line)
                    #     try:
                    #         device_id = line[device.end():device.end() + 15]
                    #     except:
                    #         print("DEVICE")
                    #         print(line)
                    #         print(device.end())
                    #         # print(vehicle.start())
                    #     topics_re = re.search('service: ', line)
                    #     try:
                    #         VIN = line[vehicle.end():topics_re.span()[0]]
                    #     except:
                    #         print("VIN in subs and topics")
                    #         print(vehicle)
                    #         print(line)
                    #         print(topics_re.span()[0])
                    #     try:
                    #         status = line[status.end():update.start()]
                    #     except:
                    #         status = line[status.end():-1].strip()
                    #     try:
                    #         topics = topics_re.group(1)
                    #         topics = topics.split(',')
                    #     except:
                    #         topics = topics.split(',')
                    #     topics_str = ''
                    #     for topic in topics:
                    #
                    #         try:
                    #             top = topic.rsplit('/', 1)[1]
                    #             topics_str += str(top) + ','
                    #         except:
                    #             topics_str += str(top) + ','
                    #
                    #     data_subs['timestamp'].append(timestamp.strip())
                    #     data_subs['device_id'].append(device_id.strip())
                    #     data_subs['VIN'].append(VIN.strip())
                    #     data_subs['subs_topics'].append(topics_str.replace(' ', ''))
                    #     data_subs['status'].append(status.strip())

                    # TODO

                    if 'Payload message part 1 of' in line:
                        timestamp, data_plus_log = line.split("message", 1)

                        timestamp = (timestamp[:23]).strip()
                        timestamp = convert_timestamp(timestamp.strip())
                        dict_level = {}
                        # List of possible API name with type (to keep update)


                        direction = '0'  # TBM->SDP

                        data, log = data_plus_log.split("from")
                        device = re.search(r'\b(device: )\b', log)
                        vehicle = re.search(r'\b( for vehicle: )\b', log)
                        sessionId = re.search(r'\b(sessionId)\b', log)
                        scope = re.search(r'\b(SCOPE)\b', log)
                        device_id = log[device.end():vehicle.start()]
                        VIN = log[vehicle.end():sessionId.start()]
                        session_id = log[sessionId.end() + 2: scope.start() - 2]

                        utils_sc.create_json_data_2(data, API_list, dict_level, device_id, VIN, session_id,
                                                    timestamp,
                                                    direction,
                                                    data_received)

                        # TRANSMITTING
                    else:
                        # # LOG INFO TO ADD
                        direction = '1'  # SDP->TBM

                        data, log = data_plus_log.strip().split(' ', 1)
                        log = log.split('.')[0]

                        device = re.search(r'\b(from device: )\b', log)
                        if device is None:
                            device = re.search(r'\b(device_id=)\b', log)

                        vehicle = re.search(r'\b( for vehicle: )\b', log)
                        if vehicle is None:
                            vehicle = re.search(r'\b(vehicle_id=)\b', log)
                        sessionId = re.search(r'\b(sessionId)\b', log)
                        if sessionId is None:
                            sessionId = re.search(r'\b(sessionId")\b', log)
                        device_id = log[device.end():vehicle.start()]
                        VIN = log[vehicle.end():]
                        session_id = log[sessionId.end() + 2:device.start()]

                        utils_sc.create_json_data_2(data, API_list, dict_level, device_id, VIN, session_id,
                                                    timestamp,
                                                    direction, data_send)
                    if 'PHEV V2C' in line:
                        timestamp, data_plus_log = line.split("message", 1)
                        if ext_file == '.csv':
                            data_plus_log = data_plus_log.replace('""', '"')
                        timestamp = (timestamp[:23]).strip()
                        timestamp = convert_timestamp(timestamp.strip())
                        dict_level = {}
                        # List of possible API name with type (to keep update)

                        if 'Received' in line:

                            direction = '0'  # TBM->SDP

                            data, log = data_plus_log.split("from")
                            device = re.search(r'\b(device: )\b', log)
                            vehicle = re.search(r'\b( for vehicle: )\b', log)
                            sessionId = re.search(r'\b(sessionId)\b', log)
                            scope = re.search(r'\b(SCOPE)\b', log)
                            device_id = log[device.end():vehicle.start()]
                            VIN = log[vehicle.end():sessionId.start()]
                            session_id = log[sessionId.end() + 2: scope.start() - 2]

                            utils_sc.create_json_data_2(data, API_list, dict_level, device_id, VIN, session_id,
                                                        timestamp,
                                                        direction,
                                                        data_received)

                        # TRANSMITTING
                        else:
                            # # LOG INFO TO ADD
                            direction = '1'  # SDP->TBM

                            data, log = data_plus_log.strip().split(' ', 1)
                            log = log.split('.')[0]

                            device = re.search(r'\b(from device: )\b', log)
                            if device is None:
                                device = re.search(r'\b(device_id=)\b', log)

                            vehicle = re.search(r'\b( for vehicle: )\b', log)
                            if vehicle is None:
                                vehicle = re.search(r'\b(vehicle_id=)\b', log)
                            sessionId = re.search(r'\b(sessionId)\b', log)
                            if sessionId is None:
                                sessionId = re.search(r'\b(sessionId")\b', log)
                            device_id = log[device.end():vehicle.start()]
                            VIN = log[vehicle.end():]
                            session_id = log[sessionId.end() + 2:device.start()]

                            utils_sc.create_json_data_2(data, API_list, dict_level, device_id, VIN, session_id,
                                                        timestamp,
                                                        direction, data_send)

                    if 'PRVI V2C' in line:
                        timestamp, data_plus_log = line.split("message", 1)
                        if ext_file == '.csv':
                            data_plus_log = data_plus_log.replace('""', '"')
                        timestamp = (timestamp[:23]).strip()
                        timestamp = convert_timestamp(timestamp.strip())
                        dict_level = {}
                        if 'Outgoing' in line:
                            direction = '1'  # SDP->TBM
                            log, data = data_plus_log.split('ucm=', 1)
                            json_data, footer = data.split('SCOPE:')
                            device = re.search(r'\b(targetDeviceId=Optional\[)\b', line)
                            vehicle = re.search(r'\b(vin=)\b', line)
                            device_id = line[device.end():vehicle.start()]
                            VIN = line[vehicle.end():device.start()]
                            session_id = 'NA'
                            utils_sc.create_json_data_2(json_data, API_list, dict_level, device_id, VIN, session_id,
                                                        timestamp,
                                                        direction, prvi_push)
                        else:
                            direction = '0'  # 'TBM -> SDP
                            log, data = data_plus_log.split('ucm=', 1)
                            json_data, footer = data.split('SCOPE:')
                            try:
                                device = re.search(r'sourceDeviceId=', line)
                                vehicle = re.search(r'vin=', line)
                                device_id = line[device.end():vehicle.start()]
                            except:
                                print('Error in PRVI Incoming')
                                print(line)
                            VIN = line[vehicle.end():device.start()]
                            session_id = 'NA'
                            utils_sc.create_json_data_2(json_data, API_list, dict_level, device_id[:15], VIN,
                                                        session_id,
                                                        timestamp,
                                                        direction, prvi_res)

                    if 'V2C SqdfPublish' in line:
                        timestamp, data_plus_log = line.split("Message :", 1)
                        if ext_file == '.csv':
                            data_plus_log = data_plus_log.replace('""', '"')
                        timestamp = (timestamp[:23]).strip()
                        timestamp = convert_timestamp(timestamp.strip())
                        dict_level = {}
                        end_vin = re.search(r'Message :', line)
                        vehicle = re.search(r'VehicleId :', line)
                        VIN = line[vehicle.end():end_vin.start()]
                        device_id = ''
                        session_id = ''
                        direction = '0'  # 'TBM -> SDP
                        log, data = data_plus_log.split(' SCOPE:', 1)
                        utils_sc.create_json_data_2(log, API_list, dict_level, device_id, VIN, session_id,
                                                    timestamp,
                                                    direction, data_sqdf)

                    if 'ERROR-Received cert validation' in line:
                        timestamp, data_plus_log = line.split("for serial:", 1)
                        timestamp = (timestamp[:23]).strip()
                        timestamp = convert_timestamp(timestamp)
                        end_vin = re.search(r'VIN=', line)
                        errorDesc = re.search(r'errorDesc=', line)
                        VIN = line[end_vin.end():end_vin.end() + 17]
                        device_id = data_plus_log[:16]
                        error = line[errorDesc.end():-2]
                        data_cert['timestamp'].append(timestamp.strip())
                        data_cert['device_id'].append(device_id.strip())
                        data_cert['VIN'].append(VIN.strip())
                        data_cert['error'].append(error.strip())

                except Exception as e:
                    print('General Error')
                    print(e)
                    print(line)
    file.close()
    # KAL
    df_KAL = pd.DataFrame.from_records(data_kal)
    name_kal = 'kal_log'

    # UBI QOS
    df_ubi_qos = pd.DataFrame.from_records(data_ubi_qos)
    name_ubi_qos = 'ubi_qos_log'

    #UBI subs from graylog
    df_ubi_subs = pd.DataFrame.from_records(data_subs_ubi)
    name_ubi_subs = 'ubi_subs_log'

    # UBI Policy Publish
    df_ubi = pd.DataFrame.from_records(data_ubi)
    name_ubi = 'ubi_log'

    # Cert error
    df_cert = pd.DataFrame.from_records(data_cert)
    name_cert = 'certificate_log'

    # create Prvi data
    df_prvi_push = pd.DataFrame.from_records(prvi_push)
    df_prvi_res = pd.DataFrame.from_records(prvi_res)
    # concat push e res
    df_prvi = pd.concat([df_prvi_push, df_prvi_res], axis=0, ignore_index=True)
    re_null = re.compile(pattern='\x00')
    df_prvi.replace(regex=re_null, value=' ', inplace=True)
    name_prvi = 'prvi_log'

    # Create table VFND
    df_vf = pd.DataFrame.from_records(data_vf)
    df_vf['region'] = region
    name_vf = 'vf_log'

    # Create df RO and table target

    df_received_ro = pd.DataFrame.from_records(data_ro_received)
    df_send_ro = pd.DataFrame.from_records(data_ro_send)
    logs_ro = pd.concat([df_received_ro, df_send_ro], axis=0, ignore_index=True)
    logs_ro['region'] = region
    name_ro = 'ro_log'

    # Create df PHEV and table target

    df_received = pd.DataFrame.from_records(data_received)
    df_other = pd.DataFrame.from_records(data_send)
    logs = pd.concat([df_received, df_other], axis=0, ignore_index=True)
    if logs.__len__() > 0:
        print('Calculate differences schedules')
        logs = utils_sc.diff_schedules_logs(logs)
        logs.drop(labels='schedules', axis=1, inplace=True)
        # logs['schedules'] = logs['schedules'].apply(json.dumps)
        logs['updates_schedules'] = logs['updates_schedules'].astype(str).apply(json.dumps)
        # print(logs[logs['eventTrigger'] == 'CHARGE_SCHEDULE']['schedules'])
        # print(logs.columns)
        #
        # print(logs[logs['updates_schedules'] != 'NaN'])
    name_phev = 'phev_log'

    # Create df Topics and table target

    df_subs = pd.DataFrame.from_records(data_subs)
    re_null = re.compile(pattern='\x00')
    df_subs.replace(regex=re_null, value=' ', inplace=True)
    name_sub = 'subs_topic_log'

    # df_cc = df_cc.apply(lambda s: s.decode("utf-8", errors="replace").replace("\x00", "\uFFFD"))

    # Create df CommCheck and table target
    df_cc = pd.DataFrame()
    if len(data_cc) > 0:
        df_cc = pd.DataFrame.from_records(data_cc,
                                          exclude=['msg_type', 'msg_name', 'schedules', 'sessionId', 'apiVersionId'])
        df_cc['device_id'] = df_cc['tbmSerialNum']
        re_null = re.compile(pattern='\x00')
        df_cc.replace(regex=re_null, value=' ', inplace=True)
    # df_cc = df_cc.apply(lambda s: s.decode("utf-8", errors="replace").replace("\x00", "\uFFFD"))

    df_sqdf = pd.DataFrame.from_records(data_sqdf)
    re_null = re.compile(pattern='\x00')
    df_sqdf.replace(regex=re_null, value=' ', inplace=True)
    name_sqdf = 'sqdf_log'

    name_cc = 'commcheck_log'
    results_list = [(df_subs, name_sub), (df_cc, name_cc), (logs, name_phev),
                    (df_prvi, name_prvi), (df_sqdf, name_sqdf), (df_KAL, name_kal), (logs_ro, name_ro),
                    (df_vf, name_vf), (df_cert, name_cert), (df_ubi_qos, name_ubi_qos), (df_ubi_subs, name_ubi_subs)]
    return results_list
