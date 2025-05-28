import ast
import json
from deepdiff import DeepDiff
import pandas as pd
import base64


def extract_prvi(json):
    drm_json = json['provisioningPush']['drm']
    to_dec = drm_json['drmFile']
    cx = bytes(to_dec, 'ISO-8859-1')
    first_decode = base64.decodebytes(cx)
    second_decode = base64.decodebytes(first_decode)
    drm_decoded = second_decode.decode('ISO-8859-1')
    data_prvi = {'drm_file': drm_decoded, 'contentType': drm_json['contentType'], 'drmUUID': drm_json['drmUUID']}
    return data_prvi


def create_json_data_2(data, API_list, dict_level, device_id, VIN, session_id, timestamp, direction, list_data):
    json_data = json.loads(data)
    messages = json_data.pop('messages')[0]
    json_data = check_type_msg(API_list, messages, json_data)
    if 'provisioningPush' in messages.keys():
        res_dict_level = extract_prvi(messages)
    else:
        res_dict_level = iterate_json(messages, dict_level)
    json_data['device_id'] = device_id.strip()
    json_data['VIN'] = VIN.strip()
    json_data['session_id'] = session_id.strip()
    json_data['log_ts'] = timestamp.strip()
    json_data['direction'] = direction.strip()

    dict_schedules = {}
    schedules = []

    if 'chargeSchedulesV2' in res_dict_level.keys() and len(res_dict_level['chargeSchedulesV2']) != 0:
        json_data['scheduleVersion'] = 'V2'
        schedules = res_dict_level.pop('chargeSchedulesV2')
    elif 'chargeSchedulesV3' in res_dict_level.keys() and len(res_dict_level['chargeSchedulesV3']) != 0:
        json_data['scheduleVersion'] = 'V3'
        schedules = res_dict_level.pop('chargeSchedulesV3')
    elif 'chargeSchedulesV4' in res_dict_level.keys() and len(res_dict_level['chargeSchedulesV4']) != 0:
        json_data['scheduleVersion'] = 'V4'
        schedules = res_dict_level.pop('chargeSchedulesV4')
    elif 'updatedChargeSchedules' in res_dict_level.keys() and len(res_dict_level['updatedChargeSchedules']) != 0:
        json_data['scheduleVersion'] = 'V1'
        schedules = res_dict_level.pop('updatedChargeSchedules')
    elif 'updateChargeShedulesV2' in res_dict_level.keys() and len(res_dict_level['updateChargeShedulesV2']) != 0:
        json_data['scheduleVersion'] = 'V2'
        schedules = res_dict_level.pop('updateChargeShedulesV2')
    elif 'updateChargeShedulesV3' in res_dict_level.keys() and len(res_dict_level['updateChargeShedulesV3']) != 0:
        json_data['scheduleVersion'] = 'V3'
        schedules = res_dict_level.pop('updateChargeShedulesV3')
    elif 'updateChargeShedulesV4' in res_dict_level.keys() and len(res_dict_level['updateChargeShedulesV4']) != 0:
        json_data['scheduleVersion'] = 'V4'
        schedules = res_dict_level.pop('updateChargeShedulesV4')
    else:
        pass
    res = json_data | res_dict_level | {'schedules': schedules}
    # if res['msg_name'] == 'chargeDataPublish':
    #     print(res)
    #     exit()
    list_data.append(res)

    # if len(schedules) != 0:
    #
    #     for schedule in schedules:
    #         dict_schedules = iterate_base_schedules(schedule, dict_schedules)
    #         res = dict_schedules | json_data | res_dict_level | {'schedules': schedules}
    #         list_data.append(res)


def create_json_data(data, API_list, dict_level, device_id, VIN, session_id, timestamp, list_data):
    json_data = json.loads(data)
    messages = json_data.pop('messages')[0]
    json_data = check_type_msg(API_list, messages, json_data)
    res_dict_level = iterate_json(messages, dict_level)

    json_data['device_id'] = device_id.strip()
    json_data['VIN'] = VIN.strip()
    json_data['session_id'] = session_id.strip()
    json_data['log_ts'] = timestamp.strip()
    dict_schedules = {}
    schedules = []

    if 'chargeSchedulesV2' in res_dict_level.keys() and len(res_dict_level['chargeSchedulesV2']) != 0:
        json_data['scheduleVersion'] = 'V2'
        schedules = res_dict_level.pop('chargeSchedulesV2')
    elif 'chargeSchedulesV3' in res_dict_level.keys() and len(res_dict_level['chargeSchedulesV3']) != 0:
        json_data['scheduleVersion'] = 'V3'
        schedules = res_dict_level.pop('chargeSchedulesV3')
    elif 'chargeSchedulesV4' in res_dict_level.keys() and len(res_dict_level['chargeSchedulesV4']) != 0:
        json_data['scheduleVersion'] = 'V4'
        schedules = res_dict_level.pop('chargeSchedulesV4')
    elif 'updatedChargeSchedules' in res_dict_level.keys() and len(res_dict_level['updatedChargeSchedules']) != 0:
        json_data['scheduleVersion'] = 'V1'
        schedules = res_dict_level.pop('updatedChargeSchedules')
    elif 'updateChargeShedulesV2' in res_dict_level.keys() and len(res_dict_level['updateChargeShedulesV2']) != 0:
        json_data['scheduleVersion'] = 'V2'
        schedules = res_dict_level.pop('updateChargeShedulesV2')
    elif 'updateChargeShedulesV3' in res_dict_level.keys() and len(res_dict_level['updateChargeShedulesV3']) != 0:
        json_data['scheduleVersion'] = 'V3'
        schedules = res_dict_level.pop('updateChargeShedulesV3')
    elif 'updateChargeShedulesV4' in res_dict_level.keys() and len(res_dict_level['updateChargeShedulesV4']) != 0:
        json_data['scheduleVersion'] = 'V4'
        schedules = res_dict_level.pop('updateChargeShedulesV4')
    else:
        res = json_data | res_dict_level | {'schedules': schedules}
        list_data.append(res)

    if len(schedules) != 0:

        for schedule in schedules:
            dict_schedules = iterate_base_schedules(schedule, dict_schedules)
            res = dict_schedules | json_data | res_dict_level | {'schedules': schedules}
            list_data.append(res)


def check_type_msg(API_list, messages, json_data):
    json_data['msg_type'] = 'Unknown'
    json_data['msg_name'] = 'UNK'
    for api, api_type in API_list:
        if api in messages.keys():
            json_data['msg_type'] = api_type
            json_data['msg_name'] = api
            break
        else:
            pass
    return json_data


def iterate_json(entry, dict_level):
    for key, value in entry.items():
        if type(value) == dict:
            iterate_json(value, dict_level)
        else:
            dict_level.update({key: value})
    return dict_level


def iterate_schedules(schedule, i, desc, dict_schedules):
    for k, v in schedule.items():
        if type(v) == dict:
            iterate_schedules(v, i, desc, dict_schedules)
        else:
            dict_schedules.update({str(k) + '_' + str(i) + '_' + desc: v})
    return dict_schedules


def iterate_base_schedules(schedule, dict_schedules):
    for k, v in schedule.items():
        if type(v) == dict:
            iterate_base_schedules(v, dict_schedules)
        else:
            dict_schedules.update({str(k): v})
    return dict_schedules


'''
Merge received and sent logs
find sessions and schedule change request, 
return sessions with schedule and differences with previous session
'''


def diff_schedules_logs(logs):
    updateChargeSchedule = logs.query("msg_name == 'updateChargeSchedules'").astype(str)
    logs = logs[logs['msg_name'] != 'updateChargeSchedules']
    logs['updates_schedules'] = ''
    chargeDataPublish = logs.query("msg_name == 'chargeDataPublish'").astype(str)
    # find the most recent chargeDataPublish responding success happening before the updateChargeSchedule
    # and save it as old_schedule in row update charge schedule,
    # finding the difference with the schedule in updateChargeSchedules
    # in grafana: if success session in updateChargeSchedule then report the column 'updates_schedules'

    #updateChargeSchedule['log_ts'] = pd.to_datetime(updateChargeSchedule['log_ts'], format="%Y-%m-%d %H:%M:%S.%f")
    #chargeDataPublish['log_ts'] = pd.to_datetime(chargeDataPublish['log_ts'], format="%Y-%m-%d %H:%M:%S.%f")
    list_diff = []
    for i, row in updateChargeSchedule.iterrows():
        new_sched = row['schedules']
        try:
            most_recent_chargePub = chargeDataPublish[chargeDataPublish['log_ts'] < row['log_ts']].max()
            if most_recent_chargePub['response'] == 'SUCCESS':
                old_sched = most_recent_chargePub['schedules']
                new_list_sched = ast.literal_eval(new_sched)
                old_list_sched = ast.literal_eval(old_sched)
                diff = DeepDiff(old_list_sched, new_list_sched)

                # print('This is the old: \n {} \n This is the new: \n {} \n This is the update \n {} \n'.format(old_list_sched[i], list_sched[i], diff))
                list_diff.append(diff)
            else:
                list_diff.append('')
        except ValueError:
            empty_dict = {'values_changed': []}
            list_diff.append(empty_dict)

    updateChargeSchedule['updates_schedules'] = list_diff

    results = pd.concat([logs, updateChargeSchedule], ignore_index=True)
    return results
