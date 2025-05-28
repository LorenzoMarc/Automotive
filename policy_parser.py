import pandas as pd
import json
import utils_db as db
from sqlalchemy import types


def upload_policy(df_policy, name):
    try:
        engine = db.set_conn_parameters(port="5432",
                                        db="postgres",
                                        user="postgres",
                                        psw="admin",
                                        host="localhost")

        df_policy.to_sql(name.upper() + '_policy', engine, schema="FcaData", if_exists='append',
                         dtype=types.VARCHAR())

        return True
    except TypeError as e:
        print('Error uploading policy')
        print(e)


def extract_params_and_sources(data, i):
    result = []
    for item in data:
        if isinstance(item, dict):
            param = item.get("param")
            source = item.get("source")
            if param is not None and source is not None:
                result.append({"param": param, "source": source, "plan_num": i})
            for key, value in item.items():
                if isinstance(value, list):
                    result.extend(extract_params_and_sources(value, i))
                elif isinstance(value, dict):
                    result.extend(extract_params_and_sources([value], i))
    return result


def main(paths, name_table):
    df_policies = []
    for path in paths:
        file_object = open(path)
        policy = json.load(file_object)
        name = db.extract_name(path)
        plans = policy["plans"]
        data_plans = []
        for plan in plans:
            normalize_plan = pd.json_normalize(plan)["data"]
            data_plans.append(normalize_plan)
        df_data_plans = pd.concat(data_plans, ignore_index=True)

        pname = policy["pname"]
        signature = policy["signature"]

        filtered_data = []

        for i, plan_level in enumerate(df_data_plans):
            filtered_data.append(extract_params_and_sources(plan_level, i))
        res = {"param": [], "source": [], "plan_num": [], 'signature':[], "pname": []}
        for json_l in filtered_data:
            for json_dat in json_l:
                res["param"].append(json_dat["param"])
                res["source"].append(json_dat["source"])
                res["plan_num"].append(json_dat["plan_num"])
                res["signature"].append(signature)
                res["pname"].append(pname)
        data_df = pd.DataFrame.from_records(res)
        # data_df = pd.DataFrame.from_records(plan[0][0]['data'])
        # data_df['pname'] = pname[0]
        # data_df['signature'] = signature[0]
        data_df["policy_name"] = name
        data_df = data_df.astype(str)

        # data_df['pname'] = data_df['pname'].apply(normalize_json.pname)
        # print(data_df)
        df_policies.append(data_df)
    result = pd.concat(df_policies, ignore_index=True)
    result.drop_duplicates(inplace=True)

    done = upload_policy(result, name_table)
    return done
