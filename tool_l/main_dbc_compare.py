import pandas as pd
import utils_db
import dbc_parser


def main(list_dbcs):
    list_res = []

    # [ [dbc1_1, dbc1_2], [ dbc2_1, dbc2_2] ]
    for dbcs in list_dbcs:
        list_df = []
        list_ecu = []
        dbc_output = ''
        ecu_output = ''
        for dbc in dbcs:
            # dbc1_1, dbc1_2

            dbc_name = utils_db.extract_name(dbc)
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
        bh_c1 = pd.concat(list_df, ignore_index=True).drop_duplicates()
        ecus = pd.concat(list_ecu, ignore_index=True).drop_duplicates()
        #db_utils.del_temporary_files()
        dbc_output += '_DBC'
        ecu_output += '_ECU'
        list_res.append((bh_c1, dbc_output))
        list_res.append((ecus, ecu_output))
    return list_res
