import os
from weasyprint import HTML, CSS
from jinja2 import Environment, FileSystemLoader
import numpy as np
import pandas as pd
from datetime import datetime
import hashlib
from utils_db import extract_name


def compliance_check(f, dbc, arch, vehicle):
    print("Creating report for compliance check N.6")
    # find signals in dbc requested by features
    join = pd.merge(f, dbc, on=['Msg_Name', 'Signal_name'], how='left')
    join.fillna('-', inplace=True)
    # filtering for architecture selected
    join = join[join['Architecture'] == arch]
    join['Vehicle'] = join['Vehicle'].str.upper()
    # filtering for vehicle
    join = join[join['Vehicle'].str.contains(vehicle.upper())]

    # Set False on feature signals not present on dbc
    join['Compliance'] = np.where(join.CAN_ID == '-', 'Missing in DBC', True)
    join['choices'] = join.choices.str.replace('{}', '-', regex=True)

    tbm_compliant = []
    compliance_value = []
    value_in_dbc = []

    for i, row in join.iterrows():
        receiver = row['Receiver']
        sender = row['Sender']
        tx_rx = row['TX\\RX']
        # check if TBM is correctly set as Transmitter or Receiver
        if (tx_rx == 'RX' and 'TBM' in receiver) or (tx_rx == 'TX' and 'TBM' in sender):
            tbm_compliant.append('True')
        else:
            tbm_compliant.append('TX/RX Error')
    join['TBM_compliance'] = tbm_compliant
    join['Value'] = join['Value'].str.split(',')
    join = join.explode('Value')

    for i, row in join.iterrows():
        value = row['Value'].upper().strip()
        choices = row['choices'].upper()

        # check if value required by feature is a choice
        if value in choices or value == '-':
            compliance_value.append('True')
        else:
            compliance_value.append('Missing Req. Value')
            value_in_dbc.append(choices)

    join['Value_compliance'] = compliance_value
    tx_rx_error = join.query('Value_compliance == "True" and TBM_compliance == "TX/RX Error"')
    tx_rx_error = tx_rx_error.drop(columns='Value')
    tx_rx_error = tx_rx_error.drop_duplicates()
    join = join.query('Value_compliance =="Missing Req. Value"')
    rejoin = pd.concat([join, tx_rx_error], ignore_index=True)

    tab_compliant = rejoin[
        (rejoin['Value_compliance'] == 'Missing Req. Value') | (rejoin['TBM_compliance'] == 'TX/RX Error')]
    tab_compliant = tab_compliant[["Feature",
                                   "SubFeature",
                                   "Msg_Name",
                                   "Signal_name",
                                   "TX\\RX",
                                   "Receiver",
                                   "Sender",
                                   "Value",
                                   "choices",
                                   "Description",
                                   "Compliance",
                                   "Value_compliance",
                                   "TBM_compliance"]]

    tab_compliant.drop_duplicates(inplace=True)

    return tab_compliant


def report_excel(report_html, df_compliance, hash, arch, vehicle, dbcs, CR):
    df_compliance = df_compliance.fillna('-')

    df_missing_dbc = df_compliance.query('Compliance == "Missing in DBC"')
    df_missing_dbc = df_missing_dbc[['Feature', 'SubFeature', 'Msg_Name', 'Signal_name']]

    df_value_compliance = df_compliance.query('Compliance == "True" and Value_compliance =="Missing Req. Value"')
    df_value_compliance = df_value_compliance[['Feature', 'SubFeature', 'Msg_Name', 'Signal_name', 'Value', 'choices']]

    df_tbm_compliance = df_compliance.query('Compliance == "True" and TBM_compliance == "TX/RX Error"')
    df_tbm_compliance = df_tbm_compliance[
        ['Feature', 'SubFeature', 'Msg_Name', 'Signal_name', 'Receiver', 'Sender']]
    name_file = report_html.split('.')[0]
    # Set up an ExcelWriter
    data_df = pd.DataFrame([hash, arch, vehicle, str(dbcs), CR])
    with pd.ExcelWriter(name_file + '.xlsx', engine='openpyxl') as writer:
        # Export data
        df_missing_dbc.to_excel(writer, sheet_name='Missing in DBC')
        df_tbm_compliance.to_excel(writer, sheet_name='RXTX Error')
        df_value_compliance.to_excel(writer, sheet_name='Value Check')
        data_df.to_excel(writer, sheet_name='meta')


def report_html_template(df_compliance, hash, arch, vehicle, feature_file_path, dbcs, CR):

    feature_file = extract_name(feature_file_path)
    df_compliance = df_compliance.fillna('-')

    df_missing_dbc = df_compliance.query('Compliance == "Missing in DBC"')
    df_missing_dbc = df_missing_dbc[['Feature', 'SubFeature', 'Msg_Name', 'Signal_name']]
    if len(df_missing_dbc) != 0:
        dbc_compliant = '<h4 style="color:red;"> DBC NOT Compliant <span>&#10060;</span></h4>'
    else:
        dbc_compliant = '<h4 style="color:#009879;"> DBC Compliant <span>&#10003;</span></h4>'

    df_value_compliance = df_compliance.query('Compliance == "True" and Value_compliance =="Missing Req. Value"')
    df_value_compliance = df_value_compliance[['Feature', 'SubFeature', 'Msg_Name', 'Signal_name', 'Value']]
    if len(df_value_compliance) != 0:
        value_compliant = '<h4 style="color:red;"> Value NOT Compliant <span>&#10060;</span></h4>'
    else:
        value_compliant = '<h4 style="color:#009879;"> Value Compliant <span>&#10003;</span></h4>'

    df_tbm_compliance = df_compliance.query('Compliance == "True" and TBM_compliance == "TX/RX Error"')
    df_tbm_compliance = df_tbm_compliance[
        ['Feature', 'SubFeature', 'Msg_Name', 'Signal_name', 'Receiver', 'Sender', 'TX\\RX']]
    if len(df_tbm_compliance) != 0:
        tbm_compliant = '<h4 style="color:red;"> TBM NOT Compliant <span>&#10060;</span></h4>'
    else:
        tbm_compliant = '<h4 style="color:#009879;"> TBM Compliant <span>&#10003;</span></h4>'

    # 2. Create a template Environment
    env = Environment(loader=FileSystemLoader('template'))

    # 3. Load the template from the Environment
    template = env.get_template('report_template.html')
    now = datetime.now()
    string_date = now.strftime("%B %d, %Y")
    time_to_title = now.strftime("%Y_%m_%d")
    ts_title = now.timestamp()

    # 4. Render the template with variables
    html = template.render(page_title_text='Compliance Check',
                           title_text='Compliance report DBC and TBM Features',
                           date=string_date,
                           timestamp=ts_title,
                           hash='Report ID: ' + hash,
                           architecture='Architecture: ' + arch,
                           vehicle_project='Vehicle Project: ' + vehicle,
                           dbcs='DBC(s) selected: ' + str(dbcs),  # its a list,
                           CR = 'CR number: ' + str(CR),
                           missing_sig_section='Missing Signals in DBC',
                           TBM_feature_mapping=feature_file,
                           value_check_section='Mismatching values for signals',
                           df_tbm_compliance=df_tbm_compliance.reset_index(),
                           df_value_compliance=df_value_compliance.reset_index(),
                           compliant_tbm_section='Signals non compliant with TBM ',
                           missing_df=df_missing_dbc.reset_index(),
                           foot='This is a report autogenerated with tool developed by TBM Team',
                           dbc_compliant = dbc_compliant,
                           value_compliant = value_compliant,
                           tbm_compliant = tbm_compliant
                           )

    # 5. Write the template to an HTML file
    name_report = hash + '_' + time_to_title + '.html'
    with open(name_report, 'w') as f:
        f.write(html)
    return name_report


def html_to_pdf(report_html):
    os.add_dll_directory(r"C:\Program Files\GTK3-Runtime Win64\bin")
    css_config ='''@page {size: A4; margin: 1cm; }
        .prologue {padding-top: 40px;padding-bottom: 30px;}
        table {border-collapse: collapse; margin: 25px 0; width: 100%;font-family: sans-serif;font-size: 10px;
        box-shadow: 0 0 20px rgba(0, 0, 0, 0.15);}
        p {font-size: 10px;font-family: sans-serif;}
        ol {font-size: 10px;font-family: sans-serif;}
        thead tr {
                background-color: #009879;
                    color: #ffffff;
                    text-align: left;
            }
        th, td {border: 1px solid black;overflow-wrap: break-word; word-wrap: break-word;}
        h2 {color: red;font-family: sans-serif; font-size: 13px}
        '''

    css = CSS(string=css_config)
    name_file = report_html.split('.')[0]
    HTML(report_html).write_pdf(name_file + '.pdf', stylesheets=[css])


def delete_html_gen(html_file):
    if os.path.exists(html_file):
        os.remove(html_file)
    else:
        print(html_file + " does not exist")


def create_report_feature_compliance(data_array, feature_file, dbcs, arch, vehicle, CR):
    feature, dbc_df, ecu = data_array
    complete_hash = ""
    # extract name dbcs from list and create a unique hash ID
    dbcs_list = [dbc.split('/')[-1:] for dbc in dbcs]
    dbcs_file = [item for sublist in dbcs_list for item in sublist]
    for name_dbc in dbcs_file:
        hash_file = hashlib.md5(name_dbc.encode())
        complete_hash += hash_file.hexdigest()
    print(f'Architecture: {arch}\n'
          f'Vehicle Project: {vehicle}\n'
          f'ID Hash: {complete_hash}')
    # CC-6 with dataframe of DBCs
    df_compliance = compliance_check(feature[0], dbc_df[0], arch, vehicle)

    # save table results as png image to import in pdf report

    html_file = report_html_template(df_compliance, complete_hash, arch, vehicle, feature_file, dbcs_file, CR)
    html_to_pdf(html_file)
    report_excel(html_file, df_compliance, complete_hash, arch, vehicle, dbcs_file, CR)
    delete_html_gen(html_file)
    return True
