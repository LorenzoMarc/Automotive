from tkinter import *
from tkinter.ttk import *
from tkinter.filedialog import askopenfile, askopenfiles
import feature_dbc
import utils_db
from pathlib import PurePath
from reporting import create_report_feature_compliance


def main(port, db, user, psw, host):
    ws = Toplevel()
    ws.title('Feature Check')
    ws.geometry('500x400')

    global ff
    global dbc
    global architecture_selected
    global vehicle_selected
    if (port and db and user and psw and host) == "":
        port = "5432"
        db = "postgres"
        user = "postgres"
        psw = "admin"
        host = "localhost"

    OPTIONS = [
        "SELECT DBC ARCHITECTURE",
        "POWERNET",
        "ATLANTISHIGH",
        "ATLANTISMID",
        "CUSW"
    ]

    def open_file_ff():
        global ff
        file_path = askopenfile(mode='r', filetypes=[('featureIO', '*xlsx')])
        print("Feature file selected: " + str(file_path.name))
        Label(ws, text="Feature file selected: \n" + str(file_path.name), foreground='green').grid(row=10, columnspan=3,
                                                                                                   pady=10)
        if file_path is not None:
            ff = file_path.name
        else:
            print("Select a Feature file .xlsx")
            Label(ws, text="Select a valid Feature file .xlsx", foreground='red').grid(row=10, columnspan=3, pady=10)

    def open_file_dbc():
        global dbc
        file_path = askopenfiles(mode='r', filetypes=[('dbc', '*dbc')])
        if file_path is not None:
            list_dbc = []
            for i, dbc_file in enumerate(file_path):
                name = PurePath(dbc_file.name).parts[-1:][0]
                list_dbc.append(name)
                Label(ws, text="dbc file selected: \n" + str(name), foreground='green').grid(row=10 + i, columnspan=3,
                                                                                             pady=10)
            print("dbc file selected: " + str(list_dbc))
            dbc = file_path
        else:
            print("Select a DBC file .dbc")
            Label(ws, text="Select a valid DBC file .dbc", foreground='red').grid(row=10, columnspan=3, pady=10)

    def uploadFiles():
        vehicle_selected = textvehicle.get(1.0, "end-1c")
        CR_selected = textcr.get(1.0, "end-1c")
        if ff is None:
            print("Select a valid Feature file")
            Label(ws, text="Select a valid Feature file", foreground='red').grid(row=10, columnspan=3, pady=10)
        elif dbc is None:
            print("Select a valid dbc file .dbc")
            Label(ws, text="Select a valid dbc file .dbc", foreground='red').grid(row=10, columnspan=3, pady=10)
        elif vehicle_selected is None:
            print("Select vehicle project of the dbc")
            Label(ws, text="Select a valid vehicle project", foreground='red').grid(row=10, columnspan=3, pady=10)
        elif CR_selected is None:
            print("Insert CR of the dbc")
            Label(ws, text="Insert CR of the dbc", foreground='red').grid(row=10, columnspan=3, pady=10)

        architecture_selected = variable.get()
        check_valid = any(architecture_selected in arch for arch in OPTIONS[1:])
        if check_valid:
            pass
        else:
            print("Select a valid architecture")
            Label(ws, text="Select a valid architecture", foreground='red').grid(row=10, columnspan=3, pady=10)

        dbcs_file = []
        for dbc_file in dbc:
            a = dbc_file.name
            dbcs_file.append(a)
        res = feature_dbc.main(ff, dbcs_file)

        conn = utils_db.set_conn_parameters(host, port, db, user, psw)
        done = utils_db.upload_dfs(res, conn)
        done_report = create_report_feature_compliance(res, ff, dbcs_file, architecture_selected,
                                                       vehicle_selected, CR_selected)
        utils_db.del_temporary_files()
        if done and done_report:
            Label(ws, text='Feature computed and uploaded!'.upper(), foreground='green').grid(row=12, columnspan=3,
                                                                                              pady=10)
            print('Feature computed and uploaded!')
        else:
            Label(ws, text='problem', foreground='red').grid(row=10, columnspan=3, pady=10)

    ### BUTTON UI
    canbtn = Label(
        ws, text='Upload Feature file format .xlsx'
    )
    canbtn.grid(row=0, column=0, padx=10)

    excanbtn = Button(
        ws,
        text='Choose File',
        command=lambda: open_file_ff()
    )
    excanbtn.grid(row=0, column=1)

    dbcbtn = Label(
        ws,
        text='Upload DBC file '
    )
    dbcbtn.grid(row=2, column=0, padx=10)

    exdbcbtn = Button(
        ws,
        text='Choose File ',
        command=lambda: open_file_dbc()
    )
    exdbcbtn.grid(row=2, column=1)

    archlab = Label(
        ws,
        text='Select Architecture DBC'
    )
    archlab.grid(row=3, column=0, padx=10)

    variable = StringVar(ws)

    archs = OptionMenu(ws, variable, *OPTIONS)

    archs.grid(row=3, columnspan=2, column=1)

    #  vehicle
    textvehicle = Text(
        ws, width=10, height=1
    )
    textvehicle.grid(row=4, column=1)

    vehicletxt = Label(
        ws,
        text='Select vehicle db'
    )
    vehicletxt.grid(row=4, column=0, padx=10)

    #  CR
    textcr = Text(
        ws, width=10, height=1
    )
    textcr.grid(row=5, column=1)

    crtext = Label(
        ws,
        text='Insert number CR'
    )
    crtext.grid(row=5, column=0, padx=10)

    upld = Button(
        ws,
        text='Compute Files',
        command=uploadFiles
    )
    upld.grid(row=9, columnspan=3, pady=10)

    ws.mainloop()
