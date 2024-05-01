'''

UI with INPUT:
    1) CAN log file  .asc (<600 MB)
    2) DBC file .dbc (multiple file selection available)

    OUTPUT 2+  csv files:
        1. can_log_asc_file.csv: logs can in formato csv
        2. DBC_file.csv: DBC (if multiple DBC selected also merged csv of the dbc )

'''

from tkinter import *
from tkinter.ttk import *
from tkinter.filedialog import askopenfile, askopenfiles
import main_logger
import os
import utils_db
from pathlib import PurePath

asc = ''
dbc = None


def main(port, db, user, psw, host):
    ws = Toplevel()
    ws.title('CAN Analysis')
    ws.geometry('500x400')

    if (port and db and user and psw and host) == "":
        port = "5432"
        db = "postgres"
        user = "postgres"
        psw = "admin"
        host = "localhost"

    def open_file_can():
        global asc
        file_path = askopenfile(mode='r', filetypes=[('log', '*asc'), ('log', '*blf')])
        dim = os.path.getsize(file_path.name)
        if dim > 700000000:
            print("Your file is HUGE, select a smallest log file (<600 MB)")
            Label(ws, text="Your file is HUGE, select a smallest log file (<600 MB)", foreground='red').grid(row=10,
                                                                                                             columnspan=3,
                                                                                                             pady=10)
        else:
            print("Log file selected: " + str(file_path.name))
            Label(ws, text="Log file selected: \n" + str(file_path.name), foreground='green').grid(row=10, columnspan=3,
                                                                                                   pady=10)
            if file_path is not None:
                asc = file_path.name
            else:
                print("Select a log file .asc")
                Label(ws, text="Select a valid log file .asc", foreground='red').grid(row=10, columnspan=3, pady=10)

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

    '''
    Main function triggered with "Compute Files" button. Calls main module 
    of the task        
    '''

    def uploadFiles():
        if asc is None:
            print("Select a valid log file .asc")
            Label(ws, text="Select a valid log file .asc", foreground='red').grid(row=10, columnspan=3, pady=10)

        elif dbc is None:
            print("Select a valid dbc file .dbc")
            Label(ws, text="Select a valid dbc file .dbc", foreground='red').grid(row=10, columnspan=3, pady=10)

        else:
            dbcs_file = []
            for dbc_file in dbc:
                a = dbc_file.name
                dbcs_file.append(a)
            # main module function
            res, dbc_csv, ecu_path = main_logger.main(asc, dbcs_file)
            # connection to DB
            conn = utils_db.set_conn_parameters(host, port, db, user, psw)
            done = utils_db.main(res, conn, ecu_path, dbc_csv)
            utils_db.del_temporary_files()
            if done:
                Label(ws, text='CAN Mapping computed and uploaded!'.upper(), foreground='green').grid(row=12,
                                                                                                      columnspan=3,
                                                                                                      pady=10)
            else:
                Label(ws, text='problem', foreground='red').grid(row=10, columnspan=3, pady=10)

    '''
    UI
    '''

    canbtn = Label(
        ws,
        text='Upload CAN log format .asc '
    )
    canbtn.grid(row=0, column=0, padx=10)

    excanbtn = Button(
        ws,
        text='Choose File',
        command=lambda: open_file_can()
    )
    excanbtn.grid(row=0, column=1)

    dbcbtn = Label(
        ws,
        text='Upload DBC file '
    )
    dbcbtn.grid(row=1, column=0, padx=10)

    exdbcbtn = Button(
        ws,
        text='Choose File ',
        command=lambda: open_file_dbc()
    )
    exdbcbtn.grid(row=1, column=1)

    upld = Button(
        ws,
        text='Compute Files',
        command=uploadFiles
    )
    upld.grid(row=9, columnspan=3, pady=10)

    ws.mainloop()


if __name__ == '__main__':
    main(port="5432",
         db="postgres",
         user="postgres",
         psw="admin",
         host="localhost")

