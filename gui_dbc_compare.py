from pathlib import PurePath
from tkinter import *
from tkinter.ttk import *
from tkinter.filedialog import askopenfile, askopenfiles
import utils_db
import main_dbc_compare


def main(port, db, user, psw, host):

    ws = Toplevel()
    ws.title('DBC Compare')
    ws.geometry('500x400')

    global dbcs

    if (port and db and user and psw and host) == "":
        port = "5432"
        db = "postgres"
        user = "postgres"
        psw = "admin"
        host = "localhost"
    dbcs = []

    def open_file_dbc():
        list_dbc = []
        file_path = askopenfiles(mode='r', filetypes=[('dbc', '*dbc')])
        if file_path is not None:

            for i, dbc_file in enumerate(file_path):
                name = PurePath(dbc_file.name)
                list_dbc.append(name)
                Label(ws, text="dbc file selected: \n" + str(name), foreground='green').grid(row=10 + i, columnspan=3,
                                                                                             pady=10)
            print("dbc file selected: " + str(list_dbc))
            # dbc = file_path
        else:
            print("Select a DBC file .dbc")
            Label(ws, text="Select a valid DBC file .dbc", foreground='red').grid(row=10, columnspan=3, pady=10)
        dbcs.append(list_dbc)
        #     dbc = file_path
        #     dbcs.append(dbc)
        # else:
        #     print("Select a DBC file .dbc")
        #     Label(ws, text="Select a valid DBC file .dbc", foreground='red').grid(row=10, columnspan=3, pady=10)

    def uploadFiles():
        for i, dbc in enumerate(dbcs):
            if dbc is None:
                print("Select a valid dbc file .dbc")
                Label(ws, text="Select a valid dbc file .dbc", foreground='red').grid(row=10, columnspan=3, pady=10)
            # else:
            #     name = PurePath(dbc.name).stem
            #     print(f"dbc {i} selected: {name}")
            #     Label(ws, text="dbc file selected: \n" + str(name), foreground='green').grid(row=10+int(i),
            #                                                                                  columnspan=3,
            #                                                                                  pady=10)

        res = main_dbc_compare.main(dbcs)
        conn = utils_db.set_conn_parameters(host,port, db, user, psw)
        done = utils_db.upload_dfs(res, conn)
        utils_db.del_temporary_files()
        if done:
            Label(ws, text='DBCs uploaded!'.upper(), foreground='green').grid(row=12, columnspan=3,
                                                                                         pady=10)
            print('DBCs uploaded!')
        else:
            Label(ws, text='problem', foreground='red').grid(row=10, columnspan=3, pady=10)

    ### BUTTON UI

    dbcbtn = Label(
        ws,
        text='OLD DBC files '
    )
    dbcbtn.grid(row=2, column=0, padx=10)

    exdbcbtn = Button(
        ws,
        text='Choose File ',
        command=lambda: open_file_dbc()
    )
    exdbcbtn.grid(row=2, column=1)

    dbcbtn_new = Label(
        ws,
        text='NEW DBC files '
    )
    dbcbtn_new.grid(row=3, column=0, padx=10)

    exdbcbtn_new = Button(
        ws,
        text='Choose File ',
        command=lambda: open_file_dbc()
    )
    exdbcbtn_new.grid(row=3, column=1)

    upld = Button(
        ws,
        text='Compute Files',
        command=uploadFiles
    )
    upld.grid(row=4, columnspan=3, pady=10)

    ws.mainloop()
