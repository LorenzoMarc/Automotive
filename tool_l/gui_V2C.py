from tkinter import *
from tkinter.ttk import *
from tkinter.filedialog import askopenfile
import utils_db
import global_parser


def main(port, db, user, psw, host):
    ws = Toplevel()
    ws.title('V2C Log Reader')
    ws.geometry('500x400')

    global log
    if (port and db and user and psw and host) == "":
        port = "5432"
        db = "postgres"
        user = "postgres"
        psw = "admin"
        host = "localhost"

    OPTIONS = [
        "SELECT REGION ",
        "EMEA",
        "NA",
        "LATAM",
    ]

    def open_file_log():
        global log
        file_path = askopenfile(mode='r', filetypes=[('logs', '*txt'), ('csv', '*csv')])
        print("Log file selected: " + str(file_path.name))
        Label(ws, text="Log file selected: \n" + str(file_path.name), foreground='green').grid(row=10, columnspan=3,
                                                                                               pady=10)
        if file_path is not None:
            log = file_path.name
        else:
            print("Select a log file .txt")
            Label(ws, text="Select a valid log file .txt", foreground='red').grid(row=10, columnspan=3, pady=10)

    def uploadFiles():
        region_selected = variable.get()
        greylog_flag = greylog.get()

        if log is None:
            print("Select a valid log file")
            Label(ws, text="Select a valid log file ", foreground='red').grid(row=10, columnspan=3, pady=10)
        else:
            res = global_parser.main(log, region_selected, greylog_flag)
            # for logrse in res:
            #     if logrse[1] == 'subs_topic_log':
            #         logrse[0].to_excel('topic_subs_topic_log.xlsx')
            #     print(logrse[0].head())
            #     print(logrse[1])
            engine = utils_db.set_conn_parameters(host, port, db, user, psw)
            done = utils_db.upload_dfs(res, engine)
            utils_db.del_temporary_files()
            if done:
                print("Log uploaded, select another V2C log file or close this window")
                Label(ws, text="Log uploaded, select another log RO file or close this window ",
                      foreground='green').grid(row=10, columnspan=3, pady=10)
            else:
                print("Error uploading V2C log")
                Label(ws, text="Error uploading V2C log",
                      foreground='red').grid(row=10, columnspan=3, pady=10)

    greylog = IntVar()
    Checkbutton(ws, variable=greylog, onvalue=1, offvalue=0).grid(row=2, column=1)
    Label(ws, text='Converted Greylog?').grid(row=2, column=0)

    logbtn = Label(
        ws,
        text='Upload log format .txt '
    )
    logbtn.grid(row=0, column=0, padx=10)

    exlogbtn = Button(
        ws,
        text='Choose File',
        command=lambda: open_file_log()
    )
    exlogbtn.grid(row=0, column=1)
    reglab = Label(
        ws,
        text='Select Region'
    )
    reglab.grid(row=3, column=0, padx=10)

    variable = StringVar(ws)

    region = OptionMenu(ws, variable, *OPTIONS)

    region.grid(row=3, columnspan=2, column=1)

    upld = Button(
        ws,
        text='Compute Files',
        command=uploadFiles
    )
    upld.grid(row=9, columnspan=3, pady=10)

    ws.mainloop()
