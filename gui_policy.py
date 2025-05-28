from tkinter import *
from tkinter.ttk import *
from tkinter.filedialog import askopenfiles
import policy_parser

f = []
architecture_selected = ""


def main(port, db, user, psw, host):
    ws = Toplevel()
    ws.title('Policy Loader')
    ws.geometry('500x400')

    OPTIONS = [
        "SELECT DBC ARCHITECTURE",
        "POWERNET",
        "ATLANTISHIGH",
        "ATLANTISMID",
        "CUSW"
    ]

    if (port and db and user and psw and host) == "":
        port = "5432"
        db = "postgres"
        user = "postgres"
        psw = "admin"
        host = "localhost"

    def open_file_log():
        global f
        file_path = askopenfiles(mode='r', filetypes=[('json', '*json'), ('policy', '*txt')])
        for p in file_path:
            print("Policy file selected: \n" + str(p.name))
            Label(ws, text="Policy file selected: \n" + str(p.name), foreground='green').grid(row=10, columnspan=3,
                                                                                              pady=10)
        if len(file_path) > 0:
            for p in file_path:
                f.append(p.name)
        else:
            print("Select a Policy file ")
            Label(ws, text="Select a valid Policy file ", foreground='red').grid(row=10, columnspan=3, pady=10)

    def uploadFiles():
        global f
        global architecture_selected
        if len(f) == 0:
            print("Select a valid Policy file")
            Label(ws, text="Select a valid Policy file ", foreground='red').grid(row=10, columnspan=3, pady=10)
        else:
            architecture_selected = variable.get()
            check_valid = any(architecture_selected in arch for arch in OPTIONS[1:])
            if check_valid:
                pass
            else:
                print("Select a valid architecture")
                Label(ws, text="Select a valid architecture", foreground='red').grid(row=10, columnspan=3, pady=10)

            # Lettura di un policy
            done = policy_parser.main(f, architecture_selected)
            # engine = utils_db.set_conn_parameters(host, port, db, user, psw)
            # done = utils_db.upload_dfs(res, engine)
            # utils_db.del_temporary_files()
            if done:
                print("Policy uploaded, select another Policy file or close this window")
                Label(ws, text="Policy uploaded, select another Policy file or close this window ",
                      foreground='green').grid(row=10, columnspan=3, pady=10)
            else:
                print("Error uploading Policy")
                Label(ws, text="Error uploading Policy",
                      foreground='red').grid(row=10, columnspan=3, pady=10)

    logbtn = Label(
        ws,
        text='Upload Policy'
    )
    logbtn.grid(row=0, column=0, padx=10)

    exlogbtn = Button(
        ws,
        text='Choose File',
        command=lambda: open_file_log()
    )
    exlogbtn.grid(row=0, column=1)

    upld = Button(
        ws,
        text='Compute Files',
        command=uploadFiles
    )
    upld.grid(row=9, columnspan=3, pady=10)

    archlab = Label(
        ws,
        text='Select Architecture DBC'
    )
    archlab.grid(row=3, column=0, padx=10)

    variable = StringVar(ws)

    archs = OptionMenu(ws, variable, *OPTIONS)

    archs.grid(row=3, columnspan=2, column=1)

    ws.mainloop()
