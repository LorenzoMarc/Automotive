from tkinter import *
from tkinter.ttk import *
from tkinter.filedialog import askopenfile
import dlt_parser
import utils_db


def main(port, db, user, psw, host):
    ws = Toplevel()
    ws.title('DLT Loader')
    ws.geometry('500x400')

    global log
    if (port and db and user and psw and host) == "":
        port = "5432"
        db = "postgres"
        user = "postgres"
        psw = "admin"
        host = "localhost"

    def open_file_log():
        global f
        file_path = askopenfile(mode='r', filetypes=[('dlt', '*dlt')])
        print("DLT file selected: " + str(file_path.name))
        Label(ws, text="DLT file selected: \n" + str(file_path.name), foreground='green').grid(row=10, columnspan=3,
                                                                                                  pady=10)
        if file_path is not None:
            f = file_path.name
        else:
            print("Select a DLT file ")
            Label(ws, text="Select a valid DLT file ", foreground='red').grid(row=10, columnspan=3, pady=10)

    def uploadFiles():

        if f is None:
            print("Select a valid DLT file")
            Label(ws, text="Select a valid DLT file ", foreground='red').grid(row=10, columnspan=3, pady=10)
        else:
            dfs = dlt_parser.main(f)
            engine = utils_db.set_conn_parameters(host, port, db, user, psw)
            done = utils_db.upload_dfs(dfs, engine)

            if done:
                print("DLT uploaded, select another DLT file or close this window")
                Label(ws, text="DLT uploaded, select another DLT file or close this window ",
                      foreground='green').grid(row=10, columnspan=3, pady=10)
            else:
                print("Error uploading DLT")
                Label(ws, text="Error uploading DLT",
                      foreground='red').grid(row=10, columnspan=3, pady=10)

    logbtn = Label(
        ws,
        text='Upload DLT'
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

    ws.mainloop()
