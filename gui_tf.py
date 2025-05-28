'''

Crea interfaccia utente che prende in INPUT:
    1) Transfer function
    2) DBC file

    OUTPUT:

'''

from tkinter import *
from tkinter.ttk import *
from tkinter.filedialog import askopenfile
import action1
import action2
import utils_db
import dbc_parser


def main(port, db, user, psw, host):

    ws = Toplevel()
    ws.title('Tranfer Function Check')
    ws.geometry('500x400')

    global tf
    global dbc
    if (port and db and user and psw and host) == "":
        port = "5432"
        db = "postgres"
        user = "postgres"
        psw = "admin"
        host = "localhost"

    def open_file_tf():
        global tf
        file_path = askopenfile(mode='r', filetypes=[('excel', '*xlsx')])
        if file_path is not None:
            tf = file_path.name
            Label(ws, text="tf file selected: \n" + str(tf), foreground='green').grid(row=10, columnspan=3,
                                                                                                   pady=10)
        else:
            print("Select a Transfer function file .xlsx")
            Label(ws, text="Select a Transfer function file .xlsx", foreground='red').grid(row=10, columnspan=3, pady=10)

    def open_file_dbc():
        global dbc
        file_path = askopenfile(mode='r', filetypes=[('dbc', '*dbc')])
        if file_path is not None:
            print("dbc file selected: " + str(file_path.name))
            Label(ws, text="dbc file selected: \n" + str(file_path.name), foreground='green').grid(row=10, columnspan=3,
                                                                         pady=10)
            dbc = file_path.name
        else:
            print("Select a DBC file .dbc")
            Label(ws, text="Select a valid DBC file .dbc", foreground='red').grid(row=10, columnspan=3, pady=10)


    '''
    Funzione principale, avviata premendo il pulsante "Compute Files" da UI.
    '''

    def uploadFiles():
        if tf is None:
            print("Select a valid TF file .xlsx")
            Label(ws, text="Select a valid TF file .xlsx", foreground='red').grid(row=10, columnspan=3, pady=10)

        elif dbc is None:
            print("Select a valid dbc file .dbc")
            Label(ws, text="Select a valid dbc file .dbc", foreground='red').grid(row=10, columnspan=3, pady=10)

        else:

            tf_V1 = action1.main(tf)
            dbc_csv, ecu_path = dbc_parser.main(dbc)
            df_to_upload = action2.main(dbc_csv, tf_V1, ecu_path)
            engine = utils_db.set_conn_parameters(host, port, db, user, psw)
            done = utils_db.upload_dfs(df_to_upload, engine)
            utils_db.del_temporary_files()

            if done:
                Label(ws, text='Transfer Function computed and uploaded!'.upper(),
                      foreground='green').grid(row=12,  columnspan=3, pady=10)
                print('Data Uploaded to', host)
                print('Choose other files or close this application')
            else:
                Label(ws, text='problem', foreground='red').grid(row=10, columnspan=3, pady=10)

    '''
    Costruzione interfaccia app
    
    '''

    tfbtn = Label(
        ws,
        text='Upload Transfer function format .xlsx '
    )
    tfbtn.grid(row=0, column=0, padx=10)

    extfbtn = Button(
        ws,
        text='Choose File',
        command=lambda: open_file_tf()
    )
    extfbtn.grid(row=0, column=1)

    dbcbtn = Label(
        ws,
        text='Upload DBC file .dbc'
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
