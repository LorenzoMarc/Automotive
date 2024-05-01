'''

MAIN UI with tasks analysis (implemented):

    1) CAN LOG parser
    2) TF and DBC analysis (CC-2 and CC-3)
    3) Feature Check on DBC (CC-6)
    4) DBC compare (CC-5)
    5) Smartphone Control 3.0 Log parser (on going development)

Server Connection Parameters:
        default:
                port = "5432"
                db = "postgres"
                user = "postgres"
                psw = "admin"
                host = "localhost"
        NB: if one parameters is set with user's value,
        so the others must be set
'''

from tkinter import *
from tkinter.ttk import *
import gui_can_mapping
import gui_V2C
import gui_dbc_compare
import gui_dlt
import gui_feat
import gui_tf
import gui_policy

main_w = Tk()
main_w.title('TOOL ANALYSIS')
main_w.geometry('500x400')


def run_can_mapping():
    global can_mapping
    can_mapping = gui_can_mapping.main(port, db, user, psw, host)


def run_tf_check():
    global tf_check
    tf_check = gui_tf.main(port, db, user, psw, host)


def run_dbc_feature():
    global dbc_feature
    dbc_feature = gui_feat.main(port, db, user, psw, host)


def run_dbc_compare():
    global dbc_comparing
    dbc_comparing = gui_dbc_compare.main(port, db, user, psw, host)


def run_V2C_parser():
    gui_V2C.main(port, db, user, psw, host)


def run_policy_reader():
    gui_policy.main(port, db, user, psw, host)


def run_dlt_reader():
    gui_dlt.main(port, db, user, psw, host)


# BUTTON UI
btn = Button(main_w, text='CAN log mapping on DBC', command=run_can_mapping)
btn.grid(row=0, column=1)
btn1 = Button(main_w, text='Transfer Function Check on DBC', command=run_tf_check)
btn1.grid(row=1, column=1)
btn2 = Button(main_w, text='DBC Feature Compliance', command=run_dbc_feature)
btn2.grid(row=2, column=1)
btn3 = Button(main_w, text='DBC Compare', command=run_dbc_compare)
btn3.grid(row=3, column=1)
btn4 = Button(main_w, text='V2C Logger', command=run_V2C_parser)
btn4.grid(row=4, column=1)
btn5 = Button(main_w, text='Policy Loader', command=run_policy_reader)
btn5.grid(row=5, column=1)
btn5 = Button(main_w, text='DLT Loader', command=run_dlt_reader)
btn5.grid(row=6, column=1)

# Porta db
textport = Text(
    main_w, width=10, height=1
)
textport.grid(row=10, column=1)

porttxt = Label(
    main_w,
    text='Select port db'
)
porttxt.grid(row=10, column=0, padx=10)

# Nome DB
textdb = Text(
    main_w, width=10, height=1
)
textdb.grid(row=11, column=1)

dbtxt = Label(
    main_w,
    text='Select db'
)
dbtxt.grid(row=11, column=0, padx=10)

#  username
textuser = Text(
    main_w, width=10, height=1
)
textuser.grid(row=12, column=1)

usertxt = Label(
    main_w,
    text='Select user db'
)
usertxt.grid(row=12, column=0, padx=10)

#  psw
textpsw = Text(
    main_w, width=10, height=1
)
textpsw.grid(row=8, column=1)

pswtxt = Label(
    main_w,
    text='PSW DB'
)
pswtxt.grid(row=8, column=0, padx=10)

#  host
texthost = Text(
    main_w, width=10, height=1
)
texthost.grid(row=9, column=1)

hosttxt = Label(
    main_w,
    text='Host DB'
)
hosttxt.grid(row=9, column=0, padx=10)

port = textport.get(1.0, "end-1c")
db = textdb.get(1.0, "end-1c")
user = textuser.get(1.0, "end-1c")
psw = textpsw.get(1.0, "end-1c")
host = texthost.get(1.0, "end-1c")

main_w.mainloop()
