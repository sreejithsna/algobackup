import kiteconnect
import time
import math
from datetime import datetime
from datetime import timedelta
from sample import Functions
import pandas as pd
from configs import *
from telegram import *


import cx_Oracle 
cx_Oracle.init_oracle_client(lib_dir=CLIENT_LOC)
con = cx_Oracle.connect(user="DHAN", password="Welcome12345#", dsn="ssn1", encoding="UTF-8")
cursor = con.cursor()
cursor_sl = con.cursor()

kite=Functions()
kite.readZerodhaAccessToken()
try:
    if (kite.profile()['user_id'] != 'QF1425'):
        print("Failed to Connect to Zerodha account.")
    else:
        print("Connected to Zerodha")
except:
        print("Failed to Connect to Zerodha account.")
        exit()

start=1
end=0
instr_list="NFO:"
send_message("Kite Option Chain Daemon Started...", '542739135')

while start == 1 and end == 0:
    #Update LTP for FNO
    for row in cursor.execute("select * from ( select listagg('NFO:' || TRADINGSYMBOL||',')  symbol from option_chain ) where symbol is not null"):
        instr_list=row[0][:-1]
    resp=kite.ltp(list(instr_list.split(",")))
    for key in resp.keys():
        sql="update option_chain set LTP="+str(resp[key].get("last_price"))+" where TRADINGSYMBOL='"+str(key.replace("NFO:",''))+"'"
        cursor.execute(sql)
    con.commit()

    now = datetime.now()
    todayclose = now.replace(hour=15, minute=30, second=0, microsecond=0)
    if now >  todayclose:
        end=1
    print('LTP Option Chain OK...')  
    time.sleep(5)