import kiteconnect
from sample import Functions
import pandas as pd
from configs import *

import cx_Oracle 
cx_Oracle.init_oracle_client(lib_dir=CLIENT_LOC)
con = cx_Oracle.connect(user="DHAN", password="Welcome12345#", dsn="ssn1", encoding="UTF-8")
cursor = con.cursor()

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

print("ZERODHA  => Truncating ZERODHA_SCRIPTS....")
cursor.execute("TRUNCATE TABLE ZERODHA_SCRIPTS DROP STORAGE")
print("Loading Scripts....")
resp=kite.kiteInstruments("NFO")
df = pd.DataFrame(resp)

print("Loading FNO....")
instruments=df[df["exchange"]=="NFO"]
rows = [tuple(x) for x in instruments.values]
cursor.executemany("INSERT INTO ZERODHA_SCRIPTS VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12)",rows)
con.commit()
resp=kite.kiteInstruments("NSE")
df = pd.DataFrame(resp)

print("Loading CASH....")
instruments=df[df["exchange"]=="NSE"]
rows = [tuple(x) for x in instruments.values]
cursor.executemany("INSERT INTO ZERODHA_SCRIPTS VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12)",rows)
con.commit()
