import pandas as pd
from configs import *
import cx_Oracle 
import numpy as np
from telegram import *

msg=""
script_url = 'https://images.dhan.co/api-data/api-scrip-master.csv'

print("Downloading Instruments data...")

#Oracle Connection
cx_Oracle.init_oracle_client(lib_dir=CLIENT_LOC)
con = cx_Oracle.connect(user="DHAN", password="Welcome12345#", dsn="ssn1", encoding="UTF-8")
cursor = con.cursor()
msg="DHAN Instrument Load Started...\n"

print("DHAN => Truncating Scripts....")
cursor.execute("TRUNCATE TABLE dhan_scripts DROP STORAGE")
for row in cursor.execute("select count(*) from dhan_scripts"):
    msg=msg+"TRUNCATED => Row Count :"+str(row[0])+"\n"
print("Loading Scripts....")
df = pd.read_csv(script_url, error_bad_lines=False)
df=df.drop(['SEM_LOT_UNITS'], axis=1)
print("Loading FNO....")
opt_ins=df[df["SEM_INSTRUMENT_NAME"]=="OPTIDX"]
rows = [tuple(x) for x in opt_ins.values]
try:
    cursor.executemany("INSERT INTO DHAN_SCRIPTS VALUES (:1,:2,:3,:4,:5,:6,:7)",rows)
except cx_Oracle.Error:
    for row in rows:
        try:
            cursor.execute("INSERT INTO DHAN_SCRIPTS VALUES (:1,:2,:3,:4,:5,:6,:7)",row)
            #print("Processing =>",row)
            con.commit()
        except cx_Oracle.Error:
            print("ERROR ",row)
            #error = e
            #print (error.code , error.message )
            #exit(1)
con.commit()
for row in cursor.execute("select count(*) from dhan_scripts"):
    msg=msg+"LOADED => Row Count :"+str(row[0])+"\n"
send_message(msg, '542739135')
