import json
import requests as req
import cx_Oracle 
import os
import time
import math
from datetime import datetime
from datetime import timedelta
from configs import *


today = datetime.now().date()
try:
    filetime = datetime.fromtimestamp(os.path.getctime(FILE_LOC+"kotak_tokens.txt"))
    print('Kotak Token File Available...')
except OSError:
    print('Generating Tokens...')
    filetime=0
    ott=""
    sesstoken=""

if os.path.exists(FILE_LOC+"kotak_tokens.txt"):
    if filetime.date() == today:
            print('Reading OTT and SESSIONTOKEN...')
            file1 = open(FILE_LOC+"kotak_tokens.txt", "r")
            Lines = file1.readlines()
            ott=Lines[0].replace("\n",'')
            sesstoken=Lines[1]
    else:
        print('Generating Tokens...')
        filetime=0
        ott=""
        sesstoken=""

header_ltp_kotak = {
    "accept": "application/json",
	"consumerKey": "Z3l254sO82sHdPjpm80Ahbkmru0a",
	"sessionToken": sesstoken,
    "Authorization": "Bearer d0337058-675c-352f-ab63-247ae9e61b9a"
}

def execute_sql (sql):
    try:
        cursor.execute(sql)
        con.commit()
    except cx_Oracle.Error :
        print("SQL :",sql)
        os.system('say ' "Database Error")
        error = e
        print (error.code , error.message )

cx_Oracle.init_oracle_client(lib_dir=CLIENT_LOC)
con = cx_Oracle.connect(user="DHAN", password="Welcome12345#", dsn="dhan_high", encoding="UTF-8")
cursor = con.cursor()
cursor_sl = con.cursor()

start=1
end=0
while start == 1 and end == 0:
    for row in cursor.execute("SELECT o.script edel_script, k.instrumenttoken  FROM option_chain  o, edel_scripts  e, kotak_scripts k WHERE o.script = e.exchangetoken AND e.expiry = k.expiry AND e.symbolname = k.instrumentname AND e.strikeprice = k.strike AND e.optiontype = k.optiontype"):
        try:
            ltp_url="https://ctradeapi.kotaksecurities.com/apim/quotes/v1.0/ltp/instruments/"+row[1]
            resp = req.get(url = ltp_url, headers = header_ltp_kotak)    
            resp = json.loads(resp.text)
            ltp=resp['success'][0].get("lastPrice")
            #print(ltp)
            sql="UPDATE OPTION_CHAIN p SET LTP=round("+str(ltp)+") WHERE SCRIPT='"+row[0]+"'"
            #print(sql)
            cursor_sl.execute(sql)
            time.sleep(1)
            con.commit() 
        except Exception as e :
            print('Error LTP Option Chain...')
            error = e
            print (error.code , error.message )

    now = datetime.now()
    todayclose = now.replace(hour=15, minute=30, second=0, microsecond=0)
    if now >  todayclose:
        end=1
    print('LTP Chain OK...')  
    time.sleep(5)



