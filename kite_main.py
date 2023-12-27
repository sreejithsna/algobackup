from sqlite3 import sqlite_version_info
from sample import Functions
from configs import *
import json
import time
import time
import os
from datetime import datetime
from configs import *
from telegram import *


import cx_Oracle 
cx_Oracle.init_oracle_client(lib_dir=CLIENT_LOC)
con = cx_Oracle.connect(user="DHAN", password="Welcome12345#", dsn="dhan_high", encoding="UTF-8")
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

send_message("KITE MAIN Daemon Started...", '542739135')

while start == 1 and end == 0:
    #holdings
    resp=kite.holdings()
    #print(resp)
    for i in range(len(resp)):
        sql="MERGE INTO HOLDINGS p USING (SELECT  '"+resp[i]["tradingsymbol"]+"' TRADINGSYMBOL from dual ) a ON ( a.TRADINGSYMBOL = p.TRADINGSYMBOL ) WHEN MATCHED THEN UPDATE SET LTP="+str(resp[i]["last_price"])+", AVG_PRICE="+str(resp[i]["average_price"])+",PNL='"+str(resp[i]["pnl"])+"',QTY="+str(resp[i]["realised_quantity"])+" WHEN NOT MATCHED THEN insert (TRADINGSYMBOL,LTP,AVG_PRICE,PNL,QTY,STATUS) values ('"+resp[i]["tradingsymbol"]+"',"+str(resp[i]["last_price"])+","+str(resp[i]["average_price"])+","+str(resp[i]["pnl"])+","+str(resp[i]["realised_quantity"])+",'"+'OPEN'+"')"
        #print(sql)
        cursor.execute(sql)

    print("Zerodha Holdings ok")
    con.commit()

    time.sleep(2)

    resp=kite.positions()
    resp=resp["net"]
    #print(resp)
    for i in range(len(resp)):
        sql="MERGE INTO ZERODHA_POSITIONS p USING (SELECT  '"+resp[i]["tradingsymbol"]+"' TRADINGSYMBOL from dual ) a ON ( a.TRADINGSYMBOL = p.TRADINGSYMBOL ) WHEN MATCHED THEN UPDATE SET EXCHANGE='"+str(resp[i]["exchange"])+"', PRODUCT='"+str(resp[i]["product"])+"',QUANTITY="+str(resp[i]["quantity"])+",PNL="+str(resp[i]["pnl"])+",M2M="+str(resp[i]["m2m"])+",UNREALISED="+str(resp[i]["unrealised"])+",REALISED="+str(resp[i]["realised"])+",BUY_PRICE="+str(resp[i]["buy_price"])+",SELL_PRICE="+str(resp[i]["sell_price"])+",LTP="+str(resp[i]["last_price"])+" WHEN NOT MATCHED THEN insert (TRADINGSYMBOL,LTP,EXCHANGE,PRODUCT,QUANTITY,PNL,M2M,UNREALISED,BUY_PRICE,SELL_PRICE,REALISED) values ('"+resp[i]["tradingsymbol"]+"',"+str(resp[i]["last_price"])+",'"+str(resp[i]["exchange"])+"','"+str(resp[i]["product"])+"',"+str(resp[i]["quantity"])+","+str(resp[i]["pnl"])+","+str(resp[i]["m2m"])+","+str(resp[i]["unrealised"])+","+str(resp[i]["buy_price"])+","+str(round(resp[i]["sell_price"]))+","+str(resp[i]["realised"])+")"
        #print(sql)
        cursor.execute(sql)
    con.commit()
    print("Zerodha Positions ok")

    #Check BANK Straddle
    cursor.execute("select  count(*) POSTNS from zerodha_positions where STRATEGY='BANK_STRADDLE'")
    postns = cursor.fetchone()
    postns=postns[0]
    if postns == 2:
        #check if two legs are taken
        print("         Active Position..")
        cursor.execute("select  count(*) from zerodha_positions where STRATEGY='BANK_STRADDLE'  and QUANTITY=0")
        sqoff = cursor.fetchone()
        sqoff=sqoff[0]
        if sqoff == 1:
            #SL hit, Adjust to cost
            print("Inside Adjust..")
            for row in cursor_sl.execute("select TRADINGSYMBOL,ORDER_ID,SELL_PRICE from zerodha_positions where STRATEGY='BANK_STRADDLE' and QUANTITY<>0"):
                symbol=row[0]
                print("SL to Cost..")
                order_id = kite.modify_order(order_id=row[1],price=round(row[2]),trigger_price=round(row[2]-3),quantity=25,variety="regular",order_type="SL")
                sql="update zerodha_positions set STRATEGY='BANK_STRADDLE_MOD' where STRATEGY='BANK_STRADDLE'" 
                cursor.execute(sql)
                con.commit()
        print("BANKNIFTY STRADDLE Ok..")

    time.sleep(5)

    #end=1
    now = datetime.now()
    todayclose = now.replace(hour=15, minute=30, second=0, microsecond=0)
    if now >  todayclose:
        end=1

#Cleanup
sql="delete from zerodha_positions where QUANTITY=0" 
cursor.execute(sql)
sql="delete from zerodha_orders" 
cursor.execute(sql)
sql="delete from zerodha_positions where BUY_PRICE=0 and ( EXCHANGE='NSE' or EXCHANGE='BSE') " 
cursor.execute(sql)
con.commit()
