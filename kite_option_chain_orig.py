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
con = cx_Oracle.connect(user="DHAN", password="Welcome12345#", dsn="dhan_high", encoding="UTF-8")
cursor = con.cursor()
cursor_sl = con.cursor()

def get_ltp (symbol):
    try:
        resp=kite.ltp("NSE:"+symbol)
        for key in resp.keys():
            ltp=round(resp[key].get("last_price"))
            return ltp       
    except Exception as e :
        print('Error LTP Zerodha.')
        error = e
        print (error.code , error.message )

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

     #Update LTP for DHAN
    for row in cursor.execute("SELECT DISTINCT  listagg('NFO:' ||z.tradingsymbol||',')  symbol FROM dhan_positions p, dhan_scripts s, zerodha_scripts z WHERE p.NETQTY <>0 and  p.SECURITYID=s.SEM_SMST_SECURITY_ID and s.SEM_TRADING_SYMBOL=z.TRADINGSYMBOL"):
        instr_list=row[0][:-1]
    resp=kite.ltp(list(instr_list.split(",")))
    for key in resp.keys():
        sql="update option_chain set LTP="+str(resp[key].get("last_price"))+" where TRADINGSYMBOL='"+str(key.replace("NFO:",''))+"'"
        cursor.execute(sql)
    con.commit()

    #Place Order
    for row in cursor.execute("select TRADINGSYMBOL,action,qty,order_type,nvl(SL_PRICE,0),nvl(TARGET_PRICE,0),VALIDITY,EXCHANGE from zerodha_orders  where status='New'"):
        try:
            #Place Order
            if row[3]== 'MARKET':
                print(row[0])
                order_id = kite.place_order(tradingsymbol=row[0],exchange=row[7] , transaction_type=row[1],quantity=row[2],variety="regular",order_type="MARKET",product=row[6],validity='DAY')
                sql="update zerodha_orders set ORDER_ID='"+str(order_id)+"',STATUS='Complete' where TRADINGSYMBOL='"+row[0]+"' and status='New'"
                cursor_sl.execute(sql)
                con.commit()
                #print(order_id)
                print("Order placed =>"+order_id)
                if row[6] == "CNC":
                    #Place GTT Order
                    time.sleep(1)
                    ltp=get_ltp(row[0])
                    if row[4]== 0:
                        stop_price=round(ltp-((ltp*2)/100))
                    else:
                        stop_price=row[4]
                    if row[5]== 0:  
                        target_price=round(ltp+((ltp*3)/100))
                    else:
                        target_price=row[5]
                    
                    order=[
                            {
                            "exchange": row[7],
                            "tradingsymbol": row[0],
                            "transaction_type": "SELL",
                            "quantity": row[2],
                            "order_type": "LIMIT",
                            "product":row[6],
                            "price": stop_price
                            },
                            {
                            "exchange": row[7],
                            "tradingsymbol": row[0],
                            "transaction_type": "SELL",
                            "quantity": row[2],
                            "order_type": "LIMIT",
                            "product": row[6],
                            "price": target_price
                            }
                            ]
                    resp=kite.gtt_order(exchange=row[7],tradingsymbol=row[0],trigger_values=[stop_price,target_price],last_price=ltp,orders=order)
                    trigger_id=resp["trigger_id"]
                    sql="update zerodha_orders set GTT_ID='"+str(trigger_id)+"' where TRADINGSYMBOL='"+row[0]+"'"
                    cursor_sl.execute(sql)
                    con.commit()
                    print("GTT placed =>"+str(trigger_id))

                if row[6] == "MIS" and  row[7] == "NSE":
                    order_id = kite.place_order(tradingsymbol=row[0],exchange=row[7] , transaction_type=row[1],quantity=row[2],variety="regular",order_type="MARKET",product=row[6],validity='DAY')
                    sql="update zerodha_orders set ORDER_ID='"+str(order_id)+"',STATUS='Complete' where TRADINGSYMBOL='"+row[0]+"' and status='New'"
                    cursor_sl.execute(sql)
                    con.commit()


            else:
                order_id = kite.place_order(tradingsymbol=row[0],exchange=row[7] , transaction_type=row[1],quantity=row[2],variety="regular",order_type="SL",product=row[6],validity='DAY',price=row[4],trigger_price=row[4]-3)
                sql="update zerodha_orders set ORDER_ID='"+str(order_id)+"',STATUS='Complete' where TRADINGSYMBOL='"+row[0]+"' and status='New' "
                cursor_sl.execute(sql)
                con.commit()
                sql="update zerodha_positions set STRATEGY='BANK_STRADDLE',ORDER_ID='"+str(order_id)+"' where TRADINGSYMBOL='"+row[0]+"'"
                cursor_sl.execute(sql)
                con.commit()
                #print(order_id)
                print("Order placed =>"+order_id)
                #Place GTT Order
                time.sleep(1)
                ltp=get_ltp(row[0])
        except Exception as e:
            print("Order placement failed",e)
    now = datetime.now()
    todayclose = now.replace(hour=15, minute=30, second=0, microsecond=0)
    if now >  todayclose:
        end=1
    print('LTP Option Chain OK...')  
    time.sleep(5)