import json
import kiteconnect
import requests as req
import cx_Oracle 
import os
import time
import math
from sample import Functions
from datetime import datetime
from datetime import timedelta
from configs import *
from telegram import *

today = datetime.now().date()
url_pos="https://api.dhan.co/positions"
url_orders="https://api.dhan.co/orders"
url_funds="https://api.dhan.co/fundlimit"

header_pos = {
    "access-token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNjgyMDQ5MDM1LCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJkaGFuQ2xpZW50SWQiOiIxMDAwNTcxODM1In0.W3hfPSvI2YjWnyouasQkx7ruOvql35tQpQH4C-z8sR5bGhT5cWPma4kCyKjHMDL82NqHpPAYIAYzEHc_78AGrQ",
    "Content-Type": "application/json"
}

#Oracle Connection
cx_Oracle.init_oracle_client(lib_dir=CLIENT_LOC)
con = cx_Oracle.connect(user="DHAN", password="Welcome12345#", dsn="ssn1", encoding="UTF-8")
cursor = con.cursor()
cursor_sl = con.cursor()

def execute_sql (sql):
    try:
        cursor.execute(sql)
        con.commit()
    except cx_Oracle.Error :
        print("SQL :",sql)
        os.system('say ' "Database Error")
        error = e
        print (error.code , error.message )

def get_ltp (symbol):
    for row in cursor.execute("select EXCHANGE||':' ||p.tradingsymbol from zerodha_scripts p where p.name like '"+symbol+"' and SEGMENT='NFO-FUT' and expiry IN ( SELECT MIN(expiry) FROM zerodha_scripts WHERE SEGMENT='NFO-FUT'  AND name = p.name)"):
        try:
            resp=kite.ltp(row[0])
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
        send_message("Failed Connection to Zerodha...", '542739135')
    else:
        send_message("Connected to Zerodha...", '542739135')
except:
        send_message("Failed Connection to Zerodha...", '542739135')
        exit()

start=1
end=0
instr_list="NFO:"

#print DHAN funds
try:
    resp = req.get(url =url_funds,headers = header_pos)    
    resp = json.loads(resp.text)   
    print(resp['availabelBalance'])
    send_message("Connected to Dhan..", '542739135')
except:
        send_message("Failed Connection to Dhan...", '542739135')
        exit()

start=1
end=0
while start == 1 and end == 0:
    #Update LTP for Edel Option Chain
    for row in cursor_sl.execute("select * from ( select listagg('NFO:' || TRADINGSYMBOL||',')  symbol from option_chain ) where symbol is not null"):
        instr_list=row[0][:-1]
        resp=kite.ltp(list(instr_list.split(",")))
        for key in resp.keys():
            sql="update option_chain set LTP="+str(resp[key].get("last_price"))+" where TRADINGSYMBOL='"+str(key.replace("NFO:",''))+"'"
            cursor.execute(sql)
        con.commit()
    print("EDEL Chain Ok..")

    #Check for Dhan Orders
    for row in cursor.execute("select TRADINGSYMBOL,SECURITYID,PRODUCTTYPE,action,qty,order_type,VALIDITY,nvl(PRICE,0) from dhan_orders  where status='New'"):
        try:
            #Place Order
            if row[5]=="MARKET":
                order_data={"dhanClientId":"1000571835","correlationId":"123payee678","transactionType":row[3],"exchangeSegment":"NSE_FNO","productType":row[2],"orderType":row[5],"validity":row[6],"tradingSymbol":row[0],"securityId":row[1],"quantity":row[4],"disclosedQuantity":"","price":row[7]}
                send_message("Dhan Traded: "+row[0],'542739135')
            else:
                order_data={"dhanClientId":"1000571835","correlationId":"123payee678","transactionType":row[3],"exchangeSegment":"NSE_FNO","productType":row[2],"orderType":row[5],"validity":row[6],"tradingSymbol":row[0],"securityId":row[1],"quantity":row[4],"disclosedQuantity":"","price":row[7]+3,"triggerPrice":row[7]}
                send_message("Dhan SL Order Traded: "+row[0],'542739135')
            
            resp = req.post(url = url_orders, headers = header_pos,data=json.dumps(order_data))   
            resp = json.loads(resp.text)       
            time.sleep(2)
            order_id=resp['orderId']
            sql="update dhan_orders set ORDERID='"+str(order_id)+"',STATUS='Complete' where SECURITYID='"+row[1]+"' and status='New' "
            cursor_sl.execute(sql)
            con.commit()
        except Exception as e:
            print("Order placement failed",e)
            send_message("Dhan Order placement failed ", '542739135')
    print("DHAN Order Ok..")

    #Retrieve Dhan Positions
    resp = req.get(url = url_pos, headers = header_pos)       
    resp = json.loads(resp.text)
    for i in range(len(resp)):

        if resp[i].get("exchangeSegment") == "NSE_FNO":
            sql="MERGE INTO dhan_positions p USING (SELECT '"+str(resp[i].get("securityId"))+"' securityId from dual ) s ON ( p.securityId = s.securityId ) WHEN MATCHED THEN UPDATE SET PRODUCTTYPE='"+str(resp[i].get("productType"))+"',NETQTY="+str(resp[i].get("netQty"))+",TRADINGSYMBOL='"+str(resp[i].get("tradingSymbol"))+"',REALIZEDPROFIT="+str(resp[i].get("realizedProfit"))+",UNREALIZEDPROFIT="+str(resp[i].get("unrealizedProfit"))+" WHEN NOT MATCHED THEN insert (TRADINGSYMBOL,PRODUCTTYPE,BUYAVG,NETQTY,SELLAVG,SECURITYID,REALIZEDPROFIT,UNREALIZEDPROFIT) values('"+str(resp[i].get("tradingSymbol"))+"','"+str(resp[i].get("productType"))+"',"+str(resp[i].get("buyAvg"))+","+str(resp[i].get("netQty"))+","+str(resp[i].get("sellAvg"))+",'"+str(resp[i].get("securityId"))+"',"+str(resp[i].get("realizedProfit"))+","+str(resp[i].get("unrealizedProfit"))+")"
            
            #print(sql)
            execute_sql(sql)
            con.commit()
    print("DHAN Positions Ok..")

    #Update DHAN Positions LTP
    for row in cursor_sl.execute("SELECT DISTINCT  listagg('NFO:' ||z.tradingsymbol||',')  symbol FROM dhan_positions p,  zerodha_scripts z WHERE p.NETQTY <>0 and  p.SECURITYID=z.EXCHANGE_TOKEN"):
        instr_list=row[0][:-1]
        resp=kite.ltp(list(instr_list.split(",")))
        for key in resp.keys():
            sql="update dhan_positions set LTP="+str(resp[key].get("last_price"))+" where SECURITYID IN ( select EXCHANGE_TOKEN from zerodha_scripts where TRADINGSYMBOL='"+str(key.replace("NFO:",''))+"')"
            #print(sql)
            cursor.execute(sql)
            con.commit()
        sql="UPDATE dhan_positions SET unrealizedprofit = CASE WHEN netqty < 0 THEN ( ltp - sellavg ) * netqty WHEN netqty > 0 THEN ( ltp - buyavg ) * netqty ELSE 0 END"
        execute_sql(sql)
    print("DHAN LTP Ok..")

    #Update DHAN STRATEGIES
    for row in cursor_sl.execute("SELECT a.group_name, ( SELECT SUM(nvl(unrealizedprofit, 0)) FROM dhan_positions WHERE group_name = a.group_name  ) unrealizedprofit, ( SELECT SUM(realizedprofit) FROM ( SELECT nvl(SUM(realizedprofit), 0) realizedprofit FROM dhan_positions WHERE group_name = a.group_name UNION SELECT nvl(SUM(realizedprofit), 0) FROM ( SELECT nvl(SUM(realizedprofit), 0) realizedprofit FROM dhan_positions_archive WHERE group_name = a.group_name ) )) realizedprofit, a.symbol FROM ( SELECT DISTINCT group_name, substr(TRADINGSYMBOL,0,instr(TRADINGSYMBOL,'-')-1) symbol FROM dhan_positions  ) a"):
        print("DHAN Strategy",row[3]) 
        try:
            try:
                ltp=get_ltp(row[3])
            except:
                print("Error Fetching LTP..")
                ltp=0 
            print(row[3],ltp) 
            sql="MERGE INTO DHAN_STRATEGIES p USING (SELECT  '"+row[0]+"' group_name from dual ) a ON ( a.group_name = p.STRATEGY_NAME ) WHEN MATCHED THEN UPDATE SET UNREALISED_PNL="+str(row[1])+", REALISED_PNL="+str(row[2])+",SYMBOL='"+row[3]+"',LTP="+str(ltp)+" WHEN NOT MATCHED THEN insert (STRATEGY_NAME,UNREALISED_PNL,REALISED_PNL,SYMBOL,LTP) values ('"+row[0]+"',"+str(row[1])+","+str(row[2])+",'"+str(row[3])+"', "+str(ltp)+" )"
            execute_sql(sql)    
        except cx_Oracle.Error :
            print('Error LTP Group...')
            error = e
            print (error.code , error.message )

    #end=1
    con.commit()
    time.sleep(10)
    now = datetime.now()

    todayclose = now.replace(hour=15, minute=30, second=0, microsecond=0)
    if now >  todayclose:
        end=1

#Cleanup
sql="delete from dhan_positions where NETQTY=0" 
cursor.execute(sql)
sql="DELETE FROM DHAN_STRATEGIES where STRATEGY_NAME not in ( select group_name from dhan_positions)" 
cursor.execute(sql)
sql="delete from dhan_orders" 
cursor.execute(sql)
con.commit()