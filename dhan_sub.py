import json
import requests as req
import cx_Oracle 
import os
import time
import math
from datetime import datetime
from datetime import timedelta
from configs import *
from telegram import *

today = datetime.now().date()

#Oracle Connection
cx_Oracle.init_oracle_client(lib_dir=CLIENT_LOC)
con = cx_Oracle.connect(user="DHAN", password="Welcome12345#", dsn="dhan_high", encoding="UTF-8")
cursor = con.cursor()
cursor_sl = con.cursor()

print("Starting...")
def execute_sql (sql):
    try:
        cursor.execute(sql)
        con.commit()
    except cx_Oracle.Error :
        print("SQL :",sql)
        os.system('say ' "Database Error")
        error = e
        print (error.code , error.message )

url_pos="https://api.dhan.co/positions"
url_orders="https://api.dhan.co/orders"
url_funds="https://api.dhan.co/fundlimit"
header_pos = {
    "access-token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNjY5NTIyMTc4LCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJkaGFuQ2xpZW50SWQiOiIxMDAwNTcxODM1In0.AR3j7EepLYW9ROKwbNf9NwAhIVPrn-tSv6MScCrwO6o_36OuTF70lwJuxBgFjIUEwpkEkNvDCLuI19zMLlUvsw",
    "Content-Type": "application/json"
}

#print funds
resp = req.get(url =url_funds,headers = header_pos)    
resp = json.loads(resp.text)   
print(resp['availabelBalance'])

send_message("Started Dhan..", '542739135')

start=1
end=0
while start == 1 and end == 0:
    #Check for Orders
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
    #Retrieve Positions
    resp = req.get(url = url_pos, headers = header_pos)       
    resp = json.loads(resp.text)
    for i in range(len(resp)):
        '''sql="MERGE INTO dhan_positions p USING (SELECT '"+str(resp[i].get("tradingSymbol"))+"' TRADINGSYMBOL from dual ) s ON ( p.TRADINGSYMBOL = s.TRADINGSYMBOL ) WHEN MATCHED THEN UPDATE SET p.DHANCLIENTID = '"+str(resp[i].get("dhanClientId"))+"',PRODUCTTYPE='"+str(resp[i].get("productType"))+"',BUYAVG='"+str(resp[i].get("buyAvg"))+"',NETQTY='"+str(resp[i].get("netQty"))+"',SELLAVG='"+str(resp[i].get("sellAvg"))+"',DRVEXPIRYDATE='"+str(resp[i].get("drvExpiryDate"))+"',DRVSTRIKEPRICE='"+str(resp[i].get("drvStrikePrice"))+"',DRVOPTIONTYPE='"+str(resp[i].get("drvOptionType"))+"',SECURITYID='"+str(resp[i].get("securityId"))+"' WHEN NOT MATCHED THEN insert (DHANCLIENTID,TRADINGSYMBOL,PRODUCTTYPE,BUYAVG,DRVEXPIRYDATE,NETQTY,SELLAVG,DRVSTRIKEPRICE,DRVOPTIONTYPE,SECURITYID) values('"+str(resp[i].get("dhanClientId"))+"','"+str(resp[i].get("tradingSymbol"))+"','"+str(resp[i].get("productType"))+"','"+str(resp[i].get("buyAvg"))+"','"+str(resp[i].get("drvExpiryDate"))+"','"+str(resp[i].get("netQty"))+"','"+str(resp[i].get("sellAvg"))+"','"+str(resp[i].get("drvStrikePrice"))+"','"+str(resp[i].get("drvOptionType"))+"','"+str(resp[i].get("securityId"))+"')"'''

        sql="MERGE INTO dhan_positions p USING (SELECT '"+str(resp[i].get("securityId"))+"' securityId from dual ) s ON ( p.securityId = s.securityId ) WHEN MATCHED THEN UPDATE SET BUYAVG="+str(resp[i].get("buyAvg"))+",SELLAVG="+str(resp[i].get("sellAvg"))+",PRODUCTTYPE='"+str(resp[i].get("productType"))+"',NETQTY="+str(resp[i].get("netQty"))+",TRADINGSYMBOL='"+str(resp[i].get("tradingSymbol"))+"',REALIZEDPROFIT="+str(resp[i].get("realizedProfit"))+",UNREALIZEDPROFIT="+str(resp[i].get("unrealizedProfit"))+" WHEN NOT MATCHED THEN insert (TRADINGSYMBOL,PRODUCTTYPE,BUYAVG,NETQTY,SELLAVG,SECURITYID,REALIZEDPROFIT,UNREALIZEDPROFIT) values('"+str(resp[i].get("tradingSymbol"))+"','"+str(resp[i].get("productType"))+"',"+str(resp[i].get("buyAvg"))+","+str(resp[i].get("netQty"))+","+str(resp[i].get("sellAvg"))+",'"+str(resp[i].get("securityId"))+"',"+str(resp[i].get("realizedProfit"))+","+str(resp[i].get("unrealizedProfit"))+")"

        #print(sql)
        execute_sql(sql)
        con.commit()

    #Check BANK Straddle
    cursor.execute("select  count(*) POSTNS from dhan_positions where STRATEGY='BANK_STRADDLE'")
    postns = cursor.fetchone()
    postns=postns[0]
    if postns == 2:
        #check if two legs are taken
        print("         Active Position..")
        cursor.execute("select  count(*) from dhan_positions where STRATEGY='BANK_STRADDLE'  and NETQTY=0")
        sqoff = cursor.fetchone()
        sqoff=sqoff[0]
        if sqoff == 1:
            #SL hit, Adjust to cost
            print("Inside Adjust..")
            for row in cursor_sl.execute("select p.securityid,o.ORDERID,round(p.sellavg),o.qty,p.TRADINGSYMBOL from dhan_positions p, dhan_orders o where p.STRATEGY='BANK_STRADDLE' and p.securityid=o.securityid and o.order_type='STOP_LOSS' and p.netqty<>0"):
                url_order_mod="https://api.dhan.co/orders/"+row[1]
                order_data={"dhanClientId":"1000571835","orderId":row[1],"orderType":"STOP_LOSS","quantity":row[3],"price":row[2]+3,"triggerPrice":row[2]}
                resp = req.put(url = url_order_mod, headers = header_pos,data=json.dumps(order_data))
                resp = json.loads(resp.text)
                print("SL to Cost..")
                send_message("BANK SL Hit..", '542739135')
                sql="update dhan_positions set STRATEGY='BANK_SL_HIT' where STRATEGY='BANK_STRADDLE'" 
                cursor.execute(sql)
                con.commit()
                #Place target order
                '''order_data={"dhanClientId":"1000571835","correlationId":"123payee678","transactionType":"BUY","exchangeSegment":"NSE_FNO","productType":"INTRADAY","orderType":"STOP_LOSS","validity":"DAY","tradingSymbol":row[4],"securityId":row[0],"quantity":row[3],"disclosedQuantity":"","price":row[2]-80,"triggerPrice":row[2]-82}
                send_message("BANK Target SL Order Placed: "+row[0],'542739135')
                resp = req.post(url = url_orders, headers = header_pos,data=json.dumps(order_data))   
                resp = json.loads(resp.text)
                print(resp)       
                time.sleep(2)
                order_id=resp['orderId']
                sql="update dhan_orders set TARGET_ORDER_ID='"+str(order_id)+"' where SECURITYID='"+row[0]+"' and ORDER_TYPE='STOP_LOSS' "
                cursor.execute(sql)
                con.commit()
                print("Target Order done..")  '''
        print("BANKNIFTY STRADDLE Ok..")

    #Check NIFTY Straddle
    cursor.execute("select  count(*) POSTNS from dhan_positions where STRATEGY='NIFTY_STRADDLE'")
    postns = cursor.fetchone()
    postns=postns[0]
    if postns == 2:
        #check if two legs are taken
        print("         Active Position..")
        cursor.execute("select  count(*) from dhan_positions where STRATEGY='NIFTY_STRADDLE'  and NETQTY=0")
        sqoff = cursor.fetchone()
        sqoff=sqoff[0]
        if sqoff == 1:
            #SL hit, Adjust to cost
            print("Inside Adjust..")
            for row in cursor_sl.execute("select p.securityid,o.ORDERID,round(p.sellavg),o.qty,p.TRADINGSYMBOL from dhan_positions p, dhan_orders o where p.STRATEGY='NIFTY_STRADDLE' and p.securityid=o.securityid and o.order_type='STOP_LOSS' and p.netqty<>0"):
                url_order_mod="https://api.dhan.co/orders/"+row[1]
                order_data={"dhanClientId":"1000571835","orderId":row[1],"orderType":"STOP_LOSS","quantity":row[3],"price":row[2]+3,"triggerPrice":row[2]}
                resp = req.put(url = url_order_mod, headers = header_pos,data=json.dumps(order_data))
                resp = json.loads(resp.text)
                print("SL to Cost..")
                send_message("NIFTY SL Hit..", '542739135')
                sql="update dhan_positions set STRATEGY='NIFTY_SL_HIT' where STRATEGY='NIFTY_STRADDLE'" 
                cursor.execute(sql)
                con.commit()
                #Place target order
                '''order_data={"dhanClientId":"1000571835","correlationId":"123payee678","transactionType":"BUY","exchangeSegment":"NSE_FNO","productType":"INTRADAY","orderType":"STOP_LOSS","validity":"DAY","tradingSymbol":row[4],"securityId":row[0],"quantity":row[3],"disclosedQuantity":"","price":row[2]-40,"triggerPrice":row[2]-42}
                send_message("NIFTY Target SL Order Placed: "+row[0],'542739135')
                resp = req.post(url = url_orders, headers = header_pos,data=json.dumps(order_data))   
                resp = json.loads(resp.text)  
                print(resp) 
                time.sleep(2)
                order_id=resp['orderId']
                sql="update dhan_orders set TARGET_ORDER_ID='"+str(order_id)+"' where SECURITYID='"+row[0]+"' and ORDER_TYPE='STOP_LOSS' "
                cursor_sl.execute(sql)
                con.commit()
                print("Target Order done..")'''      
        print("NIFTY STRADDLE Ok..")

    #PNL Check and close

    for row in cursor.execute("select * from (select STRATEGY, round(sum(REALIZEDPROFIT)) REALIZEDPROFIT,sum(NETQTY) NETQTY from dhan_positions where STRATEGY like '%_SL_HIT'  group by STRATEGY) where NETQTY=0"):
        pnl=int(row[1])
        strategy=row[0]
        if pnl >= 900:
            send_message(row[0]+" Profit !!", '542739135')
            #cancel the pending order
            '''print(row[0])
            for row in cursor.execute("select ORDERID from dhan_orders where TARGET_ORDER_ID is not null and SECURITYID in ( select SECURITYID from dhan_positions where STRATEGY='"+row[0]+"')"):
                print(row[0])
                send_message("Cancel Order"+row[0], '542739135')
                url_order_canc="https://api.dhan.co/orders/"+str(row[0])
                resp = req.delete(url = url_order_canc, headers = header_pos)
                resp = json.loads(resp.text)
                print("Cancel Order ",url_order_canc)
                print(resp)'''
            sql="update dhan_positions set STRATEGY='"+strategy+"_PROFIT_CLOSED' where STRATEGY='"+strategy+"'" 
            cursor_sl.execute(sql)
            con.commit()

        if pnl < 0:
            send_message(row[0]+" Loss !!", '542739135')
            #cancel the pending order
            '''print(row[0])
            for row in cursor.execute("select TARGET_ORDER_ID from dhan_orders where TARGET_ORDER_ID is not null and SECURITYID in ( select SECURITYID from dhan_positions where STRATEGY='"+row[0]+"')"):
                print(row[0])
                send_message("Cancel Order"+row[0], '542739135')
                url_order_canc="https://api.dhan.co/orders/"+str(row[0])
                resp = req.delete(url = url_order_canc, headers = header_pos)
                resp = json.loads(resp.text)'''
            sql="update dhan_positions set STRATEGY='"+strategy+"_LOSS_CLOSED' where STRATEGY='"+strategy+"'"
            cursor_sl.execute(sql)
            con.commit()
    #end=1
    time.sleep(5)
    now = datetime.now()
    print("DHAN Positions Ok..")

    todayclose = now.replace(hour=15, minute=30, second=0, microsecond=0)
    if now >  todayclose:
        end=1

#Cleanup
sql="delete from dhan_positions where NETQTY=0" 
cursor.execute(sql)
sql="delete from dhan_orders" 
cursor.execute(sql)
con.commit()