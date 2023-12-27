import cx_Oracle 
import EdelweissAPIConnect
import json
import time
import os
from datetime import datetime
from configs import *
from telegram import *

#Oracle Connection
cx_Oracle.init_oracle_client(lib_dir=CLIENT_LOC)
con = cx_Oracle.connect(user="DHAN", password="Welcome12345#", dsn="ssn1", encoding="UTF-8")
cursor = con.cursor()
cursor_sl = con.cursor()
edel_token=""
print("-----------------------------------------------------")

def execute_sql (sql):
    try:
        cursor.execute(sql)
        con.commit()
    except cx_Oracle.Error :
        print("SQL :",sql)
        os.system('say ' "Database Error")
        error = e
        print (error.code , error.message )


while edel_token == "":
    print("Looking for Edelweiss Token..")
    try:
        file1 = open(FILE_LOC+"edel_tokens.txt", "r")
        Lines = file1.readlines()
        edel_token=Lines[0].replace("\n",'')
    except OSError:
        print("Waiting for Edelweiss Token..")
    time.sleep(5)

print("ORDER:edel_token",edel_token)
if [ edel_token !="" ]:
    edel=EdelweissAPIConnect.EdelweissAPIConnect("Mygj2rZICb-eOw","UTc!829BWjDp9%JP",edel_token,True)
else:
    print("ERROR : edeltoken issue in orders")
    exit(1)
start=1
end=0
order_flag=0
send_message("EDEL Order Daemon Started...", '542739135')

while start == 1 and end == 0:
    for row in cursor.execute("SELECT SYMBOL_TOKEN,ACTION,abs(QTY),TRADINGSYMBOL,ORDER_TYPE from EDEL_ORDERS where STATUS='New' order by qty desc"):
        resp=edel.PlaceTrade(row[3],"NFO",row[1],"DAY","MARKET",row[2],row[0],0,0,0,row[4])
        resp_msg=json.loads(resp)["data"]["msg"]
        orderid=json.loads(resp)["data"]["oid"]
        sql="update EDEL_ORDERS set RETURN_STATUS='"+resp_msg+"',STATUS='Completed',ORDER_STATUS='Pending',ORDER_ID='"+orderid+"' where SYMBOL_TOKEN='"+row[0]+"' and ORDER_ID is null"
        cursor_sl.execute(sql)
        #print(row[3],json.loads(resp)["data"]["msg"])
        time.sleep(2)
        
    con.commit()
    time.sleep(5)

    for row in cursor.execute("SELECT SYMBOL_TOKEN,TRADINGSYMBOL,ORDER_ID from EDEL_ORDERS where ORDER_STATUS='Pending' and ORDER_ID is not null"):
        resp=edel.OrderBook()
        resp=json.loads(resp)["eq"]["data"]["ord"]
        #print(resp)
        for i in range(len(resp)):
            if resp[i]["ordID"] == row[2]:
                sql="update edel_orders set ORDER_STATUS='"+resp[i]["sts"]+"', ORDER_COMMENTS='"+resp[i]["rjRsn"]+"' where ORDER_ID='"+resp[i]["ordID"]+"'"
                cursor_sl.execute(sql)
                send_message(row[1]+"=>"+resp[i]["sts"], '542739135')
                con.commit()
                order_flag=1   

    if  order_flag != 0 :
        #Update Margin   
        resp=edel.Limits()
        resp=json.loads(resp)["eq"]["data"]["cshAvl"]
        sql="UPDATE MARGIN set AVAILABELBALANCE="+str(resp) 
        cursor.execute(sql) 
        con.commit()
        order =0
        print("Updated Margin..")

    print("Order Daemon OK..")

    now = datetime.now()
    todayclose = now.replace(hour=15, minute=30, second=0, microsecond=0)
    if now >  todayclose:
        end=1

    