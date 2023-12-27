import cx_Oracle 
from APIConnect.APIConnect import APIConnect
import os
import json
import time
from configs import *
from datetime import datetime
from datetime import timedelta
from get_edel_token import *
from telegram import *
from sample import *

edeltoken=""

def read_tokens(broker):
    token_file="edel_tokens.txt"
    file1 = open(FILE_LOC+token_file, "r")
    Lines = file1.readlines()
    edeltoken=Lines[0].replace("\n",'')
       
def get_tokens(broker):

    token_file="edel_tokens.txt"

    try:
        filetime = datetime.fromtimestamp(os.path.getctime(FILE_LOC+token_file))
        print(broker+' Token File Available ...')
        if filetime.date() == today:
            read_tokens(broker)
        else:
            print('Generating '+broker+' Tokens...')
            generate_edel_token()   
            read_tokens(broker)          
    except OSError:
        print('Generating '+broker+' Tokens...')
        generate_edel_token()
        read_tokens(broker)

def get_ltp (symbol):
    for row in cursor_kite.execute("SELECT exchange|| ':'|| p.tradingsymbol FROM zerodha_scripts p WHERE CASE WHEN p.TRADINGSYMBOL LIKE 'NIFTY 50' THEN 'NIFTY' WHEN p.TRADINGSYMBOL LIKE 'NIFTY BANK' THEN 'BANKNIFTY' ELSE p.TRADINGSYMBOL END LIKE '"+symbol+"' and SEGMENT= CASE WHEN TRADINGSYMBOL LIKE 'NIFTY 50' THEN 'INDICES' WHEN p.TRADINGSYMBOL LIKE 'NIFTY BANK' THEN 'INDICES' ELSE 'NSE' END"):
        try:
            resp=kite.ltp(row[0])
            for key in resp.keys():
                ltp=round(resp[key].get("last_price"))
                return ltp    
            print(ltp)   
        except Exception as e :
            print('Error LTP Zerodha.')
            error = e
            print (error.code , error.message )

def execute_sql (sql):
    try:
        cursor.execute(sql)
        con.commit()
    except cx_Oracle.Error :
        print("SQL :",sql)
        os.system('say ' "Database Error")
        error = e
        print (error.code , error.message )

#CODE MAIN STARTS FROM HERE

#Initialize

today = datetime.now().date()

print("-----------------------------------------------------")
#Oracle Connection
cx_Oracle.init_oracle_client(lib_dir=CLIENT_LOC)
con = cx_Oracle.connect(user="DHAN", password="Welcome12345#", dsn="ssn1", encoding="UTF-8")
cursor = con.cursor()
cursor_sl = con.cursor()
cursor_kite = con.cursor()

get_tokens("edelweiss")
kite=Functions()
kite.readZerodhaAccessToken()

file1 = open(FILE_LOC+"kotak_tokens.txt", "r")
Lines = file1.readlines()
ott=Lines[0].replace("\n",'')
sesstoken=Lines[1]

header_ltp_kotak = {
    "accept": "application/json",
	"consumerKey": "Z3l254sO82sHdPjpm80Ahbkmru0a",
	"sessionToken": sesstoken,
    "Authorization": "Bearer d0337058-675c-352f-ab63-247ae9e61b9a"
}

file1 = open(FILE_LOC+"edel_tokens.txt", "r")
Lines = file1.readlines()
edeltoken=Lines[0].replace("\n",'')
print("Edeltoken",edeltoken)

if [ edeltoken !="" ]:
    edel=APIConnect("Mygj2rZICb-eOw","UTc!829BWjDp9%JP",edeltoken,True)
else:
    print("ERROR : edeltoken issue")
    exit(1)

#Update Margin   
resp=edel.Limits()
resp=json.loads(resp)["eq"]["data"]["cshAvl"]
print(resp)
sql="UPDATE MARGIN set AVAILABELBALANCE="+str(resp) 
cursor.execute(sql) 
con.commit()

start=1
end=0
pos_count=0

while start == 1 and end == 0:
    resp=edel.NetPosition()
    resp=json.loads(resp)["eq"]["data"]["pos"]
    if pos_count == 0:
        pos_count=len(resp)
        send_message("EDELWEISS STARTED...", '542739135')
        send_message("Positions Retrived =>"+str(pos_count), '542739135')

    for i in range(len(resp)):
        if resp[i].get("asTyp") != "EQUITY":
            sql="MERGE INTO positions p USING (SELECT '"+str(resp[i].get("sym"))+"' securityId,'"+str(resp[i].get("prdCode"))+"' POS_TYPE from dual) s ON ( p.securityId = s.securityId and p.POS_TYPE=s.POS_TYPE ) WHEN MATCHED THEN UPDATE SET p.DHANCLIENTID = '45009940',PRODUCTTYPE='"+str(resp[i].get("asTyp"))+"',BUYAVG="+str(resp[i].get("avgByPrc"))+",SELLAVG="+str(resp[i].get("avgSlPrc"))+",LTP="+str(resp[i].get("ltp"))+",NETQTY="+str(resp[i].get("ntQty"))+",REALIZEDPROFIT="+str(resp[i].get("rlzPL"))+",UNREALIZEDPROFIT="+str(resp[i].get("urlzPL"))+",DRVEXPIRYDATE='"+str(resp[i].get("dpExpDt")).replace("'"," ")+"',DRVSTRIKEPRICE=CASE when '"+str(resp[i].get("asTyp"))+"'='FUTIDX' then NULL else '"+str(resp[i].get("stkPrc"))+"' end,DP_NAME='"+str(resp[i].get("dpName"))+"',DRVOPTIONTYPE='"+str(resp[i].get("opTyp"))+"',TRADINGSYMBOL='"+str(resp[i].get("trdSym"))+"' WHEN NOT MATCHED THEN insert (DHANCLIENTID,TRADINGSYMBOL,PRODUCTTYPE,BUYAVG,DRVEXPIRYDATE,NETQTY,SELLAVG,DRVSTRIKEPRICE,DRVOPTIONTYPE,SECURITYID,REALIZEDPROFIT,UNREALIZEDPROFIT,LTP,DP_NAME,POS_TYPE) values('45009940','"+str(resp[i].get("trdSym"))+"','"+str(resp[i].get("asTyp"))+"',"+str(resp[i].get("avgByPrc"))+",'"+str(resp[i].get("dpExpDt")).replace("'"," ")+"',"+str(resp[i].get("ntQty"))+","+str(resp[i].get("avgSlPrc"))+",case when '"+str(resp[i].get("asTyp"))+"'='FUTIDX' then NULL else '"+str(resp[i].get("stkPrc"))+"' end,'"+str(resp[i].get("opTyp"))+"','"+str(resp[i].get("sym"))+"','"+str(resp[i].get("rlzPL"))+"','"+str(resp[i].get("urlzPL"))+"','"+str(resp[i].get("ltp"))+"','"+str(resp[i].get("dpName"))+"','"+str(resp[i].get("prdCode"))+"')"
            execute_sql(sql)
    con.commit()
    print("Positions Ok")

    for row in cursor.execute("SELECT a.group_name, ( SELECT SUM(nvl(unrealizedprofit,0)) FROM positions WHERE group_name = a.group_name  and pos_type = 'NRML' ) unrealizedprofit, ( SELECT SUM(realizedprofit) FROM ( SELECT nvl(SUM(realizedprofit), 0) realizedprofit FROM positions WHERE group_name = a.group_name and POS_TYPE='NRML'  UNION SELECT nvl(SUM(realizedprofit),0) FROM ( SELECT nvl(SUM(realizedprofit), 0) realizedprofit FROM positions_archive  WHERE group_name = a.group_name and POS_TYPE='NRML'))) realizedprofit, a.symbol FROM ( SELECT DISTINCT group_name,dp_name symbol FROM positions WHERE POS_TYPE='NRML') a"):
        try:
            try:
                ltp=get_ltp(row[3])
            except:
                print("Error Fetching LTP..")
                ltp=0
            
            sql="MERGE INTO MY_STRATEGIES p USING (SELECT  '"+row[0]+"' group_name from dual ) a ON ( a.group_name = p.STRATEGY_NAME ) WHEN MATCHED THEN UPDATE SET UNREALISED_PNL="+str(row[1])+", REALISED_PNL="+str(row[2])+",SYMBOL='"+row[3]+"',LTP="+str(ltp)+" WHEN NOT MATCHED THEN insert (STRATEGY_NAME,UNREALISED_PNL,REALISED_PNL,SYMBOL,LTP) values ('"+row[0]+"',"+str(row[1])+","+str(row[2])+",'"+str(row[3])+"', "+str(ltp)+" )"
            #print(sql)
            cursor_sl.execute(sql)    
        except cx_Oracle.Error :
            print('Error LTP Group...')
            error = e
            print (error.code , error.message )
    
    con.commit()
    print("Groups Ok")

    for row in cursor.execute("select distinct SYMBOL,SYMBOL||' : '|| ltp||' '||case when last_ltp < ltp then 'UP' else 'DOWN' end ||' by '||round(abs((last_ltp-ltp) /last_ltp *100),2) ||' %' msg from my_strategies  where round(abs((last_ltp-ltp) /last_ltp *100)) > 0.5 and ltp <> 0"):
        sql="UPDATE MY_STRATEGIES set LAST_LTP=case when LTP=0 then last_ltp else ltp end where symbol='"+row[0]+"'"
        cursor_sl.execute(sql) 
        send_message(row[1], '542739135')
    con.commit()

    #end=1
    time.sleep(10)
    now = datetime.now()
    todayclose = now.replace(hour=15, minute=30, second=0, microsecond=0)
    if now >  todayclose:
        end=1

#Cleanup
sql="insert into positions_archive  select * from positions where REALIZEDPROFIT<>0" 
execute_sql(sql)
sql="update positions set CLOSED_DATE=sysdate where SECURITYID in ( select EXCHANGETOKEN from edel_scripts where trunc(EXPIRY)<=trunc(sysdate)) and closed_date is null" 
execute_sql(sql)
sql="insert into positions_archive select * from positions where SECURITYID in ( select EXCHANGETOKEN from edel_scripts where trunc(EXPIRY)<=trunc(sysdate)) and (TRADINGSYMBOL,REALIZEDPROFIT,GROUP_NAME) not in  (select TRADINGSYMBOL,REALIZEDPROFIT,GROUP_NAME from positions_archive)" 
execute_sql(sql)
sql="insert into  edel_orders_archive select * from edel_orders" 
execute_sql(sql)
sql="update positions_archive set REALIZEDPROFIT=UNREALIZEDPROFIT where REALIZEDPROFIT=0" 
execute_sql(sql)
sql="delete from positions where SECURITYID in ( select EXCHANGETOKEN from edel_scripts where trunc(EXPIRY)<=trunc(sysdate))" 
execute_sql(sql)
sql="delete from edel_orders" 
execute_sql(sql)
sql="delete from positions where REALIZEDPROFIT<>0 and UNREALIZEDPROFIT=0" 
execute_sql(sql)
sql="DELETE FROM MY_STRATEGIES where STRATEGY_NAME not in ( select group_name from positions)" 
execute_sql(sql)
sql="UPDATE MY_STRATEGIES set LAST_LTP=case when LTP=0 then LAST_LTP else LTP end" 
cursor.execute(sql) 
con.commit()

