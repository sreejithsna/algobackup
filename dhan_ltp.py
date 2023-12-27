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

text=""
today = datetime.now().date()

def execute_sql (sql):
    try:
        cursor.execute(sql)
        con.commit()
    except cx_Oracle.Error :
        print("SQL :",sql)
        os.system('say ' "Database Error")
        error = e
        print (error.code , error.message )

def update_ltp(con,cursor,cursor_sl):
    for row in cursor.execute("select p.TRADINGSYMBOL,s.INSTRUMENTTOKEN,p.SECURITYID from kotak_scripts s, positions p where  substr(p.TRADINGSYMBOL, 0, instr(p.TRADINGSYMBOL, '-', 1, 1)-1) = s.instrumentname and s.STRIKE=p.DRVSTRIKEPRICE and s.OPTIONTYPE=case when p.DRVOPTIONTYPE='CALL' then 'CE'  when p.DRVOPTIONTYPE='PUT' then 'PE' else 'FUT' end and s.EXPIRY=to_date(p.DRVEXPIRYDATE,'YYYY-MM-DD')"):
        try:
            ltp_url="https://ctradeapi.kotaksecurities.com/apim/quotes/v1.0/ltp/instruments/"+row[1]
            resp = req.get(url = ltp_url, headers = header_ltp_kotak)    
            resp = json.loads(resp.text)
            ltp=resp['success'][0].get("lastPrice")
            time.sleep(2)
            sql="update positions set ltp=round("+ltp+",2) where SECURITYID='"+row[2]+"'"
            #print(sql)
            cursor_sl.execute(sql)
        except :
            print('Error LTP Positions...')
    con.commit() 
    print('Update Positions LTP OK...')

def update_pnl(con,cursor,cursor_sl):
    for row in cursor.execute("SELECT a.group_name, ( SELECT p.instrumenttoken FROM kotak_scripts p WHERE p.optiontype = 'XX' AND p.instrumentname =a.symbol AND expiry IN ( SELECT MIN(expiry) FROM kotak_scripts WHERE optiontype = 'XX' AND instrumentname = p.instrumentname)) SECURITYID, (select sum(UNREALIZEDPROFIT)  FROM positions where GROUP_NAME=a.GROUP_NAME and realizedprofit is null) UNREALIZEDPROFIT,(  select sum (realizedprofit) from ( SELECT nvl(SUM(realizedprofit), 0) realizedprofit FROM positions WHERE group_name = a.group_name and trunc(positions.closed_date)=trunc(sysdate) union select sum(PNL) realizedprofit from pnl where STRATEGY_NAME= a.group_name)) REALIZEDPROFIT,SYMBOL FROM ( SELECT DISTINCT group_name, TRIM(substr(tradingsymbol, 0, instr(tradingsymbol, '-', 1, 1) - 1)) symbol  FROM positions) a"):
        try:
            ltp_url="https://ctradeapi.kotaksecurities.com/apim/quotes/v1.0/ltp/instruments/"+row[1]
            resp = req.get(url = ltp_url, headers = header_ltp_kotak)    
            resp = json.loads(resp.text)
            ltp=resp['success'][0].get("lastPrice")
            #print(ltp)
            time.sleep(2)
            sql="MERGE INTO MY_STRATEGIES p USING (SELECT  '"+row[0]+"' group_name from dual ) a ON ( a.group_name = p.STRATEGY_NAME ) WHEN MATCHED THEN UPDATE SET SECURITYID='"+row[1]+"',UNREALISED_PNL="+str(row[2])+", REALISED_PNL="+str(row[3])+",LTP=round("+str(ltp)+"),SYMBOL='"+row[4]+"' WHEN NOT MATCHED THEN insert (STRATEGY_NAME,UNREALISED_PNL,REALISED_PNL,SECURITYID,LTP,SYMBOL) values ('"+row[0]+"',"+str(row[2])+","+str(row[3])+",'"+row[1]+"',round("+str(ltp)+"),'"+row[4]+"')"
            #print(sql)
            cursor_sl.execute(sql)         
        except :
            print('Error LTP Group...')
    sql="DELETE FROM MY_STRATEGIES where STRATEGY_NAME not in ( select group_name from positions)" 
    cursor_sl.execute(sql) 

    con.commit() 
    print('Update Groups OK...')

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

#con = cx_Oracle.connect(user="DHAN", password="Welcome12345#", dsn="dhan_high", encoding="UTF-8")
#cursor = con.cursor()
#cursor_sl = con.cursor()

url_login_kotak="https://ctradeapi.kotaksecurities.com/apim/session/1.0/session/login/userid"
url_access_kotak="https://ctradeapi.kotaksecurities.com/apim/session/1.0/session/2FA/accesscode"
url_session_kotak="https://ctradeapi.kotaksecurities.com/apim/session/1.0/session/2FA/accesscode"

header_login_kotak = {
    "accept": "application/json",
	"consumerKey": "Z3l254sO82sHdPjpm80Ahbkmru0a",
	"Content-Type": "application/json",
	"Authorization": "Bearer d0337058-675c-352f-ab63-247ae9e61b9a"
}
payload_login_kotak = {
		"userid" : "SSNAIR",
		"password" : "Mar2022#"
}

#Login to Kotak
if ott == "":
    resp = req.post(url = url_login_kotak, headers = header_login_kotak, data = json.dumps(payload_login_kotak))       
    if "200" in str(resp):
        print("Login OK !!!")
    else:
        print("ERROR : Login Failed !!!!")
        resp = json.loads(resp.text)
        print(resp)
        exit()
        
    resp = json.loads(resp.text)
    if "Success" in str(resp):
        print("Access OK !!!")
    else:
        print("ERROR : Access Failed !!!!")
        print(resp)
        exit()
    ott=resp["Success"].get("oneTimeToken")
    #print("OTT==> "+ ott)
    header_access_kotak = {
        "accept": "application/json",
        "consumerKey": "Z3l254sO82sHdPjpm80Ahbkmru0a",
        "oneTimeToken": ott,
        "appId" : "SSN",
        "ip" : "127.0.0.1",
        "userid" : "SSNAIR",
        "Content-Type": "application/json",
        "Authorization": "Bearer d0337058-675c-352f-ab63-247ae9e61b9a"
    }
    resp = req.get(url = url_access_kotak, headers = header_access_kotak)   
    resp = json.loads(resp.text)
    print(resp["Success"].get("message"))

    ott=resp["Success"].get("oneTimeToken")
    print("OTT==> "+ ott)

    while text == "":
        send_message("Enter Access Code", '542739135')
        print(text)
        time.sleep(20)
        text, chat = get_last_chat_id_and_text(get_updates())
        print("Access Code",text)
    
    access_code=text
    #access_code=input("Enter Access Code in SMS     :")

    header_session_kotak = {
        "accept": "application/json",
        "consumerKey": "Z3l254sO82sHdPjpm80Ahbkmru0a",
        "oneTimeToken": ott,
        "appId" : "SSN",
        "ip" : "127.0.0.1",
        "userid" : "SSNAIR",
        "Content-Type": "application/json",
        "Authorization": "Bearer d0337058-675c-352f-ab63-247ae9e61b9a"
    }
    payload_session_kotak = {
            "userid" : "SSNAIR",
            "accessCode" : access_code
    }
    resp = req.post(url = url_session_kotak, headers = header_session_kotak, data = json.dumps(payload_session_kotak))    
    resp = json.loads(resp.text)
    #print(resp)
    sesstoken=resp["success"].get("sessionToken")
    print("Session Token ==> "+sesstoken)
    f = open(FILE_LOC+"kotak_tokens.txt", "w")
    f.write(ott+"\n")
    f.write(sesstoken)
    f.close()
    #exit()
    send_message("Started Kotak..", '542739135')
header_ltp_kotak = {
    "accept": "application/json",
	"consumerKey": "Z3l254sO82sHdPjpm80Ahbkmru0a",
	"sessionToken": sesstoken,
    "Authorization": "Bearer d0337058-675c-352f-ab63-247ae9e61b9a"
}


