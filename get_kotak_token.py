import requests as req
import os
from datetime import datetime

from configs import *
from telegram import *

today = datetime.now().date()

def generate_kotak_token():
    ott=""
    sesstoken=""
    text=""
    
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
            send_message("Enter Kotak Access Code", '542739135')
            print(text)
            time.sleep(30)
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
        send_message("Kotak Token Generated..", '542739135')