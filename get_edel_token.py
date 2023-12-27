import requests as req
import os
from datetime import datetime

from configs import *
from telegram import *

today = datetime.now().date()
def generate_edel_token():
    text=""
    while text == "":
        send_message("Enter EDELWEISS Access Token", '542739135')
        print("Waiting for Edelweiss Token...")
        time.sleep(60)
        text, chat = get_last_chat_id_and_text(get_updates())
        print("Edelweiss Token",text)
    print("Edel Token ==> "+text)
    f = open(FILE_LOC+"edel_tokens.txt", "w")
    f.write(text+"\n")
    f.close()
    #exit()
