from APIConnect.APIConnect import APIConnect
import requests
from configs import *
import pandas as pd
import cx_Oracle 
from telegram import *
from get_edel_token import *

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

get_tokens("edelweiss")

file1 = open(FILE_LOC+"edel_tokens.txt", "r")
Lines = file1.readlines()
edeltoken=Lines[0].replace("\n",'')
print("Edeltoken",edeltoken)
edel = APIConnect("Mygj2rZICb-eOw","UTc!829BWjDp9%JP", edeltoken, True)

#Oracle Connection
cx_Oracle.init_oracle_client(lib_dir=CLIENT_LOC)
con = cx_Oracle.connect(user="DHAN", password="Welcome12345#", dsn="ssn1", encoding="UTF-8")
cursor = con.cursor()
msg="EDELWEISS Instrument Load Started...\n"
print("EDELWEISS  => Truncating EDEL_SCRIPTS....")
cursor.execute("TRUNCATE TABLE EDEL_SCRIPTS DROP STORAGE")
print("Loading Scripts....")
for row in cursor.execute("select count(*) from EDEL_SCRIPTS"):
    msg=msg+"TRUNCATED => Row Count :"+str(row[0])+"\n"

'''for instument in  edel.instruments :
    if instument['assettype'] != "EQUITY" and ( instument['assettype']== 'FUTIDX' or instument['assettype']== 'OPTIDX' ) :
        try:
            sql="Insert into EDEL_SCRIPTS (EXCHANGETOKEN,TRADINGSYMBOL,SYMBOLNAME,DESCRIPTION,EXPIRY,STRIKEPRICE,LOTSIZE,OPTIONTYPE,ASSETTYPE,EXCHANGE) values ('"+instument['exchangetoken']+"','"+instument['tradingsymbol']+"','"+instument['symbolname']+"','"+instument['description']+"',to_date('"+instument['expiry']+"','DD/MON/RR'),"+instument['strikeprice']+","+instument['lotsize']+",'"+instument['optiontype']+"','"+instument['assettype']+"','"+instument['exchange']+"')"
            cursor.execute(sql) 
            print("Loaded =>",instument['tradingsymbol'])
        except cx_Oracle.Error :
            print('Error Loading Scripts...')
            print(sql)
            error = e
            print (error.code , error.message )
        con.commit()'''
print("Loading INDEX Options....")
df = pd.DataFrame(edel.instruments)
instruments=df[df["assettype"]=="OPTIDX"]
instruments=instruments.drop(['ticksize', 'series','isin','qtyunits','prcunits','prcqtn','multiplier'], axis=1)
rows = [tuple(x) for x in instruments.values]
#print(rows)
cursor.executemany("Insert into EDEL_SCRIPTS (EXCHANGETOKEN,TRADINGSYMBOL,SYMBOLNAME,DESCRIPTION,EXPIRY,STRIKEPRICE,LOTSIZE,OPTIONTYPE,ASSETTYPE,EXCHANGE) VALUES (:1,:2,:3,:4,to_date(:5,'DD-MON-RR'),:6,:8,:11,:9,:12)",rows)
for error in cursor.getbatcherrors():
    print("Error", error.message, "at row offset", error.offset)
con.commit()

print("Loading FUTURE Options....")
instruments=df[df["assettype"]=="FUTIDX"]
instruments=instruments.drop(['ticksize', 'series','isin','qtyunits','prcunits','prcqtn','multiplier'], axis=1)
rows = [tuple(x) for x in instruments.values]
#print(rows)
cursor.executemany("Insert into EDEL_SCRIPTS (EXCHANGETOKEN,TRADINGSYMBOL,SYMBOLNAME,DESCRIPTION,EXPIRY,STRIKEPRICE,LOTSIZE,OPTIONTYPE,ASSETTYPE,EXCHANGE) VALUES (:1,:2,:3,:4,to_date(:5,'DD-MON-RR'),:6,:8,:11,:9,:12)",rows)
for error in cursor.getbatcherrors():
    print("Error", error.message, "at row offset", error.offset)
con.commit()
for row in cursor.execute("select count(*) from EDEL_SCRIPTS"):
    msg=msg+"LOADED => Row Count :"+str(row[0])+"\n"
send_message(msg, '542739135')       