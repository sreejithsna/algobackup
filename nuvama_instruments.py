import pandas as pd
import requests
import zipfile
from configs import *
import cx_Oracle 
import numpy as np
from telegram import *

msg=""
# URL of the zip file
url = 'https://client.edelweiss.in/app/toccontracts/instruments.zip'

# Download the zip file
response = requests.get(url)

# Save the contents to a local file
open("instruments.zip", "wb").write(response.content)

# Open the zip file
with zipfile.ZipFile("instruments.zip", "r") as zip_ref:
    # Extract the CSV file to the current working directory
    zip_ref.extract("instruments.csv")

# Load the CSV file into a Pandas dataframe
df = pd.read_csv("instruments.csv")

print("Downloading Instruments data...")

#Oracle Connection
cx_Oracle.init_oracle_client(lib_dir=CLIENT_LOC)
con = cx_Oracle.connect(user="DHAN", password="Welcome12345#", dsn="ssn1", encoding="UTF-8")
cursor = con.cursor()

#Truncate the Scripts
msg="NUVAMA Instrument Load Started...\n"
print("EDELWEISS => Truncating Scripts....")
cursor.execute("TRUNCATE TABLE EDEL_SCRIPTS DROP STORAGE")
for row in cursor.execute("select count(*) from EDEL_SCRIPTS"):
    msg=msg+"TRUNCATED => Row Count :"+str(row[0])+"\n"

#Start Load
print("Loading Scripts....")
print("Loading INDEX Options....")
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
cursor.executemany("Insert into EDEL_SCRIPTS (EXCHANGETOKEN,TRADINGSYMBOL,SYMBOLNAME,DESCRIPTION,EXPIRY,STRIKEPRICE,LOTSIZE,OPTIONTYPE,ASSETTYPE,EXCHANGE) VALUES (:1,:2,:3,:4,to_date(:5,'DD-MON-RR'),:6,:8,:11,:9,:12)")
for error in cursor.getbatcherrors():
    print("Error", error.message, "at row offset", error.offset)
con.commit()
for row in cursor.execute("select count(*) from EDEL_SCRIPTS"):
    msg=msg+"LOADED => Row Count :"+str(row[0])+"\n"
send_message(msg, '542739135') 