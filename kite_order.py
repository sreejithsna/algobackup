import kiteconnect
from sample import Functions

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


try:
    order_id = kite.place_order(tradingsymbol='SBIN',exchange="NFO" ,
                            transaction_type='BUY',quantity=1,variety="regular",
                            order_type="MARKET",product="MIS",validity='DAY')
except Exception as e:
    print("Issue in placing trade "+str(e))
