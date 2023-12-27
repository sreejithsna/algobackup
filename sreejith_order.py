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
order=[
{
"exchange": "NSE",
"tradingsymbol": "INFY",
"transaction_type": "SELL",
"quantity": 1,
"order_type": "LIMIT",
"product": "CNC",
"price": 1480
},
{
"exchange": "NSE",
"tradingsymbol": "INFY",
"transaction_type": "SELL",
"quantity": 1,
"order_type": "LIMIT",
"product": "CNC",
"price": 1490
}
]
resp=kite.gtt_order(exchange="NSE",tradingsymbol="INFY",trigger_values=[1480,1490],last_price=1484.15,orders=order)
print(resp["trigger_id"])