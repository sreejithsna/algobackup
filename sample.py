# from config import Credentials
# from collections import namedtuple
from kiteext import KiteExt
from requests import get
from json import loads
from datetime import datetime, date, timedelta
from pandas import DataFrame
import pandas as pd
from configs import *
import copy
import onetimepass as otp

class Functions:
    def __init__(self):
        user = loads(open(FILE_LOC+"user_zerodha.json", 'r').read().rstrip())
        self.user = user

    def zerodhaObject(self):
        kite = KiteExt()
        self.kite = kite
        return kite
    
    def order_margins(self, params):
        """
        Calculate margins for requested order list considering the existing positions and open orders

        - `params` is list of orders to retrive margins detail
        """
        return self.kite.order_margins( params=params)

    def loginZerodha(self):
        my_secret=self.user['secret']
        tokens = otp.get_totp(my_secret)
        token = str(tokens)
        print ('token',token)
        if len(token)<6:
            token = "{:06d}".format(int(token))
        self.kite.login_with_credentials(userid=self.user['user_id'], password=self.user['password'], twofa=token)
#    def readZerodhaAccessToken(self):
#        enctoken = open('enctoken.txt', 'r').read().rstrip()
#        self.Authorization = "token kiteios:"+enctoken
#        self.kite = self.zerodhaObject()
#        self.kite.set_headers(self.Authorization)
#        #print (self.kite.headers('Authorization'))
##        headers['Authorization'] = 'token kiteios: {}'.format(enctoken)
        
    def readZerodhaAccessToken(self):
        enctoken = open(FILE_LOC+'enctoken.txt', 'r').read().rstrip()
        self.enctoken = enctoken
        self.kite = self.zerodhaObject()
        self.kite.set_headers(enctoken)
        
    def kiteInstruments(self, NSE):
        return DataFrame(self.kite.instruments(exchange=NSE), index=None)
    
    def historical_data(self, token, from_date, end_date, interval, continuous = False,  getoival = False):
        return self.kite.historical_data(token, from_date, end_date, interval, continuous = continuous,  oi = getoival)
    
    '''
    def historical_data(self, token, from_date, end_date, interval, continuous = False,  getoival = False):
        return DataFrame(self.kite.historical_data(token, from_date, end_date, interval, continuous = continuous,  oi = getoival))
    
    
    def ltp(self, exchg, symbol):
        zscript = f"{exchg}:{symbol}"
        #print(zscript)
        return self.kite.ltp(zscript)
    '''
    def ltp(self, symbol):
        return self.kite.ltp(symbol)

    def ohlc(self,stock):
        # stock=f'NSE:{stock}'
        return self.kite.ohlc(stock)

    def quote(self,stock):
        return self.kite.quote(stock)

    def place_order(self, variety, exchange, tradingsymbol, transaction_type, quantity, product, order_type, price=None, validity=None, disclosed_quantity=None, trigger_price=None, squareoff=None, stoploss=None, trailing_stoploss=None, tag=None):
        return self.kite.place_order(transaction_type=transaction_type, 
                                 tradingsymbol=tradingsymbol,
                                 quantity=quantity, 
                                 product=product, 
                                 order_type=order_type,
                                 variety=variety,
                                 exchange=exchange,
                                 validity=validity,
                                 trigger_price=trigger_price,
                                 price=price)
    
    def modify_order(self, order_id, quantity, variety, order_type, price=None, validity=None, disclosed_quantity=None, trigger_price=None, squareoff=None, stoploss=None, trailing_stoploss=None, tag=None):
        return self.kite.modify_order(order_id=order_id,
                                 quantity=quantity, 
                                 order_type=order_type,
                                 variety=variety,
                                 trigger_price=trigger_price)
        
    def buyorder(self,instrument,quantity):
        return self.kite.place_order(transaction_type=self.kite.TRANSACTION_TYPE_BUY, 
                                 tradingsymbol=instrument,
                                 quantity=quantity, 
                                 product=self.kite.PRODUCT_MIS, 
                                 order_type=self.kite.ORDER_TYPE_MARKET,
                                 variety=self.kite.VARIETY_REGULAR,
                                 exchange=self.kite.EXCHANGE_NSE)

    def sellorder(self,instrument,quantity):
        return self.kite.place_order(transaction_type=self.kite.TRANSACTION_TYPE_SELL, 
                                 tradingsymbol=instrument,
                                 quantity=quantity, 
                                 product=self.kite.PRODUCT_MIS, 
                                 order_type=self.kite.ORDER_TYPE_MARKET,
                                 variety=self.kite.VARIETY_REGULAR,
                                 exchange=self.kite.EXCHANGE_NSE)
    def buyorderop(self,instrument,quantity):
        return self.kite.place_order(transaction_type=self.kite.TRANSACTION_TYPE_BUY, 
                                 tradingsymbol=instrument,
                                 quantity=quantity, 
                                 product=self.kite.PRODUCT_MIS, 
                                 order_type=self.kite.ORDER_TYPE_MARKET,
                                 variety=self.kite.VARIETY_REGULAR,
                                 exchange=self.kite.EXCHANGE_NFO)

    def sellorderop(self,instrument,quantity):
        return self.kite.place_order(transaction_type=self.kite.TRANSACTION_TYPE_SELL, 
                                     tradingsymbol=instrument,
                                     quantity=quantity, 
                                     product=self.kite.PRODUCT_MIS, 
                                     order_type=self.kite.ORDER_TYPE_MARKET,
                                     variety=self.kite.VARIETY_REGULAR,
                                     exchange=self.kite.EXCHANGE_NFO)

    def amolimitsell(self,symbol,qty,price):
        return self.kite.place_order(transaction_type=self.kite.TRANSACTION_TYPE_SELL,
                                 tradingsymbol=symbol, 
                                 quantity=qty, 
                                 price=price,
                                 product=self.kite.PRODUCT_MIS,
                                 order_type=self.kite.ORDER_TYPE_LIMIT, 
                                 variety=self.kite.VARIETY_AMO,
                                exchange=self.kite.EXCHANGE_NSE)

    def orders(self):
        return self.kite.orders()
        
    def margins(self):
        return self.kite.margins()

    def profile(self):
        return self.kite.profile()
    def cancel_order(self,variety,orderID):
        return self.kite.cancel_order(variety=variety, order_id=orderID)

    def kws(self):
        return self.kite.kws()
    
    def positions(self):
        return self.kite.positions()
    
    
    def instruments(self, symbol, expiry):
        self.instruments_dict = {}
        self.option_data = {}
        if self.Exchange is None:
            while True:
                try:
                    self.Exchange = pd.DataFrame(self.kite.instruments("NFO"))
                    break
                except:
                    pass
        if symbol and not expiry is None:
            try:
                df = copy.deepcopy(self.Exchange)
                df = df[(df["segment"] == "NFO-OPT") &
                        (df["name"] == symbol.upper())]
                df = df[df["expiry"] == sorted(list(df["expiry"].unique()))[expiry]]
                for i in df.index:
                    self.instruments_dict[f'NFO:{df["tradingsymbol"][i]}'] = {"Strike": float(df["strike"][i]),
                                                                              "Segment": df["segment"][i],
                                                                 "Instrument Type": df["instrument_type"][i],
                                                                 "Expiry": df["expiry"][i],
                                                                 "Lot": df["lot_size"][i]}
            except:
                pass
        self.prev_info = {"Symbol": symbol, "Expiry": expiry}
        print (self.prev_info)
        return self.instruments_dict
        