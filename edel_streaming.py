import EdelweissAPIConnect
import json

def getStreamingData(message, feed):
    print('inside get streaming data')
    print(message)

#feed=EdelweissAPIConnect.Feed('45009940','45009940',conf="/Library/Frameworks/Python.framework/Versions/3.7/conf/settings.ini")

#feed.subscribe("55279_NFO",getStreamingData,True,True)

edel=EdelweissAPIConnect.EdelweissAPIConnect("Mygj2rZICb-eOw","UTc!829BWjDp9%JP","326336e61ea85ca9",True)

resp=edel.OrderBook()
resp=json.loads(resp)["eq"]["data"]["ord"]
print(resp)

for i in range(len(resp)):
    print(resp[i]["trdSym"],resp[i]["sts"],resp[i]["rjRsn"],resp[i]["sym"])
