import requests
import json
url = "https://www.edelweiss.in/api-connect/login?api_key=Mygj2rZICb-eOw"
r = requests.get(url = url)
response_dict = json.loads(r.text)

for i in response_dict:
    print("key: ", i, "val: ", response_dict[i])