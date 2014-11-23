# connect to salsify and do rest operations

import requests
import json

def read_configuration():
    config = {}
    execfile("configuration.conf",config)
    return config

class salsifyed:


   def export_channel_data(self):
       config_values = read_configuration()
       api_key = config_values["salsify_api"]
       url = "https://app.salsify.com/api/channels/965/runs"
       headers = {}
       headers['X-AUTH-TOKEN'] = api_key
       print headers
       req = requests.post(url, data="{}", headers=headers)
       print req.


obj = salsifyed()
obj.export_channel_data()





