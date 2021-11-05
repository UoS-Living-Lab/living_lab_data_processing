"""
Listens for webhooks from TTN  
for the Salford Living Lab application.
"""


__author__ = "Ethan Bellmer"
__version__ = "1.0"


import requests
from decouple import config
import os
import json


API_ACCESS_TOKEN = config('X_DOWNLINK_APIKEY')

data_dir = (os.getcwd() + '/data/json/')
headers = {"X_DOWNLINK_APIKEY": API_ACCESS_TOKEN}


def send_json():
	all_dicts = []
	for file in os.listdir(data_dir):
		full_filename = "%s/%s" % (data_dir, file)
		with open(full_filename,'r') as fi:
			dict = json.load(fi)
			all_dicts.append(dict)

	i = 0
	for row in all_dicts:
		
		r = requests.post('http://localhost/uplink/messages', json= row, headers= headers)
		print('File ' + str(i) + ' status: ' + str(r.status_code))
		i = i + 1

if __name__ == '__main__':
	send_json()