"""
Listens for POST requests from the intermediary device
"""


__author__ = "Ethan Bellmer"
__version__ = "0.1"





# Import libraries
from flask import Flask, request
from flask.wrappers import Response
from flask_basicauth import BasicAuth
from werkzeug.serving import WSGIRequestHandler
import pandas as pd
from pandas import json_normalize
from decouple import config

from living_lab_functions.db import db_connect, execute_procedure, get_db, commit_db, close_db
from living_lab_functions.functions import str_to_uuid, split_dataframe_rows, remove_trailing_values, air_quality_processing

import json

from os import read
import requests
from datetime import datetime




# Envars
WEBHOOK = config('MONNIT_WEBHOOK_TOGGLE', default=True, cast=bool)

DB_DRIVER = config('DB_DRIVER')
DB_URL = config('AZURE_DB_SERVER')
DB_BATABASE = config('AZURE_DB_DATABASE')
DB_USR = config('AZURE_DB_USR')
DB_PWD = config('AZURE_DB_PWD')

# Formatted connection string for the SQL DB.
SQL_CONN_STR = "DRIVER={0};SERVER={1};Database={2};UID={3};PWD={4};".format(DB_DRIVER, DB_URL, DB_BATABASE, DB_USR, DB_PWD)

# Load lists from env variables
SENSOR_TYPES = config('MONNIT_SENSOR_TYPES', cast=lambda v: [s.strip() for s in v.split(',')])
SENSOR_COLUMNS = config('MONNIT_SENSOR_COLUMNS', cast=lambda v: [s.strip() for s in v.split(',')])
DELIMETERS = config('MONNIT_DELIMETERS', cast=lambda v: [s.strip() for s in v.split('*')])



# Flask web server
app = Flask(__name__)

app.config['BASIC_AUTH_USERNAME'] = config('NOVUS_WEBHOOK_UNAME')
app.config['BASIC_AUTH_PASSWORD'] = config('NOVUS_WEBHOOK_PWD')

app.config['BASIC_AUTH_FORCE'] = True
basic_auth = BasicAuth(app)



def do_db_stuff():	# PLACEHOLER
	#db Stuff
	conn = db_connect(SQL_CONN_STR)
	
	unitsList = get_units() # Get GUIDs for all connected devices via SEL API
	

	# Create new units & get sensor data from API
	for i, row in enumerate(unitsList):
		unitGUIDs.append(str_to_uuid(create_units(conn, row)))	# Create unit using retrieved data, or return unit GUID
		readingsJSON.append(get_data(row['id']))	# Get data for current sensor in loop, returns JSON
		

	for i, row in enumerate(readingsJSON):	
		create_request(conn, row, unitGUIDs[i])		# Insertion order: 06
		create_reading(conn, row, unitGUIDs[i])		# Insertion order: 10
		create_output(conn, row, unitGUIDs[i])		# Insertion order: 08
		create_alarm(conn, row, unitGUIDs[i])		# Insertion order: 11

	conn.commit()
	conn.close()





@app.route('/', methods=['POST'])
@basic_auth.required
# Primary function
def monnit_webhook():
	print('Request Authenticated')

	# Store the recieved JSON file from the request 
	jsonLoad = request.json
	
	# Reveived JSON contains two JSON objects. Sensor and Gateway messages 
	# Only sensor messages are needed
	sensorMessages = jsonLoad['sensorMessages']
	# Convert the JSONs into pandas dataframes
	sensorMessages = json_normalize(sensorMessages)

	process_data(sensorMessages)

	# Return status 200 (success) to the remote client
	status_code = Response(status=200)
	return status_code


# Main body
if __name__ == '__main__':
	if WEBHOOK:
		WSGIRequestHandler.protocol_version = "HTTP/1.1"
		app.run(host = '0.0.0.0', port = '80')
	else:
		print("Processing CSV")

		sensor_data = pd.read_csv(os.getcwd() + "/iMonnit/data/living_lab_monnit_2020-11-01_2021-12-14.csv", low_memory=False)
		process_data(sensor_data)