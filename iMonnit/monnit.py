"""
Receives the webhook from the Monnit servers 
and stores the data into an SQL Server database,
as well as processing the data to separate 
concatonated data from the received data.
"""


__author__ = "Ethan Bellmer"
__version__ = "1.0"


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

import os
from tqdm import tqdm

# Envars
WEBHOOK = config('MONNIT_WEBHOOK_TOGGLE', default=True, cast=bool)

DB_DRIVER = config('DB_DRIVER', default = "{ODBC Driver 17 for SQL Server}")
DB_URL = config('AZURE_DB_SERVER')
DB_BATABASE = config('AZURE_DB_DATABASE')
DB_USR = config('AZURE_DB_USR')
DB_PWD = config('AZURE_DB_PWD')


# Formatted connection string for the SQL DB.
SQL_CONN_STR = "DRIVER={0};SERVER={1};Database={2};UID={3};PWD={4};".format(DB_DRIVER, DB_URL, DB_BATABASE, DB_USR, DB_PWD)


# Load lists from env variables
SENSOR_TYPES = config('MONNIT_SENSOR_TYPES', cast=lambda v: [s.strip() for s in v.split(',')])
SENSOR_COLUMNS = config('MONNIT_SENSOR_COLUMNS', cast=lambda v: [s.strip() for s in v.split(',')])
DELIMETERS = config('MONNIT_DELIMETERS', cast=lambda v: [s.strip() for s in v.split(',')])


# Flask web server
app = Flask(__name__)

app.config['BASIC_AUTH_USERNAME'] = config('MONNIT_WEBHOOK_UNAME')
app.config['BASIC_AUTH_PASSWORD'] = config('MONNIT_WEBHOOK_PWD')

app.config['BASIC_AUTH_FORCE'] = True
basic_auth = BasicAuth(app)


# Functions
def process_data(data):
	# Remove the trailing values present in the rawData field of some sensors
	data = remove_trailing_values(data, SENSOR_TYPES)
	# Process any sensor messages for Air Quality
	data = air_quality_processing(data)
	# Split the dataframe to move concatonated values to new rows
	splitDf = split_dataframe_rows(data, SENSOR_COLUMNS, DELIMETERS)


	if 'pendingChange' in splitDf:
		pending_change = 1
		# Use the Pandas 'loc' function to find and replace pending changes in the dataset
		splitDf.loc[(splitDf.pendingChange == 'False'), 'pendingChange'] = 0
		splitDf.loc[(splitDf.pendingChange == 'True'), 'pendingChange'] = 1
	else:
		pending_change = 0


	if WEBHOOK:
		# Connect to DB and get an app context with the connector
		conn = get_db(SQL_CONN_STR)
	else:
		conn = db_connect(SQL_CONN_STR)


	print('Processing ' + str(len(splitDf.index)) + ' rows...')
	for i, sensorData in tqdm(splitDf.iterrows()):
	#for i, sensorData in tqdm(splitDf.iloc[368511:].iterrows()):
		#print("Processing sensor message " + str(i) + ".")

		## CREATE NETWORK ##
		# Prepare SQL statement to call stored procedure to create a network entry using 
		# the networkID from the JSON
		sql = "{CALL [dbo].[PROC_GET_OR_CREATE_NETWORK_MONNIT] (?)}"
		# Bind the parameters that are required for the procedure to function
		params = (sensorData['networkID'])

		# Execute the stored procedure to create a network if it doesn't exist, 
		# and ignore input if exists
		#print('Step 1/10: Creating network entry')
		execute_procedure(conn, sql, params)
		#print('Network entry created')


		## CREATE APPLICATION ##
		# Prepare SQL statement to call stored procedure to create an application entry using 
		# the applicationID from the JSON
		sql = "{CALL [dbo].[PROC_GET_OR_CREATE_APPLICATION_MONNIT] (?)}"
		# Bind the applicationID used to check if app exists in DB
		params = (sensorData['applicationID'])

		# Execute the stored procedure to create an application if it doesn't exist, 
		# and ignore input if exists
		#print('Step 2/10: Creating application entry')
		execute_procedure(conn, sql, params)
		#print('Network application created')


		## GET OR CREATE SENSOR ##
		# pyodbc doesn't support the ".callproc" function from ODBC, 
		# so the following SQL statement is used to execute the stored procedure.
		#
		# Stored prodecure will submit the applicationID, networkID, and sensorName 
		# to the procedure, and will always recieve a sensorID in return.
		sql = """\
			DECLARE @out UNIQUEIDENTIFIER;
			EXEC [dbo].[PROC_GET_OR_CREATE_SENSOR_MONNIT] @applicationID = ?, @networkID = ?, @sensorName = ?, @sensorID = @out OUTPUT;
			SELECT @out AS the_output;
			"""
		# Bind the parameters that are required for the procedure to function
		params = (sensorData['applicationID'], sensorData['networkID'], sensorData['sensorName'])
		
		# Execute the procedure using the prepared SQL & parameters to 
		# create a new sensor in the DB, or get an existing one.
		#print('Step 3/10: Creating or getting sensor')
		# Execute the procedure and return sensorID and convert trimmed string into a GUID (UUID)
		sensorData['sensorID'] = str_to_uuid(execute_procedure(conn, sql, params, True), WEBHOOK)
		#print(sensorData['sensorID'])
		

		## GET OR CREATE DATA TYPE ##
		# Prepare SQL statement to call stored procedure to a data type entry using 
		# the data type from the JSON and return a generated dataTypeID.
		sql = """\
			DECLARE @out UNIQUEIDENTIFIER;
			EXEC [dbo].[PROC_GET_OR_CREATE_DATA_TYPE_MONNIT] @dataType = ?, @dataTypeID = @out OUTPUT;
			SELECT @out AS the_output;
			"""
		# Bind the parameters that are required for the procedure to function
		params = sensorData['dataType']
		
		# Execute the procedure using the prepared SQL & parameters to 
		# create a new sensor in the DB, or get an existing one.
		#print('Step 4/10: Creating or getting data type ID')
		sensorData['dataTypeID'] = str_to_uuid(execute_procedure(conn, sql, params, True), WEBHOOK)
		#print(sensorData['dataTypeID'])


		## GET OR CREATE PLOT LABELS ##
		# Prepare SQL statement to call stored procedure to a plot label entry using 
		# the data type from the JSON and return a generated plotLabelID.
		sql = """\
			DECLARE @out UNIQUEIDENTIFIER;
			EXEC [dbo].[PROC_GET_OR_CREATE_PLOT_LABELS_MONNIT] @plotLabel = ?, @plotLabelID = @out OUTPUT;
			SELECT @out AS the_output;
			"""
		# Bind the parameters that are required for the procedure to function
		params = sensorData['plotLabels']
		# Execute the procedure using the prepared SQL & parameters to 
		# create a new plot label in the DB, or get an existing one.
		#print('Step 5/10: Creating or getting plot label ID')
		sensorData['plotLabelID'] = str_to_uuid(execute_procedure(conn, sql, params, True), WEBHOOK) ## Problem here?
		

		## GET OR CREATE READING ##
		# Prepare SQL statement to call stored procedure to create a new reading using 
		# the values aggregated from the JSON and generated variables from the DB.
		# A generated readingID will be returned. 
		sql = """\
			DECLARE @out UNIQUEIDENTIFIER;
			EXEC [dbo].[PROC_CREATE_READING_MONNIT] @dataMessageGUID = ?, @sensorID = ?, @rawData = ?, @dataTypeID = ?, @dataValue = ?, @plotLabelID = ?, @plotValue = ?, @messageDate = ?, @readingID = @out OUTPUT;
			SELECT @out AS the_output;
			"""
		# Bind the parameters that are required for the procedure to function
		params = (sensorData['dataMessageGUID'], sensorData['sensorID'], sensorData['rawData'], sensorData['dataTypeID'], sensorData['dataValue'], sensorData['plotLabelID'], sensorData['plotValues'], sensorData['messageDate'])
		
		# Execute the procedure using the prepared SQL & parameters to 
		# create a new reading in the DB, and return the genreated ID.
		#print('Step 6/10: Creating reading, and getting ID')
		sensorData['readingID'] = str_to_uuid(execute_procedure(conn, sql, params, True), WEBHOOK)
		

		## GET OR CREATE SIGNAL STATUS ##
		# Prepare SQL statement to call stored procedure to create a new signal 
		# status entry using the readingID from the DB and the dataMessgaeGUID, 
		# and signalStrength, from the JSON.
		sql = "{CALL [dbo].[PROC_CREATE_SIGNAL_STATUS_MONNIT] (?, ?, ?)}"
		# Bind the parameters that are required for the procedure to function
		params = (sensorData['readingID'], sensorData['dataMessageGUID'], sensorData['signalStrength'])
		
		# Execute the procedure using the prepared SQL & parameters to create a new signal status in the DB.
		#print('Step 7/10: Creating signal atatus')
		execute_procedure(conn, sql, params)
		

		## GET OR CREATE BATTERY STATUS ##
		# Prepare SQL statement to call stored procedure to create a new battery status 
		# entry using the readingID from the DB and dataMessgaeGUID, and batteryLevel, from the JSON.
		sql = "{CALL [dbo].[PROC_CREATE_BATTERY_STATUS_MONNIT] (?, ?, ?)}"
		# Bind the parameters that are required for the procedure to function
		params = (sensorData['readingID'], sensorData['dataMessageGUID'], sensorData['batteryLevel'])
		
		# Execute the procedure using the prepared SQL & parameters to 
		# create a new battery status in the DB.
		#print('Step 8/10: Creating battery status')
		execute_procedure(conn, sql, params)
		

		if pending_change:
			## GET OR CREATE PENDING CHANGES ##
			# Prepare SQL statement to call stored procedure to create a pending change 
			# entry using the readingID from the DB and dataMessgaeGUID, 
			# and pendingChange, from the JSON.
			sql = "{CALL [dbo].[PROC_CREATE_PENDING_CHANGES_MONNIT] (?, ?, ?)}"
			# Bind the parameters that are required for the procedure to function
			params = (sensorData['readingID'], sensorData['dataMessageGUID'], sensorData['pendingChange'])
			
			# Execute the procedure using the prepared SQL & parameters to 
			# create a new pending change in the DB.
			#print('Step 9/10: Creating pending change')
			execute_procedure(conn, sql, params)
		

		## GET OR CREATE SENSOR VOLTAGE ##
		# Prepare SQL statement to call stored procedure to create a sensor voltage 
		# entry using the readingID from the DB and dataMessgaeGUID, 
		# and voltage, from the JSON.
		sql = "{CALL [dbo].[PROC_CREATE_SENSOR_VOLTAGE_MONNIT] (?, ?, ?)}"
		# Bind the parameters that are required for the procedure to function
		params = (sensorData['readingID'], sensorData['dataMessageGUID'], sensorData['voltage'])
		
		# Execute the procedure using the prepared SQL & parameters to 
		# create a new voltage entry in the DB.
		#print('Step 10/10: Creating voltage reading')
		execute_procedure(conn, sql, params)

		if WEBHOOK:
			# Commit data and close open database connection
			commit_db()
		else:
			with open(os.getcwd() + "/iMonnit/data/monnit_current_row.txt", 'w') as txt:
				txt.write(str(i))
			conn.commit()

	if WEBHOOK:
		# Commit data and close open database connection
		close_db()
	else:
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