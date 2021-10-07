"""
Polls the SEL RESTful API for water data 
for the Salford Living Lab.
"""


__author__ = "Ethan Bellmer"
__version__ = "0.1"


from os import read
import pandas as pd
import pyodbc
import requests
import datetime
from decouple import config
import ast

from db import db_connect, execute_procedure, execute_procedure_no_return
from functions import str_to_uuid

import json


BASE_URL = config('SEL_API_URL')
USER_KEY = config('SEL_USER_KEY')
API_KEY = config('SEL_API_KEY')

DB_URL = config('AZURE_DB_SERVER')
DB_BATABASE = config('AZURE_DB_DATABASE')
DB_USR = config('AZURE_DB_USR')
DB_PWD = config('AZURE_DB_PWD')


# Formatted connection string for the SQL DB.
SQL_CONN = "DSN={0};Database={1};UID={2};PWD={3};".format(DB_URL, DB_BATABASE, DB_USR, DB_PWD)


# Gets the units available via the API
def get_units():
	API_ACTION = "units"
	FULL_URL = "{0}?user_key={1}&api_key={2}&action={3}".format(BASE_URL, USER_KEY, API_KEY, API_ACTION)

	response = requests.get(FULL_URL)
	if (response.status_code != 200):
		print('Error occurred, status: ' + str(response.status_code))
	else:
		print("Unit endpoint status: " + str(response.status_code))
		recJSON = response.json()
		unitsList = recJSON['units']
		return unitsList


# Gets data from a specific unit using the passed in ID parameter. 
def get_data(unitID):
	API_ACTION = "data"
	FULL_URL = "{0}?user_key={1}&api_key={2}&action={3}&interface={4}".format(BASE_URL, USER_KEY, API_KEY, API_ACTION, unitID)

	response = requests.get(FULL_URL)
	if (response.status_code != 200):
		print('Error occurred, status: ' + str(response.status_code))
	else:
		print("Data endpoint status: " + str(response.status_code))
		recJSON = response.json()

		return recJSON


# Creates a Unit entry in the database using a passed in dict
def create_units(conn, unit):
	print('Create Unit')

	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_GET_OR_CREATE_SEL_UNIT] @unitID = ?, @unitName= ?, @unitGUID = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = (unit['id'], unit['name'])

	return execute_procedure(conn, sql, params, True)	# Check for units existence and create if not. Return GUID.


# 
def create_measure_units(conn, unit):
	print('Create Measurement Units')
	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_GET_OR_CREATE_SEL_MEASURE_UNIT] @mUniitName = ?, @mUnitGUID = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = (unit['units'])

	return execute_procedure(conn, sql, params, True)


# 
def create_type(conn, type):
	print('Create Type')
	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_GET_OR_CREATE_SEL_TYPE] @typeID = ?, @typeGUID = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = (type['id'], type['name'])
	return execute_procedure(conn, sql, params, True)


# 
def create_status(conn, status):
	print('Create Status')
	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_GET_OR_CREATE_SEL_STATUS] @statusID = ?, @statusGUID = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = (status['id'])
	return execute_procedure(conn, sql, params, True)


# 
def create_mode(conn, mode):
	print('Create Mode')
	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_GET_OR_CREATE_SEL_MODE] @modeID = ?, @modeGUID = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = (mode['id'])
	return execute_procedure(conn, sql, params, True)


# 
def create_update(conn, datetime):
	print('Create Datetime Update')
	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_CREATE_UPDATE] @lastUpdate = ?, @updateGUID = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = (datetime['last_update'])
	return execute_procedure(conn, sql, params, True)


# 
def create_request(conn, reading, unitGUID):
	print('Create Request')

	request = reading
	details = request['details']
	blocks = request['blocks']

	updateGUID = create_update(conn, details['last_update'])
	modeGUID = create_mode(conn, blocks['mode'])

	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_CREATE_SEL_REQUEST] @unitGUID = ?, @updateGUID = ?, @modeGUID = ?, @success = ?, @requestMessage = ?, @requestNow = ?, @requestName = ?, @tz = ?, @updateCycle = ?, @requestGUID = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = (unitGUID, updateGUID, modeGUID, request['seccess'], request['message'], request['now'], details['name'], details['tz'], details['update_cycle'])
	return execute_procedure(conn, sql, params)


# Create new reading
def create_readings(conn, reading, unitGUID):	###
	print('Create Reading')

	# Split recieved JSON into dicts
	blocks = reading['blocks'][0]	# Blocks are returned as a list of dicts but are all contained in list enty 0, so get that
	analogs = blocks['analogs']
	
	for data in enumerate(analogs):
		print('Processing Analogs/Readings')
	
		mUnitGUID = create_measure_units(conn, data['units'])

		sql = """ \
			EXEC [dbo].[PROC_CREATE_SEL_READING] @unitGUID = ?, @mUnitGUID= ?, @analogID = ?, @readingName = ?, @readingValue = ?, @recharge = ?, @cyclePulses = ?, @readingStart = ?, @readingStop = ?, @dp = ?;
			"""
		params = (unitGUID, mUnitGUID, data['aid'], data['name'], data['value'], data['recharge'], data['cycle_pulses'], data['start'], data['stop'], data['dp'])
		return execute_procedure(conn, sql, params)


# 
def create_output(conn, reading, unitGUID):	###
	print('Create Output')
	blocks = reading['blocks'][0]	# Blocks are returned as a list of dicts but are all contained in list enty 0, so get that
	outputs = blocks['outputs']

	for data in enumerate(outputs):
		print('Processing Outputs')

		updateGUID = create_update(conn, data['last_change'])
		modeGUID = create_mode(conn, data['mode'])
		statusGUID = create_status(conn, data['status'])

		sql = """ \
			EXEC [dbo].[PROC_CREATE_SEL_READING] @unitGUID = ?, @updateGUID= ?, @modeGUID = ?, @statusGUID = ?, @outputID = ?, @outputName = ?, @highstate = ?, @lowstate = ?;
			"""
		params = (unitGUID, updateGUID, modeGUID, statusGUID, data['oid'], data['name'], data['high_state'], data['low_state'])
		return execute_procedure(conn, sql, params)


# 
def create_alarm(conn, reading, unitGUID):	###
	print('Create Alarm')
	blocks = reading['blocks'][0]	# Blocks are returned as a list of dicts but are all contained in list enty 0, so get that
	alarms = blocks['alarms']
	for data in enumerate(alarms):
		print('Processing Alarms')

		typeGUID = create_type(conn, data['type'])
		statusGUID = create_status(conn, data['status'])
		mUnitGUID = create_measure_units(conn, data['units'])

		sql = """ \
			EXEC [dbo].[PROC_CREATE_SEL_READING] @unitGUID = ?, @typeGUID= ?, @statusGUID = ?, @mUnitGUID = ?, @alarmID = ?, @alarmName = ?, @lastChange = ?, @healthyName = ?, @faultyName = ?, @pulseTotal = ?;
			"""
		params = (unitGUID, typeGUID, statusGUID, mUnitGUID, data['aid'], data['name'], data['last_change'], data['healthy_name'], data['faulty_name'], data['pulse_total'])
		return execute_procedure(conn, sql, params)



# Main body
if __name__ == '__main__':
	print('Running as Main')
	unitGUIDs = []	# Used getting all device GUIDs from API, and subsequently suppling data for devices using these GUIDS
	readingsJSON = []	# f

	conn = db_connect(SQL_CONN)	
	
	unitsList = get_units() # Get GUIDs for all connected devices via SEL API
	print(unitsList)


	# Create new units & get sensor data from API
	for i, row in enumerate(unitsList):
		#print('Processing Unit #' + str(i))
		unitGUIDs.append(str_to_uuid(create_units(conn, row)))	# Create unit using retrieved data, or return unit GUID
		readingsJSON.append(get_data(row['id']))	# Get data for current sensor in loop, returns JSON
		

	for i, row in enumerate(readingsJSON):
		print('Processing JSON #' + str(i))
		#tmp = json.loads(row)
		create_request(conn, row, unitGUIDs[i])		# Insertion order: 06
		create_readings(conn, row, unitGUIDs[i])	# Insertion order: 10
		create_output(conn, row, unitGUIDs[i])		# Insertion order: 08
		create_alarm(conn, row, unitGUIDs[i])		# Insertion order: 11

	conn.commit()
	conn.close()

	#print(unitGUIDs)

# Process for getting data
## Get list of units first
## Attempt to store units
##


# unit
# mode
# update
# request 

#measure_units
#types

#updates
#statuses
#modes


#outputs
#requests
#alarms
#readings