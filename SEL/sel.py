"""
Polls the SEL RESTful API for water data 
for the Salford Living Lab.
"""


__author__ = "Ethan Bellmer"
__version__ = "0.1"


from os import read
import pandas as pd
import requests
from datetime import datetime
from decouple import config

from living_lab_functions.db import db_connect, execute_procedure, execute_procedure_no_return
from living_lab_functions.functions import str_to_uuid


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
		EXEC [dbo].[PROC_GET_OR_CREATE_SEL_MEASURE_UNIT] @mUnitName = ?, @mUnitGUID = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = unit
	return execute_procedure(conn, sql, params, True)


# 
def create_type(conn, type):
	print('Create Type')
	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_GET_OR_CREATE_SEL_TYPE] @typeID = ?, @typeGUID = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = type
	return execute_procedure(conn, sql, params, True)


# 
def create_status(conn, status):
	print('Create Status')
	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_GET_OR_CREATE_SEL_STATUS] @statusID = ?, @statusGUID = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = status
	return execute_procedure(conn, sql, params, True)


# 
def create_mode(conn, mode):
	print('Create Mode')
	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_GET_OR_CREATE_SEL_MODE] @modeID = ?, @modeGUID = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = mode
	return execute_procedure(conn, sql, params, True)


# 
def create_update(conn, date_time, requestGUID = None, alarmGUID = None, readingGUID = None, outputGUID = None):
	print('Create Datetime Update')
	sql = """\
		EXEC [dbo].[PROC_CREATE_SEL_UPDATE] @requestGUID = ?, @alarmGUID = ?, @readingGUID = ?, @outputGUID = ?, @lastUpdate = ?;
		"""
	print(str(requestGUID) + ' ' + str(alarmGUID) + ' ' + str(readingGUID) + ' ' + str(outputGUID))
	params = (requestGUID, alarmGUID, readingGUID, outputGUID, datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S'))
	return execute_procedure(conn, sql, params)


def create_sensor(conn, sensorName):
	print('Creating Sensor')
	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_CREATE_SEL_SENSOR] @sensorName = ?, @sensorGUID = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = sensorName
	return execute_procedure(conn, sql, params, True)


# 
def create_request(conn, reading, unitGUID):
	print('Create Request')

	request = reading
	details = request['details']
	blocks = request['blocks'][0]

	
	modeGUID = str_to_uuid(create_mode(conn, blocks['mode']))

	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_CREATE_SEL_REQUEST] @unitGUID = ?, @modeGUID = ?, @success = ?, @requestMessage = ?, @requestNow = ?, @requestName = ?, @tz = ?, @updateCycle = ?, @requestGUID = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = (unitGUID, modeGUID, bool(request['success']), request['message'], datetime.strptime(request['now'], '%Y-%m-%dT%H:%M:%S%z'), details['name'], details['tz'], details['update_cycle'])
	requestGUID = str_to_uuid(execute_procedure(conn, sql, params, True))
	if details['last_update'] != None:
		create_update(conn, details['last_update'], requestGUID= requestGUID)


# Create new reading
def create_reading(conn, reading, unitGUID):	###
	print('Create Reading')

	# Split recieved JSON into dicts
	blocks = reading['blocks'][0]	# Blocks are returned as a list of dicts but are all contained in list enty 0, so get that
	analogs = blocks['analogs']
	
	for data in analogs:
		mUnitGUID = str_to_uuid(create_measure_units(conn, data['units']))
		sensorGUID = str_to_uuid(create_sensor(conn, data['name']))

		sql = """ \
			DECLARE @out UNIQUEIDENTIFIER;
			EXEC [dbo].[PROC_CREATE_SEL_READING] @unitGUID = ?, @mUnitGUID= ?, @sensorGUID = ?, @analogID = ?, @readingValue = ?, @recharge = ?, @cyclePulses = ?, @readingStart = ?, @readingStop = ?, @dp = ?, @readingGUID = @out OUTPUT;
			SELECT @out AS the_output;
			"""
		params = (unitGUID, mUnitGUID, sensorGUID, int(data['aid']), data['value'], int(data['recharge']), float(data['cycle_pulses']), float(data['start']), float(data['stop']), int(data['dp']))
		reading_guid = str_to_uuid(execute_procedure(conn, sql, params, True))
		if blocks['last_update'] != None:
			create_update(conn, blocks['last_update'], readingGUID= reading_guid)


# 
def create_output(conn, reading, unitGUID):	###
	print('Create Output')
	blocks = reading['blocks'][0]	# Blocks are returned as a list of dicts but are all contained in list enty 0, so get that
	outputs = blocks['outputs']

	for data in outputs:
		
		modeGUID = str_to_uuid(create_mode(conn, data['mode']))
		statusGUID = str_to_uuid(create_status(conn, data['status']))

		sql = """ \
			DECLARE @out UNIQUEIDENTIFIER;
			EXEC [dbo].[PROC_CREATE_SEL_OUTPUT] @unitGUID = ?, @modeGUID = ?, @statusGUID = ?, @outputID = ?, @outputName = ?, @highstate = ?, @lowstate = ?, @outputGUID = @out OUTPUT;
			SELECT @out AS the_output;
			"""
		params = (unitGUID, modeGUID, statusGUID, int(data['oid']), data['name'], data['high_state'], data['low_state'])
		outputGUID = str_to_uuid(execute_procedure(conn, sql, params, True))
		if data['last_update'] != None:
			create_update(conn, data['last_update'], outputGUID= outputGUID)


# 
def create_alarm(conn, reading, unitGUID):	###
	print('Create Alarm')
	blocks = reading['blocks'][0]	# Blocks are returned as a list of dicts but are all contained in list enty 0, so get that
	alarms = blocks['alarms']
	for data in alarms:
		typeGUID = str_to_uuid(create_type(conn, data['type']))
		statusGUID = str_to_uuid(create_status(conn, data['status']))
		mUnitGUID = str_to_uuid(create_measure_units(conn, data['pulse_units']))

		sql = """ \
			DECLARE @out UNIQUEIDENTIFIER;
			EXEC [dbo].[PROC_CREATE_SEL_ALARM] @unitGUID = ?, @typeGUID= ?, @statusGUID = ?, @mUnitGUID = ?, @alarmID = ?, @alarmName = ?, @healthyName = ?, @faultyName = ?, @pulseTotal = ?, @alarmGUID = @out OUTPUT;
			SELECT @out AS the_output;
			"""
		params = (unitGUID, typeGUID, statusGUID, mUnitGUID, int(data['aid']), data['name'], data['healthy_name'], data['faulty_name'], float(data['pulse_total']))
		alarm_guid = str_to_uuid(execute_procedure(conn, sql, params, True))
		if data['last_change'] != None:
			create_update(conn, data['last_change'], alarmGUID= alarm_guid)


# Main body
if __name__ == '__main__':
	print('Running as Main')
	unitGUIDs = []	# Used getting all device GUIDs from API, and subsequently suppling data for devices using these GUIDS
	readingsJSON = []	# f

	conn = db_connect(SQL_CONN)	
	
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