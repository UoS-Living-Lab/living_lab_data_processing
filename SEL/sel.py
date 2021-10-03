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

	return execute_procedure(conn, sql, params)	# Check for units existence and create if not. Return GUID.


def create_measure_units(conn, unit):
	print('Create Measurement Units')


def create_type(conn, type):
	print('Create Type')


def create_status(conn, status):
	print('Create Status')


def create_mode(conn, mode):
	print('Create Mode')


def create_request(conn, reading):
	print('Create Request')


def create_update(conn, datetime):
	print('Create Datetime Update')


# Create new reading
def create_readings(conn, unitGUID, reading):
	print('Create Reading')

	# Split recieved JSON into dicts
	request = reading
	blocks = reading['blocks'][0]	# Blocks are returned as a list of dicts but are all contained in list enty 0, so get that

	analogs = blocks['analogs']
	
	for data in enumerate(analogs):
		print('Processing Analogs/Readings')

	
	sql = """ \
		EXEC [dbo].[PROC_CREATE_SEL_READING] @unitGUID = ?, @mUnitGUID= ?, @analogID = ?, @readingName = ?, @readingValue = ?, @recharge = ?, @cyclePulses = ?, @readingStart = ?, @readingStop = ?, @dp = ?;
		"""
	params = ()


def create_output(conn, reading):
	print('Create Output')
	blocks = reading['blocks'][0]	# Blocks are returned as a list of dicts but are all contained in list enty 0, so get that
	outputs = blocks['outputs']
	for data in enumerate(outputs):
		print('Processing Outputs')

def create_alarm(conn, reading):
	print('Create Alarm')
	blocks = reading['blocks'][0]	# Blocks are returned as a list of dicts but are all contained in list enty 0, so get that
	alarms = blocks['alarms']
	for data in enumerate(alarms):
		print('Processing Alarms')


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
		print(row)
		unitGUIDs.append(str_to_uuid(create_units(conn, row)))	# Create unit using retrieved data, or return unit GUID
		readingsJSON.append(get_data(row['id']))	# Get data for current sensor in loop, returns JSON
		

	for i, row in enumerate(readingsJSON):
		print(row)
		#tmp = json.loads(row)
		create_readings(conn, row, unitGUIDs[i])


	for i, row in enumerate(unitGUIDs):
		print (row)


	conn.commit()
	conn.close()

	print(unitGUIDs)

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