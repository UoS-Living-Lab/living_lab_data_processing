import pandas as pd

import pyodbc

import requests
import json

import datetime
import os

from decouple import config

from db import dbConnect, execProcedure, execProcedureNoReturn

from uuid import UUID


BASE_URL = config('SEL_API_URL')
USER_KEY = config('SEL_USER_KEY')
API_KEY = config('SEL_API_KEY')

DB_URL = config('AZURE_DB_SERVER')
DB_BATABASE = config('AZURE_DB_DATABASE')
DB_USR = config('AZURE_DB_USR')
DB_PWD = config('AZURE_DB_PWD')


# Formatted connection string for the SQL DB.
SQL_CONN = "DSN={0};Database={1};Trusted_Connection=no;UID={2};PWD={3};".format(DB_URL, DB_BATABASE, DB_USR, DB_PWD)


# Convert returned strings from the DB into GUID
def strToUUID(struct):
	# Remove the leading and trailing characters from the ID
	struct = struct.replace("[('", "")
	struct = struct.replace("', )]", "")
	# Convert trimmed string into a GUID (UUID)
	strUUID =  UUID(struct)

	# Return to calling function
	return strUUID


# Gets the units available via the API
def getUnits():
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


# Creates a Unit entry in the database using a passed in dict
def createUnits(conn, unit):
	print('Create Unit')

	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_GET_OR_CREATE_SEL_UNIT] @unitID = ?, @unitName= ?, @param_out = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = (unit['name'].values(), unit['id'].values())

	#sensorGUID = 
	return execProcedure(conn, sql, params)


# Gets data from a specific unit using the passed in ID parameter. 
def getData(unitID):
	API_ACTION = "data"
	FULL_URL = "{0}?user_key={1}&api_key={2}&action={3}&interface={4}".format(BASE_URL, USER_KEY, API_KEY, API_ACTION, unitID)

	response = requests.get(FULL_URL)
	if (response.status_code != 200):
		print('Error occurred, status: ' + str(response.status_code))
	else:
		print("Data endpoint status: " + str(response.status_code))
		recJSON = response.json()

		return recJSON


# Main body
if __name__ == '__main__':
	print('Running as Main')
	unitGUIDs = []

	conn = dbConnect(SQL_CONN)
	
	unitsList = getUnits()

	print(unitsList)

	for row in enumerate(unitsList):
		unitGUIDs.append(strToUUID(createUnits(conn, row)))

	conn.commit()
	conn.close()

	print(unitGUIDs)
# Process for getting data
## Get list of units first
## Attempt to store units
##
