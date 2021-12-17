"""

"""


__author__ = "Ethan Bellmer"
__version__ = "0.1"


import pandas as pd
from decouple import config

from living_lab_functions.db import db_connect, execute_procedure
from living_lab_functions.functions import str_to_uuid

from tqdm import tqdm
import os

BASE_URL = config('SEL_API_URL')
USER_KEY = config('SEL_USER_KEY')
API_KEY = config('SEL_API_KEY')

DB_DRIVER = config('DB_DRIVER')
DB_URL = config('AZURE_DB_SERVER')
DB_BATABASE = config('AZURE_DB_DATABASE')
DB_USR = config('AZURE_DB_USR')
DB_PWD = config('AZURE_DB_PWD')

# Formatted connection string for the SQL DB.
SQL_CONN_STR = "DRIVER={0};SERVER={1};Database={2};UID={3};PWD={4};".format(DB_DRIVER, DB_URL, DB_BATABASE, DB_USR, DB_PWD)



# Create new reading
def create_reading(conn, data):	###
	sql = """
		SELECT DISTINCT unitGUID
		FROM [SEL_UNITS]
		WHERE unitID = ?
	"""
	params = int(data['INTERFACE_ID'])
	unitGUID = str_to_uuid(execute_procedure(conn, sql, params, True))

	try:
		sql = """
			SELECT DISTINCT sensorGUID
			FROM [SEL_READINGS]
			WHERE unitGUID = ? AND analogID = ?
		"""
		params = (unitGUID, int(data['aid']))
		sensorGUID = str_to_uuid(execute_procedure(conn, sql, params, True))
	except:
		# print("Sensor doesn't exist, storing for completeness...")
		sensorGUID = None


	if sensorGUID != None:
		sql = """
			SELECT DISTINCT mUnitGUID
			FROM [SEL_READINGS]
			WHERE unitGUID = ? AND sensorGUID = ?
		"""
		params = (unitGUID, sensorGUID)
		mUnitGUID = str_to_uuid(execute_procedure(conn, sql, params, True))
	else:
		mUnitGUID = None


	sql = """
		DECLARE @readingGUID UNIQUEIDENTIFIER
		SET @readingGUID = NULL
		SET @readingGUID = NEWID()
		INSERT INTO [SEL_READINGS] (readingGUID, unitGUID, mUnitGUID, sensorGUID, analogID, readingValue) 
		OUTPUT Inserted.readingGUID
		VALUES (@readingGUID, ?, ?, ?, ?, ?);
		"""
	params = (unitGUID, mUnitGUID, sensorGUID, int(data['aid']), data['value'])
	readingGUID = str_to_uuid(execute_procedure(conn, sql, params, True))


	sql = """
		INSERT INTO [SEL_UPDATES] (readingGUID, lastUpdate) VALUES (?, ?)
		"""
	params = (readingGUID, data['DATETIME'])
	execute_procedure(conn, sql, params)


if __name__ == '__main__':
	conn = db_connect(SQL_CONN_STR)
	df = pd.read_csv(os.getcwd() + '/SEL/data/water_data_unpivot.csv')

	df['aid'] = df['aid'].map(lambda x: x.lstrip('AI'))

	print('Procesiing ' + str(len(df.index)) + ' rows...')
	for i, row in tqdm(df.iterrows()):
		create_reading(conn, row)

	conn.commit()
	conn.close()