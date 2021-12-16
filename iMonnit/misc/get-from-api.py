"""
	Description: Fetches data from the Monnit servers 
	using the RESTful API for a specified data range.
"""


__author__ = "Ethan Bellmer"
__version__ = "0.1"


# Import Libraries
import pandas as pd 
from pandas import json_normalize
from datetime import datetime, timedelta
from pandas.core.frame import DataFrame
import requests
import re
from living_lab_functions.functions import csv_dump, remove_trailing_values, air_quality_processing, split_dataframe_rows
import json
from decouple import config
import os


# Variable Declarations
CSV_DIR = os.getcwd() + "\\iMonnit\\data\\"

URL_BASE = config('MONNIT_API_URL')
URL_ENDPOINT = config('MONNIT_API_URL_ENDPOINT')
API_KEY = config('MONNIT_API_ID')
API_SECRET = config('MONNIT_API_SECRET')
NETWORK_ID = config('MONNIT_NETWORK_ID')


# Load lists from env variables
SENSOR_TYPES = config('MONNIT_SENSOR_TYPES', cast=lambda v: [s.strip() for s in v.split(',')])
SENSOR_COLUMNS = config('MONNIT_SENSOR_COLUMNS', cast=lambda v: [s.strip() for s in v.split(',')])
DELIMETERS = config('MONNIT_DELIMETERS', cast=lambda v: [s.strip() for s in v.split('*')])


FROM_DATE = "2020/11/01"
TO_DATE = "2020/12/31"

DATE_FORMAT = "%Y/%m/%d"


sensor_data = pd.DataFrame()
sensor_names = pd.DataFrame()
gateways = pd.DataFrame()


start = datetime.strptime(FROM_DATE, DATE_FORMAT).date()
end = datetime.strptime(TO_DATE, DATE_FORMAT).date()
step = timedelta(days=1)


# Main
# Get sensor names & details
FULL_URL = "{0}{1}?NetworkID={2}".format(URL_BASE, "SensorList", NETWORK_ID)
response = requests.get(FULL_URL, headers={"APIKeyID":API_KEY, "APISecretKey":API_SECRET})
if response.status_code == 200:
	print('Got Sensor Names')
	# Store the recieved JSON file from the request 
	json_load = response.json()
	sensor_names_json = json_load['Result']
	# Convert the JSONs into pandas dataframes
	sensor_names_raw = json_normalize(sensor_names_json)

	sensor_names_raw.rename(columns={"SensorID": "sensorID", "ApplicationID": "applicationID", "SensorName": "sensorName", "AccountID": "accountID"}, inplace = True)
	#cols = sensor_names_raw[["sensorID", "applicationID", "sensorName", "accountID"]]
	sensor_names = sensor_names_raw
	
	sensor_names.drop(["CSNetID", "LastCommunicationDate", "NextCommunicationDate", "LastDataMessageMessageGUID", 
						"PowerSourceID", "Status", "CanUpdate", "CurrentReading", "BatteryLevel", "SignalStrength", 
						"AlertsActive", "CheckDigit"], axis = 1, inplace = True)


# Get gateway details for network id
FULL_URL = "{0}{1}".format(URL_BASE, "GatewayList")
response = requests.get(FULL_URL, headers={"APIKeyID":API_KEY, "APISecretKey":API_SECRET})
if response.status_code == 200:
	print('Got Gateway Data')
	# Store the recieved JSON file from the request 
	json_load = response.json()
	gateways_json = json_load['Result']
	# Convert the JSONs into pandas dataframes
	gateways_raw = json_normalize(gateways_json)

	gateways_raw.rename(columns={"GatewayID": "gatewayID", "NetworkID": "networkID", "Name": "gatewayName", 
								"GatewayType": "gatewayType", "Heartbeat": "heartbeat", "IsDirty": "isDirty", 
								"LastCommunicationDate": "lastCommunicationDate", "LastInboundIPAddress": "lastInboundIPAddress", 
								"MacAddress": "macAddress", "IsUnlocked": "isUnlocked", "CheckDigit": "checkDigit", 
								"AccountID": "accountID", "SignalStrength": "signalStrength", "BatteryLevel": "batteryLevel"}, inplace = True)
	gateways = gateways_raw

	gateways.drop(["gatewayType", "heartbeat", "isDirty", "lastCommunicationDate", 
					"lastInboundIPAddress", "macAddress", "isUnlocked", "checkDigit", 
					"accountID", "signalStrength", "batteryLevel"], axis = 1, inplace = True)


# Get sensor readings at 1 day intervals as API rate limit kicks in if requesting too many days in one request
while start < end:
	stepStart = start.strftime(DATE_FORMAT)
	start += step
	stepEnd = start.strftime(DATE_FORMAT)

	print('Step Start ' + str(stepStart) + ' | Step End ' + str(stepEnd))

	FULL_URL = "{0}{1}?networkIDInteger={2}&fromDate={3}&toDate={4}".format(URL_BASE, "AccountDataMessages", NETWORK_ID, stepStart, stepEnd)
	response = requests.get(FULL_URL, headers={"APIKeyID":API_KEY, "APISecretKey":API_SECRET})

	if response.status_code == 200:
		# Store the recieved JSON file from the request 
		jsonLoad = response.json()
		
		sensorData = jsonLoad['Result']
		# Convert the JSONs into pandas dataframes
		sensorData = json_normalize(sensorData)
		
		sensorData.rename(columns={"MessageDate": "messageDate", "SensorID": "sensorID", "DataMessageGUID": "dataMessageGUID", "State": "state", 
			"SignalStrength": "signalStrength", "Voltage": "voltage", "Battery": "batteryLevel", "Data": "rawData", "DisplayData": "displayData", 
			"PlotValue": "plotValue", "MetNotificationRequirements": "metNotificationRequirements", "GatewayID": "gatewayID", "DataValues": "dataValue",
			"DataTypes": "dataType", "PlotValues": "plotValues", "PlotLabels": "plotLabels"}, inplace=True)

		for i, data in sensorData.iterrows():
			TimestampUtc = re.split('\(|\)', data.messageDate)[1][:10]
			sensorData.messageDate.iat[i] = datetime.fromtimestamp(int(TimestampUtc))
		sensor_data = sensor_data.append(sensorData)
				
	else:
		print(response.status_code)


if sensor_names.empty == False:
	sensor_data = sensor_data.merge(sensor_names, on = "sensorID", how = "outer")
if gateways.empty == False:
	sensor_data = sensor_data.merge(gateways, on = "gatewayID", how = "outer")


sensor_data.dropna(subset = ["dataMessageGUID"], inplace = True)


csv_dump("living_lab_monnit_" + str(FROM_DATE.replace("/", "-")) + '_' + str(TO_DATE.replace("/", "-")), sensor_data, dir = CSV_DIR)


# Remove the trailing values present in the rawData field of some sensors
sensor_data_split = remove_trailing_values(sensor_data, SENSOR_TYPES)
# Process any sensor messages for Air Quality
sensor_data_split = air_quality_processing(sensor_data_split)
# Split the dataframe to move concatonated values to new rows
sensor_data_split = split_dataframe_rows(sensor_data_split, SENSOR_COLUMNS, DELIMETERS)


sensor_data_split = sensor_data_split.query('sensorID in [518776, 518778, 712997, 713417]')

csv_dump("living_lab_monnit_split_" + str(FROM_DATE.replace("/", "-")) + '_' + str(TO_DATE.replace("/", "-")), sensor_data_split, dir = CSV_DIR)