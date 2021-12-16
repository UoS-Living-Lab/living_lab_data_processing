"""
Listens for webhooks from TTN  
for the Salford Living Lab application.
"""


__author__ = "Ethan Bellmer"
__version__ = "1.0"


# Import libraries
from flask import Flask, request
from flask_httpauth import HTTPTokenAuth
from flask.cli import with_appcontext
from flask.wrappers import Response
from flask_basicauth import BasicAuth
from werkzeug.serving import WSGIRequestHandler
from decouple import config
from datetime import datetime
import dateutil.parser

from living_lab_functions.db import execute_procedure, get_db, commit_db, close_db
from living_lab_functions.functions import flask_to_uuid


# Batabase access credentials
DB_DRIVER = config('DB_DRIVER')
DB_URL = config('AZURE_DB_SERVER')
DB_BATABASE = config('AZURE_DB_DATABASE')
DB_USR = config('AZURE_DB_USR')
DB_PWD = config('AZURE_DB_PWD')
# POST Authen Token
API_ACCESS_TOKEN = config('X_DOWNLINK_APIKEY')


# Formatted connection string for the SQL DB.
SQL_CONN_STR = "DRIVER={0};SERVER={1};Database={2};UID={3};PWD={4};".format(DB_DRIVER, DB_URL, DB_BATABASE, DB_USR, DB_PWD)


# Flask web server
app = Flask(__name__)


# Create the application or get the GUID if it already exists
def create_application(conn, app):
	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_GET_OR_CREATE_TTN_APPLICATION] @application_name= ?, @application_guid = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = (app)
	return flask_to_uuid(execute_procedure(conn, sql, params, True))


# Create a warning if one exists in the JSON
def create_warning(conn, warning = None):
	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_CREATE_TTN_WARNING] @decoded_payload_warning= ?, @warning_guid = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = (warning)
	return flask_to_uuid(execute_procedure(conn, sql, params, True))


# Create a gateway entry or get GUID if it already exists
def create_gateway(conn, gateway_name, location_guid = None):
	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_GET_OR_CREATE_TTN_GATEWAY] @location_guid= ?, @gateway_name= ?, @gateway_guid = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = (location_guid, str(gateway_name))
	return flask_to_uuid(execute_procedure(conn, sql, params, True))


# Create a location or get GUID if it already exists
def create_location(conn, latitude, longitude, source = None):
	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_GET_OR_CREATE_TTN_LOCATION] @latitude= ?, @longitude= ?, @source= ?, @location_guid = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = (float(latitude), float(longitude), str(source))
	return flask_to_uuid(execute_procedure(conn, sql, params, True))


# Create a sensor or get GUID if it already exists
def create_sensor(conn, name, type = None, location = None, m_unit = None):
	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_GET_OR_CREATE_TTN_SENSOR] @sensor_name= ?, @sensor_type= ?, @sensor_location= ?, @measurement_unit= ?, @sensor_guid = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = (str(name), str(type), str(location), str(m_unit))
	return flask_to_uuid(execute_procedure(conn, sql, params, True))


# Create a device or get GUID if it already exists
def create_device(conn, app_guid, name, location = None, dev_eui = None, join_eui = None, dev_addr = None):
	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_GET_OR_CREATE_TTN_DEVICE] @application_guid= ?, @device_name= ?, @device_location= ?, @dev_eui= ?, @join_eui= ?, @dev_addr= ?, @device_guid = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = (app_guid, str(name), str(location), str(dev_eui), str(join_eui), str(dev_addr))
	return flask_to_uuid(execute_procedure(conn, sql, params, True))


def create_uplink(conn, device_guid, session_key_id = None, f_port = None, f_cnt = None, frm_payload = None, raw_bytes = None, consumed_airtime= None, warning_guid = None):
	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_CREATE_TTN_UPLINK] @device_guid= ?, @warning_guid= ?, @session_key_id= ?, @f_port= ?, @f_cnt= ?, @frm_payload= ?, @raw_bytes= ?, @consumed_airtime= ?, @uplink_guid = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = (device_guid, warning_guid, session_key_id, f_port, f_cnt, frm_payload, raw_bytes, consumed_airtime)
	return flask_to_uuid(execute_procedure(conn, sql, params, True))


def create_datetime(conn, received_at, rx_guid = None, hop_guid = None, uplink_guid = None):
	dt = dateutil.parser.isoparse(received_at) # ISO 8601 extended format
	sql = """\
		EXEC [dbo].[PROC_CREATE_TTN_DATETIME] @rx_guid= ?, @hop_guid= ?, @uplink_guid= ?, @received_at= ?;
		"""
	params = (rx_guid, hop_guid, uplink_guid, dt)
	return execute_procedure(conn, sql, params)


def create_rx(conn, gateway_guid, uplink_guid, rx):
	time = None
	timestamp = None
	rssi = None
	channel_rssi = None
	snr = None

	if 'timestamp' in rx[0]:
		timestamp = int(rx[0]['timestamp'])
	if 'time' in rx[0]:
		time = dateutil.parser.isoparse(rx[0]['time'])
	if 'rssi' in rx[0]:
		rssi = int(rx[0]['rssi'])
	if 'channel_rssi' in rx[0]:
		channel_rssi = int(rx[0]['channel_rssi'])
	if 'snr' in rx[0]:
		snr = float(rx[0]['snr'])

	if 'packet_broker' in rx:	# If the JSON contains 'packet broker' entries treat it as a V1 JSON 
		#	Unpack values and check if they exist
		message_id = None
		forwarder_net_id = None
		forwarder_tenant_id = None
		forwarder_cluster_id = None
		home_network_net_id = None
		home_network_tenant_id = None
		home_network_cluster_id = None
		
		if 'message_id' in rx[1]:
			message_id = str(rx[1]['message_id'])
		if 'forwarder_net_id' in rx[1]:
			forwarder_net_id = int(rx[1]['forwarder_net_id'])
		if 'forwarder_tenant_id' in rx[1]['forwarder_tenant_id']:
			forwarder_tenant_id = str(rx[1]['forwarder_tenant_id'])
		if 'forwarder_cluster_id' in rx[1]['forwarder_cluster_id']:
			forwarder_cluster_id = str(rx[1]['forwarder_cluster_id'])
		if 'home_network_net_id' in rx[1]['home_network_net_id']:
			home_network_net_id = int(rx[1]['home_network_net_id'])
		if 'home_network_tenant_id' in rx[1]['home_network_tenant_id']:
			home_network_tenant_id = str(rx[1]['home_network_tenant_id'])
		if 'home_network_cluster_id' in rx[1]['home_network_cluster_id']:
			home_network_cluster_id = str(rx[1]['home_network_cluster_id'])
		
		sql = """\
			DECLARE @out UNIQUEIDENTIFIER;
			EXEC [dbo].[PROC_CREATE_TTN_RX] @gateway_guid= ?, @uplink_guid= ?, @rx_time= ?, @rx_timestamp= ?, @rssi= ?, @channel_rssi= ?, @snr= ?, @message_id= ?, @forwarder_net_id= ?, @forwarder_tenant_id= ?, @forwarder_cluster_id= ?, @home_network_net_id= ?, @home_network_tenant_id= ?, @home_network_cluster_id= ?, @rx_guid = @out OUTPUT;
			SELECT @out AS the_output;
			"""
		params = (gateway_guid, uplink_guid, time, timestamp, rssi, channel_rssi, snr, message_id, forwarder_net_id, forwarder_tenant_id, forwarder_cluster_id, home_network_net_id, home_network_tenant_id, home_network_cluster_id)
		rx_guid =  flask_to_uuid(execute_procedure(conn, sql, params, True))
		create_hop(conn, rx_guid, rx[1]['packet_broker'])
	else:	# If 'packet broker' doesn't exist then treat is as a V2 JSON and isgnore the assitional variables
		sql = """\
			DECLARE @out UNIQUEIDENTIFIER;
			EXEC [dbo].[PROC_CREATE_TTN_RX] @gateway_guid= ?, @uplink_guid= ?, @rx_time= ?, @rx_timestamp= ?, @rssi= ?, @channel_rssi= ?, @snr= ?, @message_id= ?, @forwarder_net_id= ?, @forwarder_tenant_id= ?, @forwarder_cluster_id= ?, @home_network_net_id= ?, @home_network_tenant_id= ?, @home_network_cluster_id= ?, @rx_guid = @out OUTPUT;
			SELECT @out AS the_output;
			"""
		params = (gateway_guid, uplink_guid, time, timestamp, rssi, channel_rssi, snr, None, None, None, None, None, None, None)
		rx_guid = flask_to_uuid(execute_procedure(conn, sql, params, True))

	if 'uplink_token' in rx[0]:
		create_uplink_token(conn, rx_guid, gateway_guid, rx[0]['uplink_token'])
	elif 'uplink_token' in rx[1]:
		create_uplink_token(conn, rx_guid, gateway_guid, rx[1]['uplink_token'])
	
	return rx_guid


def create_hop(conn, rx_guid, hop):
	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_CREATE_TTN_HOP] @rx_guid= ?, @sender_address= ?, @receiver_name= ?, @receiver_agent= ?, @hop_guid = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = (rx_guid, str(hop['sender_address']), str(hop['receiver_name']), str(hop['receiver_agent']))
	hop_guid = flask_to_uuid(execute_procedure(conn, sql, params, True))
	create_datetime(conn, hop['received_at'], hop_guid = hop_guid) # Only run if parsing a V1 JSON
	return hop_guid


def create_correlation_id(conn, rx_guid, correlation_id):
	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_CREATE_TTN_CORRELATION_ID] @rx_guid= ?, @correlation_id= ?, @correlation_guid = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = (rx_guid, str(correlation_id))
	return flask_to_uuid(execute_procedure(conn, sql, params, True))


def create_uplink_token(conn, rx_guid, gateway_guid, uplink_token):
	sql = """\
		EXEC [dbo].[PROC_CREATE_TTN_UPLINK_TOKEN] @rx_guid= ?, @gateway_guid= ?, @uplink_token= ?;
		"""
	params = (rx_guid, gateway_guid, str(uplink_token))
	return execute_procedure(conn, sql, params)


def create_reading(conn, uplink_guid, sensor_guid, sensor_value):
	sql = """\
		EXEC [dbo].[PROC_CREATE_TTN_READING] @uplink_guid= ?, @sensor_guid= ?, @sensor_value= ?;
		"""
	params = (uplink_guid, sensor_guid, str(sensor_value))
	return execute_procedure(conn, sql, params)


def create_uplink_setting(conn, uplink_guid, uplink_settings):
	if 'timestamp' in uplink_settings:
		setting_timestamp = int(uplink_settings['timestamp'])
	else:
		setting_timestamp = None
	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_CREATE_TTN_UPLINK_SETTING] @uplink_guid= ?, @bandwidth= ?, @spreading_factor= ?, @data_rate_index= ?, @coding_rate= ?, @frequency= ?, @setting_timestamp= ?, @uplink_setting_guid = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = (uplink_guid, int(uplink_settings['data_rate']['lora']['bandwidth']), int(uplink_settings['data_rate']['lora']['spreading_factor']), int(uplink_settings['data_rate_index']), str(uplink_settings['coding_rate']), int(uplink_settings['frequency']), setting_timestamp)
	return flask_to_uuid(execute_procedure(conn, sql, params, True))


@app.route('/uplink/messages', methods=['POST'])
def ttn_webhook():
	if request.headers.get('X_DOWNLINK_APIKEY') != config('X_DOWNLINK_APIKEY'):
		print('Unauthorised')
		status_code = Response(status=401)
		return status_code
	print('Request Authenticated, Processing')


	sensor_guids = []
	sensor_value = []
	correlation_guid = []
	rx_list = []

	session_key_id = None
	f_port = None
	f_cnt = None
	frm_payload = None
	consumed_airtime = None
	warning_guid = None
	

	# Connect to DB and get an app context with teh connector
	conn = get_db(SQL_CONN_STR)

	proc = request.json	# Store the recieved JSON file from the request

	app_guid = create_application(conn, proc['end_device_ids']['application_ids']['application_id'])
	device_guid = create_device(conn, app_guid, proc['end_device_ids']['device_id'], dev_eui = proc['end_device_ids']['dev_eui'], join_eui = proc['end_device_ids']['join_eui'], dev_addr = proc['end_device_ids']['dev_addr'])
	
	#	Unpack subobject into variables to check if they exist.
	if 'decoded_payload_warnings' in proc['uplink_message']:
		if proc['uplink_message']['decoded_payload_warnings']:
			warning_guid = create_warning(conn, proc['uplink_message']['decoded_payload_warnings'])	
	if 'raw_bytes' in proc['uplink_message']['decoded_payload']:
		raw_bytes = str(proc['uplink_message']['decoded_payload']['raw_bytes'])
	elif 'bytes' in proc['uplink_message']['decoded_payload']:
		raw_bytes = str(proc['uplink_message']['decoded_payload']['bytes'])
	else:
		raw_bytes = None

	if 'session_key_id' in proc['uplink_message']:
		session_key_id = str(proc['uplink_message']['session_key_id'])
	if 'f_port' in proc['uplink_message']:
		f_port = int(proc['uplink_message']['f_port'])
	if 'f_cnt' in proc['uplink_message']:
		f_cnt = int(proc['uplink_message']['f_cnt'])
	if 'frm_payload' in proc['uplink_message']:
		frm_payload = str(proc['uplink_message']['frm_payload'])
	if 'consumed_airtime' in proc['uplink_message']:
		consumed_airtime = float(proc['uplink_message']['consumed_airtime'][:-1])

	uplink_guid = create_uplink(conn, device_guid, session_key_id, f_port, f_cnt, frm_payload, raw_bytes, consumed_airtime, warning_guid)	
	create_datetime(conn, proc['uplink_message']['received_at'], uplink_guid = uplink_guid)


	for row in proc['uplink_message']['rx_metadata']:
		rx_list.append(row)
		if 'location' in row:
			location_guid = create_location(conn, row['location']['latitude'], row['location']['longitude'])
			gateway_guid = create_gateway(conn, row['gateway_ids']['gateway_id'], location_guid)
		elif 'gateway_ids' in row:
			gateway_guid = create_gateway(conn, row['gateway_ids']['gateway_id'])

	rx_guid = create_rx(conn, gateway_guid, uplink_guid, rx_list)	# Store created GUID for later dependencies and datetime creation
	create_datetime(conn, proc['received_at'], rx_guid = rx_guid)	# Create a datetime entry using the stored RX_GUID


	#	Parse correlation IDs from received JSON and create entries
	for row in proc['correlation_ids']:
		correlation_guid.append(create_correlation_id(conn, rx_guid, row))

	create_uplink_setting(conn, uplink_guid, proc['uplink_message']['settings'])	#	Create an uplink settings entry for the recieved uplink


	#	Check if the newer 'sensor_names' and 'sensor_data' arrays exist in JSON, if not process each measurand manually from the old format.
	if 'sensor_data' and 'sensor_names' in proc['uplink_message']['decoded_payload']:
		#	Parse sensor names and get GUIDs
		for row in proc['uplink_message']['decoded_payload']['sensor_names']:
			sensor_guids.append(create_sensor(conn,row))
		#	Parse sensor values
		for row in proc['uplink_message']['decoded_payload']['sensor_data']:
			sensor_value.append(row)
	else:
		print('Processing Legacy JSON...')
		#	Manually get sensor data fields old JSON format.
		sensor_names_temp = ['Altitude', 'Battery Voltage', 'Humidity', 'Pressure', 'Rain Detect', 'Solar Voltage', 'Temperature']
		sensor_val_temp = [proc['uplink_message']['decoded_payload']['altitude'], proc['uplink_message']['decoded_payload']['battery_voltage'], proc['uplink_message']['decoded_payload']['humidity'], proc['uplink_message']['decoded_payload']['pressure'], proc['uplink_message']['decoded_payload']['rain_detect'], proc['uplink_message']['decoded_payload']['solar_voltage'], proc['uplink_message']['decoded_payload']['temp']]
		#	Parse sensor names and get GUIDs
		for row in sensor_names_temp:
			sensor_guids.append(create_sensor(conn,row))
		#	Parse sensor values
		for row in sensor_val_temp:
			sensor_value.append(row)

	i = 0
	for row in sensor_guids:
		create_reading(conn, uplink_guid, row, sensor_value[i])
		i = i + 1


	#	Commit data and close open database connection
	commit_db()
	close_db()

	#	Return status 200 (success) to the remote client
	status_code = Response(status=200)
	return status_code

if __name__ == '__main__':
	WSGIRequestHandler.protocol_version = "HTTP/1.1"
	app.run(host = '0.0.0.0', port = '80')