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

from living_lab_functions.db import execute_procedure, get_db, commit_db, close_db
from living_lab_functions.functions import flask_to_uuid


# Batabase access credentials
DB_URL = config('AZURE_DB_SERVER')
DB_BATABASE = config('AZURE_DB_DATABASE')
DB_USR = config('AZURE_DB_USR')
DB_PWD = config('AZURE_DB_PWD')
# POST Authen Token
API_ACCESS_TOKEN = config('X_DOWNLINK_APIKEY')


# Formatted connection string for the SQL DB.
SQL_CONN_STR = "DSN={0};Database={1};UID={2};PWD={3};".format(DB_URL, DB_BATABASE, DB_USR, DB_PWD)


# Flask web server
app = Flask(__name__)


# Create the application or get the GUID if it already exists
def create_application(conn, app):
	print('Creating or getting application')
	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_GET_OR_CREATE_TTN_APPLICATION] @application_name= ?, @application_guid = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = (app['name'])
	return flask_to_uuid(execute_procedure(conn, sql, params, True))


# Create a warning if one exists in the JSON
def create_warning(conn, warning = None):
	print('Creating warning.')
	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_CREATE_TTN_TTN_WARNING] @decoded_payload_warning= ?, @warning_guid = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = (warning)
	return flask_to_uuid(execute_procedure(conn, sql, params, True))


# Create a gateway entry or get GUID if it already exists
def create_gateway(conn, gateway_name, location_guid):
	print('Create or get gateway.')
	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_GET_OR_CREATE_TTN_GATEWAY] @location_guid= ?, @gateway_name= ?, @location_guid = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = (location_guid, gateway_name)
	return flask_to_uuid(execute_procedure(conn, sql, params, True))


# Create a location or get GUID if it already exists
def create_location(conn, latitude, longitude, source = None):
	print('Get or create location.')
	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_GET_OR_CREATE_TTN_LOCATION] @latitude= ?, @longitude= ?, @source= ?, @location_guid = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = (latitude, longitude, source)
	return flask_to_uuid(execute_procedure(conn, sql, params, True))


# Create a sensor or get GUID if it already exists
def create_sensor(conn, name, type = None, location = None, m_unit = None):
	print('Get or create sensor.')
	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_GET_OR_CREATE_TTN_SENSOR] @sensor_name= ?, @sensor_type= ?, @sensor_location= ?, @measurement_unit= ?, @sensor_guid = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = (name, type, location, m_unit)
	return flask_to_uuid(execute_procedure(conn, sql, params, True))


# Create a device or get GUID if it already exists
def create_device(conn, app_guid, name, location = None, dev_eui = None, join_eui = None, dev_addr = None):
	print('Get or create device.')
	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_GET_OR_CREATE_TTN_DEVICE] @application_guid= ?, @device_name= ?, @device_location= ?, @dev_eui= ?, @join_eui= ?, @dev_addr= ?, @device_guid = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = (app_guid, name, location, dev_eui, join_eui, dev_addr)
	return flask_to_uuid(execute_procedure(conn, sql, params, True))


def create_uplink(conn, device_guid, session_key_id, f_port, f_cnt, frm_payload, raw_bytes, consumed_airtime, warning_guid = None):
	print('Create uplink.')
	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_CREATE_TTN_UPLINK] @device_guid= ?, @warning_guid= ?, @session_key_id= ?, @f_port= ?, @f_cnt= ?, @frm_payload= ?, @raw_bytes= ?, @consumed_airtime= ?, @uplink_guid = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = (device_guid, warning_guid, session_key_id, f_port, f_cnt, frm_payload, raw_bytes, consumed_airtime)
	return flask_to_uuid(execute_procedure(conn, sql, params, True))


def create_datetime(conn, received_at, rx_guid = None, hop_guid = None, uplink_guid = None):
	print('Create datetime.')
	sql = """\
		EXEC [dbo].[PROC_CREATE_TTN_DATETIME] @rx_guid= ?, @hop_guid= ?, @uplink_guid= ?, @received_at= ?;
		"""
	params = (rx_guid, hop_guid, uplink_guid, received_at)
	return execute_procedure(conn, sql, params)


def create_rx(conn, gateway_guid, uplink_guid, rx):
	print('Create RX.')

	if 'packet_broker' in rx:	# If the JSON contains 'packet broker' entries treat it as a V1 JSON 
		sql = """\
			DECLARE @out UNIQUEIDENTIFIER;
			EXEC [dbo].[PROC_CREATE_TTN_RX] @gateway_guid= ?, @uplink_guid= ?, @rx_time= ?, @rx_timestamp= ?, @rssi= ?, @channel_rssi= ?, @snr= ?, @message_id= ?, @forwarder_net_id= ?, @forwarder_tenant_id= ?, @forwarder_cluster_id= ?, @home_network_net_id= ?, @home_network_tenant_id= ?, @home_network_cluster_id= ?, @rx_guid = @out OUTPUT;
			SELECT @out AS the_output;
			"""
		params = (gateway_guid, uplink_guid, rx['rx_time'], rx['rx_timestamp'], rx['rssi'], rx['channel_rssi'], rx['snr'], rx['message_id'], rx['forwarder_net_id'], rx['forwarder_tenant_id'], rx['forwarder_cluster_id'], rx['home_network_net_id'], rx['home_network_tenant_id'], rx['home_network_cluster_id'])
		rx_guid =  flask_to_uuid(execute_procedure(conn, sql, params, True))
	else:	# If 'packet broker' doesn't exist then treat is as a V2 JSON and isgnore the assitional variables
		sql = """\
			DECLARE @out UNIQUEIDENTIFIER;
			EXEC [dbo].[PROC_CREATE_TTN_RX] @gateway_guid= ?, @uplink_guid= ?, @rx_time= ?, @rx_timestamp= ?, @rssi= ?, @channel_rssi= ?, @snr= ?, @rx_guid = @out OUTPUT;
			SELECT @out AS the_output;
			"""
		params = (gateway_guid, uplink_guid, rx['rx_time'], rx['rx_timestamp'], rx['rssi'], rx['channel_rssi'], rx['snr'])
		rx_guid = flask_to_uuid(execute_procedure(conn, sql, params, True))

	create_uplink_token(conn, rx_guid, gateway_guid, rx['uplink_token'])	
	create_hop(conn, rx_guid, rx['packet_broker'])
	
	return rx_guid


def create_hop(conn, rx_guid, hop):
	print('Create hop.')
	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_CREATE_TTN_HOP] @rx_guid= ?, @sender_address= ?, @receiver_name= ?, @receiver_agent= ?, @hop_guid = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = (rx_guid, hop['sender_address'], hop['receiver_name'], hop['receiver_agent'])
	hop_guid = flask_to_uuid(execute_procedure(conn, sql, params, True))
	create_datetime(conn, hop['received_at'], hop_guid = hop_guid) # Only run if parsing a V1 JSON
	return hop_guid


def create_correlation_id(conn, rx_guid, correlation_id):
	print('Create correlation ID.')
	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_CREATE_TTN_CORRELATION_ID] @rx_guid= ?, @correlation_id= ?, @correlation_guid = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = (rx_guid, correlation_id)
	return flask_to_uuid(execute_procedure(conn, sql, params, True))


def create_uplink_token(conn, rx_guid, gateway_guid, uplink_token):
	print('Create uplink token.')
	sql = """\
		EXEC [dbo].[PROC_CREATE_TTN_UPLINK_TOKEN] @rx_guid= ?, @gateway_guid= ?, @uplink_token= ?;
		"""
	params = (rx_guid, gateway_guid, uplink_token)
	return execute_procedure(conn, sql, params)


def create_reading(conn, uplink_guid, sensor_guid, sensor_value):
	print('Create reading.')
	sql = """\
		EXEC [dbo].[PROC_CREATE_TTN_READING] @uplink_guid= ?, @sensor_guid= ?, @sensor_value= ?;
		"""
	params = (uplink_guid, sensor_guid, sensor_value)
	return execute_procedure(conn, sql, params)


def create_uplink_setting(conn, uplink_guid, uplink_settings):
	print('Create uplink setting.')
	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_CREATE_TTN_UPLINK_SETTING] @uplink_guid= ?, @bandwidth= ?, @spreading_factor= ?, @data_rate_index= ?, @coding_rate= ?, @frequency= ?, @setting_timestamp= ?, @uplink_setting_guid = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = (uplink_guid, uplink_settings['bandwidth'], uplink_settings['spreading_factor'], uplink_settings['data_rate_index'], uplink_settings['coding_rate'], uplink_settings['frequency'], uplink_settings['setting_timestamp'])
	return flask_to_uuid(execute_procedure(conn, sql, params, True))


@app.route('/uplink/messages', methods=['POST'])
def ttn_webhook():
	if config('X_DOWNLINK_APIKEY') not in request.headers:
		status_code = Response(status=401)
		return
	print('Request Authenticated')

	sensor_guids = []
	sensor_value = []
	warning_guid = None
	correlation_guid = []
	rx_list = []

	# Connect to DB and get an app context with teh connector
	conn = get_db(SQL_CONN_STR)

	proc = request.json	# Store the recieved JSON file from the request

	#TODO JSON SPLITTING AND PROCESSING HERE
	app_guid = create_application(conn, proc['end_device_ids']['application_ids']['application_id'])
	device_guid = create_device(conn, app_guid, proc['end_device_ids']['device_id'], dev_eui = proc['end_device_ids']['dev_eui'], join_eui = proc['end_device_ids']['join_eui'], dev_addr = proc['end_device_ids']['dev_addr'])
	
	if 'decoded_payload_warnings' in proc['uplink_message']:
		warning_guid = create_warning(conn, proc['uplink_message']['decoded_payload_warnings'])
	

	if warning_guid == None:
		uplink_guid = create_uplink(conn, device_guid, proc['uplink_message']['session_key_id'], proc['uplink_message']['f_port'], proc['uplink_message']['f_cnt'], proc['uplink_message']['frm_payload'], str(proc['uplink_message']['raw_bytes']), proc['uplink_message']['consumed_airtime'])
	else:
		uplink_guid = create_uplink(conn, device_guid, proc['uplink_message']['session_key_id'], proc['uplink_message']['f_port'], proc['uplink_message']['f_cnt'], proc['uplink_message']['frm_payload'], str(proc['uplink_message']['raw_bytes']), proc['uplink_message']['consumed_airtime'], warning_guid)	
	create_datetime(conn, proc['uplink_message']['received_at'], uplink_guid = uplink_guid)


	for row in proc['uplink_message']['rx_metadata']:
		rx_list.append(row)
		if 'location' in row:
			location_guid = create_location(conn, row['location']['latitude'], row['location']['longitude'])
			gateway_guid = create_gateway(conn, row['gateway_ids']['gateway_id'], location_guid)

	rx_guid = create_rx(conn, gateway_guid, uplink_guid, rx_list)	# Store created GUID for later dependencies and datetime creation
	create_datetime(conn, proc['received_at'], rx_guid = rx_guid)	# Create a datetime entry using the stored RX_GUID

	#	Parse sensor names from received JSON and get GUIDs
	for row in proc['uplink_message']['decoded_payload']['s_name']:
		sensor_guids.append(create_sensor(conn,row))
	#	Parse correlation IDs from received JSON and create entries
	for row in proc['correlation_ids']:
		correlation_guid.append(create_correlation_id(conn, rx_guid, row))

	create_uplink_setting(conn, uplink_guid, proc['uplink_message']['settings'])	#	Create an uplink settings entry for the recieved uplink

	for row in proc['uplink_message']['decoded_payload']['s_value']:
		sensor_value.append(row)
	i = 0
	for row in sensor_guids:
		create_reading(conn, uplink_guid, row, sensor_value[i])
		i = i + 1

	# Commit data and close open database connection
	commit_db()
	close_db()

	# Return status 200 (success) to the remote client
	status_code = Response(status=200)
	return status_code

if __name__ == '__main__':
	WSGIRequestHandler.protocol_version = "HTTP/1.1"
	app.run(host = '0.0.0.0', port = '80')