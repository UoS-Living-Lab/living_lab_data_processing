"""
Listens for webhooks from TTN  
for the Salford Living Lab application.
"""


__author__ = "Ethan Bellmer"
__version__ = "1.0"


# Import libraries
import sys
from flask import Flask, request, abort, current_app, g
from flask.cli import with_appcontext
from flask.wrappers import Response
from flask_basicauth import BasicAuth
from werkzeug.serving import WSGIRequestHandler
import pandas as pd
from pandas import json_normalize
import pyodbc
from decouple import config

from living_lab_functions.db import execute_procedure, get_db, commit_db, close_db
from living_lab_functions.functions import flask_to_to_uuid, split_dataframe_rows, remove_trailing_values, air_quality_processing


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

app.config['BASIC_AUTH_USERNAME'] = config('MONNIT_WEBHOOK_UNAME')
app.config['BASIC_AUTH_PASSWORD'] = config('MONNIT_WEBHOOK_PWD')
app.config['BASIC_AUTH_FORCE'] = True
basic_auth = BasicAuth(app)


# Create the application or get the GUID if it already exists
def create_application(conn, app):
	print('Creating or getting application')
	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_GET_OR_CREATE_TTN_APPLICATION] @application_name= ?, @application_guid = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = (app['name'])
	return execute_procedure(conn, sql, params, True)


# Create a warning if one exists in the JSON
def create_warning(conn, warning = None):
	print('Creating warning.')
	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_CREATE_TTN_TTN_WARNING] @decoded_payload_warning= ?, @warning_guid = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = (warning)
	return execute_procedure(conn, sql, params, True)


# Create a gateway entry or get GUID if it already exists
def create_gateway(conn, gateway_name, location_guid):
	print('Create or get gateway.')
	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_GET_OR_CREATE_TTN_GATEWAY] @location_guid= ?, @gateway_name= ?, @location_guid = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = (location_guid, gateway_name)
	return execute_procedure(conn, sql, params, True)


# Create a location or get GUID if it already exists
def create_location(conn, latitude, longitude, source = None):
	print('Get or create location.')
	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_GET_OR_CREATE_TTN_LOCATION] @latitude= ?, @longitude= ?, @source= ?, @location_guid = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = (latitude, longitude, source)
	return execute_procedure(conn, sql, params, True)


# Create a sensor or get GUID if it already exists
def create_sensor(conn, name, type = None, location = None, m_unit = None):
	print('Get or create sensor.')
	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_GET_OR_CREATE_TTN_SENSOR] @sensor_name= ?, @sensor_type= ?, @sensor_location= ?, @measurement_unit= ?, @sensor_guid = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = (name, type, location, m_unit)
	return execute_procedure(conn, sql, params, True)


# Create a device or get GUID if it already exists
def create_device(conn, app_guid, name, location = None, dev_eui = None, join_eui = None, dev_addr = None):
	print('Get or create device.')
	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_GET_OR_CREATE_TTN_DEVICE] @application_guid= ?, @device_name= ?, @device_location= ?, @dev_eui= ?, @join_eui= ?, @dev_addr= ?, @device_guid = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = (app_guid, name, location, dev_eui, join_eui, dev_addr)
	return execute_procedure(conn, sql, params, True)


def create_uplink(conn, device_guid, session_key_id, f_port, f_cnt, frm_payload, raw_bytes, consumed_airtime, warning_guid = None):
	print('Create uplink.')
	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_CREATE_TTN_UPLINK] @device_guid= ?, @warning_guid= ?, @session_key_id= ?, @f_port= ?, @f_cnt= ?, @frm_payload= ?, @raw_bytes= ?, @consumed_airtime= ?, @uplink_guid = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = (device_guid, warning_guid, session_key_id, f_port, f_cnt, frm_payload, raw_bytes, consumed_airtime)
	return execute_procedure(conn, sql, params, True)


def create_datetime(conn, received_at, rx_guid = None, hop_guid = None, uplink_guid = None):
	print('Create datetime.')
	sql = """\
		EXEC [dbo].[PROC_CREATE_TTN_DATETIME] @rx_guid= ?, @hop_guid= ?, @uplink_guid= ?, @received_at= ?;
		"""
	params = (rx_guid, hop_guid, uplink_guid, received_at)
	return execute_procedure(conn, sql, params)


def create_rx(conn, gateway_guid, uplink_guid, rx):
	print('Create RX.')
	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_CREATE_TTN_RX] @gateway_guid= ?, @uplink_guid= ?, @rx_time= ?, @rx_timestamp= ?, @rssi= ?, @channel_rssi= ?, @snr= ?, @message_id= ?, @forwarder_net_id= ?, @message_id= ?, @forwarder_tenant_id= ?, @forwarder_cluster_id= ?, @home_network_net_id= ?, @home_network_tenant_id= ?, @home_network_cluster_id= ?, @rx_guid = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = (gateway_guid, uplink_guid, rx['rx_time'], rx['rx_timestamp'], rx['rssi'], rx['channel_rssi'], rx['snr'], rx['message_id'], rx['forwarder_net_id'], rx['message_id'], rx['forwarder_tenant_id'], rx['forwarder_cluster_id'], rx['home_network_net_id'], rx['home_network_tenant_id'], rx['home_network_cluster_id'])
	return execute_procedure(conn, sql, params, True)


def create_hop(conn, gateway_guid, rx_guid, hop):
	print('Create hop.')
	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_CREATE_TTN_HOP] @gateway_guid= ?, @rx_guid= ?, @sender_address= ?, @receiver_name= ?, @receiver_agent= ?, @hop_guid = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = (gateway_guid, rx_guid, hop['sender_address'], hop['receiver_name'], hop['receiver_agent'])
	return execute_procedure(conn, sql, params, True)


def create_correlation_id(conn, rx_guid, correlation_id):
	print('Create correlation ID.')
	sql = """\
		DECLARE @out UNIQUEIDENTIFIER;
		EXEC [dbo].[PROC_CREATE_TTN_CORRELATION_ID] @rx_guid= ?, @correlation_id= ?, @correlation_guid = @out OUTPUT;
		SELECT @out AS the_output;
		"""
	params = (rx_guid, correlation_id)
	return execute_procedure(conn, sql, params, True)


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
	return execute_procedure(conn, sql, params, True)


@app.route('/imonnit', methods=['POST'])
@basic_auth.required
def ttn_webhook():
	print('Request Authenticated')
	jsonLoad = request.json	# Store the recieved JSON file from the request

	#TODO JSON SPLITTING AND PROCESSING HERE

	conn = get_db(SQL_CONN_STR)	# Connect to DB and get an app context with teh connector

	#TODO: DATA INSERTION HERE


if __name__ == '__main__':
	WSGIRequestHandler.protocol_version = "HTTP/1.1"
	app.run(host = '0.0.0.0', port = '80')