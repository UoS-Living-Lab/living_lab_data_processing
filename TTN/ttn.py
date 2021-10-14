"""

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


@app.route('/imonnit', methods=['POST'])
@basic_auth.required
# Primary function
def ttn_webhook():
	print('Request Authenticated')
	jsonLoad = request.json	# Store the recieved JSON file from the request

	#TODO JSON SPLITTING AND PROCESSING HERE

	conn = get_db(SQL_CONN_STR)	# Connect to DB and get an app context with teh connector

	#TODO: DATA INSERTION HERE


if __name__ == '__main__':
	WSGIRequestHandler.protocol_version = "HTTP/1.1"
	app.run(host = '0.0.0.0', port = '80')