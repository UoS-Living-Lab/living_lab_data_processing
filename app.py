import os
import pyodbc
import flask
from flask import Flask, request
from flask_httpauth import HTTPTokenAuth
import json
import sys
import datetime

from werkzeug.serving import WSGIRequestHandler



# JSON & CSV storage directories
JSON_NAME = 'ttn_' + str(datetime.datetime.now()) + '.json'
CSV_DIR = os.getcwd() + '/data/csv/'
JSON_DIR = os.getcwd() + '/data/json/'


# POST credentials info
with open("./config/.apiTokens.json") as f:
	accessTokens = json.load(f)

# Flask app & routes
app = Flask(__name__)
auth = HTTPTokenAuth(scheme='Bearer')

tokens = {
	"X-Downlink-Apikey": accessTokens['X-Downlink-Apikey']
}


# Function to save the recieved JSON file to disk
def jsonDump(struct, name):
	print('JSON dump')
	# Open a file for writing, filename will always be unique so append functions uneccessary
	with open(JSON_DIR + JSON_NAME, 'w') as f:
		# Save the JSON to a JSON file on disk
		json.dump(struct, f)




#@auth.verify_token
def verify_token(token):
	if token in tokens:
		return tokens[token]

@app.route('/uplink/messages', methods=['POST'])
def uplink_messages():
	headers = request.headers
	auth = headers.get("X-Api-Key")
	if auth == verify_token(auth):
		status_code = flask.Response(status=200)

		print("Uplink Message JSON Recieved...")
		requestJSON = json.load(request.json)
		jsonDump(requestJSON, 'uplink_' + str(datetime.datetime.now()) + '.json') # Disabled for testing as it doesn't work on Windows

	else:
		status_code = flask.Response(status=401)
	
	return status_code

@app.route('/', methods=['POST'])
def root():
	headers = request.headers
	auth = headers.get("X-Api-Key")
	if auth == verify_token(auth):
		status_code = flask.Response(status=200)

		print("Root JSON Recieved...")
		requestJSON = json.load(request.json)
		jsonDump(requestJSON, 'root_' + str(datetime.datetime.now()) + '.json') # Disabled for testing as it doesn't work on Windows

	else:
		status_code = flask.Response(status=401)

	return status_code

# Main body
if __name__ == '__main__':
	WSGIRequestHandler.protocol_version = "HTTP/1.1"
	app.run(host= '0.0.0.0', port = '80')
