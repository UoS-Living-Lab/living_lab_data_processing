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
CSV_DIR = os.getcwd() + '/data/csv/'
JSON_DIR = os.getcwd() + '/data/json/'

# POST credentials info
with open("./config/.apiTokens.json") as f:
	accessTokens = json.load(f)


# Flask app & routes
app = Flask(__name__)
auth = HTTPTokenAuth(scheme='Bearer')
tokens = {
	"X-Downlink-Apikey": accessTokens['X-Downlink-Apikey'],
	"ID": accessTokens['ID']
}


# Function to save the recieved JSON file to disk
def jsonDump(struct, name):
	print('JSON dump')
	# Open a file for writing, filename will always be unique so append functions uneccessary
	with open(JSON_DIR + name, 'w') as f:
		# Save the JSON to a JSON file on disk
		json.dump(struct, f)


#	Authent Functions
def verify_token(token):
	if token in tokens:
		return tokens[token]


#	TTN Listener
@app.route('/uplink/messages', methods=['POST'])
def TTN_UPLINK_MESSAGE():
	headers = request.headers
	auth = headers.get("X-Api-Key")
	if auth == verify_token(auth):
		status_code = flask.Response(status=200)

		print("TTN Uplink Message JSON Recieved...")
		requestJSON = json.load(request.json)
		jsonDump(requestJSON, '/ttn/ttn_' + str(datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")) + '.json')
	else:
		status_code = flask.Response(status=401)
	return status_code


#	TTN Webhook Testing for other messages
#@app.route('/', methods=['POST'])
#def root():
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


#	NOVUS Listener
@app.route('/provision/activate', methods=['POST'])
def NOVUS_ACTIVATE():
	sys.stdout.flush()
	#headers = request.headers
	
	data = request.form.to_dict()
	devID = data["sn"]

	print("devID: " + str(devID))

	if devID == verify_token(devID):
		print("Method: " + str(request.method))
		print("Form: " + str(request.form))
		print("Headers: " + str(request.headers))
		print(str(request.host_url))
		status_code = flask.Response(status=200, response="68b30835a221da4f9ea940dac83871f8497a0000", content_type="text/plain")
	else:
		print("ID not in approved list...")
		status_code = flask.Response(status=401)
	return status_code

@app.route('/onep:v1/rpc/process', methods=['POST'])
def NOVUS_PROCESS():
	sys.stdout.flush()
	#headers = request.headers
	devID = request.form.get("ID")

	if devID == verify_token(devID):
		print("NOVUS JSON Received...")
		#print("Host URL: " + str(request.host_url))
		#print("Full Path: " + str(request.full_path))
		#print("Base URL: " + str(request.base_url))
		#print("URL: " + str(request.url))
		#print("JSON: " + str(request.json))
		requestJSON = json.loads(str(request.json).replace("'", '"'))
		jsonDump(requestJSON, '/novus/novus_' + devID + "_" + str(datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")) + '.json')

		status_code = flask.Response(status=200)
	else:
		print("ID not in approved list...")
		status_code = flask.Response(status=401)

	return status_code


# Main body
if __name__ == '__main__':
	WSGIRequestHandler.protocol_version = "HTTP/1.1"
	app.run(host= '0.0.0.0', port = '80')
