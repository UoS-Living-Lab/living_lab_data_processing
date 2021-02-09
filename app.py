import os
import pyodbc
import flask
from flask import Flask, request
from flask_httpauth import HTTPTokenAuth
import json
import sys

from werkzeug.serving import WSGIRequestHandler


# POST credentials info
with open("./config/.apiTokens.json") as f:
	accessTokens = json.load(f)

# Flask app & routes
app = Flask(__name__)
auth = HTTPTokenAuth(scheme='Bearer')

tokens = {
	"X-Downlink-Apikey": accessTokens['X-Downlink-Apikey']
}

@auth.verify_token
def verify_token(token):
	if token in tokens:
		return tokens[token]

@app.route('/', methods=['POST'])
@auth.login_required
def webhook():
	print(request)
	print("POST Recieved")
	status_code = flask.Response(status=200)
	return status_code

# Main body
if __name__ == '__main__':
	WSGIRequestHandler.protocol_version = "HTTP/1.1"
	app.run(host= '0.0.0.0', port = '80')
