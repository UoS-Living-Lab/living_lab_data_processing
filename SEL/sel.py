import pandas as pd

import pyodbc

import requests
import json

import datetime

from decouple import config

BASE_URL = "https://cwc-portal.com/portal/arduino/1.0/apiv2.php"
USER_KEY = config('SEL_USER_KEY')
API_KEY = config('SEL_API_KEY')


def getUnits():
	API_ACTION = "units"

	FULL_URL = "{0}?user_key={1}&api_key={2}".format(BASE_URL, USER_KEY, API_KEY)

def getData(unitID):
	API_ACTION = "data"

	FULL_URL = "{0}?user_key={1}&api_key={2}&action={3}&interface={4}".format(BASE_URL, USER_KEY, API_KEY, API_ACTION, unitID)

def main():
	print('Running as main')