import pandas as pd

import pyodbc

import requests
import json



import datetime
import logging

import azure.functions as func


from decouple import config

BASE_URL = "https://cwc-portal.com/portal/arduino/1.0/apiv2.php"
USER_KEY = config('SEL_USER_KEY')
API_KEY = config('SEL_API_KEY')


def getData():
	print('GetDate')

# Testing timer functions for Azure
def main(selTimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if selTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)