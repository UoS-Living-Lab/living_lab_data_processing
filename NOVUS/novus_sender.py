"""
Reads CSV files stored on the file system, 
formats them into a JSON structure, 
and POSTs them to the off-site listener. 
"""


__author__ = "Ethan Bellmer"
__version__ = "0.2"


import flask
import json
import requests
import time
import shutil
import pandas as pd
from decouple import config
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from living_lab_functions.db import db_connect, execute_procedure, execute_procedure_no_return
from living_lab_functions.functions import str_to_uuid


FTP_DIR = config('NOVUS_FTP_DIR')
ARCHIVE_DIR = config('NOVUS_ARCHIVE_DIR')
SERVER_URL = config('NOVUS_SERVER_URL')


class OnMyWatch:
	# Set the directory on watch
	watchDirectory = FTP_DIR
 
	def __init__(self):
		self.observer = Observer()
 
	def run(self):
		event_handler = Handler()
		self.observer.schedule(event_handler, self.watchDirectory, recursive = True)
		self.observer.start()
		try:
			while True:
				time.sleep(5)
		except:
			self.observer.stop()
			print("Observer Stopped")

		self.observer.join()


class Handler(FileSystemEventHandler):
	@staticmethod
	def on_any_event(event):
		dt = datetime.now()
		if event.is_directory and event.event_type == 'created':
			print("Watchdog received folder create event - % s." % event.src_path)
			#time.sleep(300)
			FTP_SRC_PATH = str(event.src_path).split('/')
			FTP_CSV_DATA_PATH = "{0}/{1}/{2}".format(event.src_path, 'MemFlash', 'MemFlash.csv')
			NOVUS_LOGGER_SERIAL = FTP_SRC_PATH[len(FTP_SRC_PATH) - 1]


			# Read the csv data and convert it into a JSON string (possibly too large)
			CSV_DATA = pd.DataFrame(pd.read_csv(FTP_CSV_DATA_PATH, sep = ",", header = 1, index_col = False))
			DATA_JSON = CSV_DATA.to_json(orient = "records", date_format = "epoch", date_unit = "ms")


			params = {'NOVUS_SERIAL': NOVUS_LOGGER_SERIAL}
			POST_DATA_REQUEST = requests.post(SERVER_URL, params=params, data=DATA_JSON, headers={'Content-type': 'application/json', 'Accept': 'text/plain'})	# POST the JSON data to the server, adding the serial number of the logger as a parameter

			if POST_DATA_REQUEST.status_code == 200:
				print('Data posted successfully')
				# Archive processed data
				ARCHIVE_FILEPATH = str('{0}/{1}_{2}').format(ARCHIVE_DIR, NOVUS_LOGGER_SERIAL, f'{dt:%Y-%M-%d_%H-%M-%S}')
				shutil.move(event.src_path, ARCHIVE_FILEPATH)
			else:
				print('Error')
				# Archive processed data
				ARCHIVE_FILEPATH = str('{0}/{1}_{2}_{3}').format(ARCHIVE_DIR, NOVUS_LOGGER_SERIAL, 'POST-ERROR', f'{dt:%Y-%M-%d_%H-%M-%S}')
				shutil.move(event.src_path, ARCHIVE_FILEPATH)
		else:
			return None


# Main body
if __name__ == '__main__':
	print('Running as Main')
	watch = OnMyWatch()
	watch.run()