"""
Reads CSV files stored on the file system, 
formats them into a JSON structure, 
and POSTs them to the off-site listener. 
"""


__author__ = "Ethan Bellmer"
__version__ = "0.1"

import flask
import json
from decouple import config
from datetime import datetime
import time
import shutil
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from living_lab_functions.db import db_connect, execute_procedure, execute_procedure_no_return
from living_lab_functions.functions import str_to_uuid


FTP_DIR = config('NOVUS_FTP_DIR')
ARCHIVE_DIR = config('NOVUS_ARCHIVE_DIR')


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
			FTP_DATA_PATH = str(event.src_path).split('/')
			NOVUS_LOGGER_SERIAL = FTP_DATA_PATH[len(FTP_DATA_PATH) - 1]


			# PROCESS AND SEND DATA TO LISTENER HERE


			# Archive processed data
			ARCHIVE_FILEPATH = str('{0}/{1}_{2}').format(ARCHIVE_DIR, NOVUS_LOGGER_SERIAL, f'{dt:%Y-%M-%d_%H-%M-%S}')
			shutil.move(event.src_path, ARCHIVE_FILEPATH)

		else:
			return None
			

# Main body
if __name__ == '__main__':
	print('Running as Main')
	unitGUIDs = []	# Used getting all device GUIDs from API, and subsequently suppling data for devices using these GUIDS
	readingsJSON = []	# f


	watch = OnMyWatch()
	watch.run()