import os, json
import pandas as pd
from pandas import json_normalize

import glob
pd.set_option('display.max_columns', None)

temp = pd.DataFrame()

path_to_json = "./data/json/"
json_pattern = os.path.join(path_to_json,'*.json')
file_list = glob.glob(json_pattern)

CSV_DIR = os.getcwd() + '/data/csv/'

# Function declaration
# Function to save the processed data to a CSV
def csvDump(fileName, struct, index_set = False, index_label_usr = False):
	print('CSV Dump')
	if os.path.exists(CSV_DIR + fileName + '.csv'):
		with open(CSV_DIR + fileName + '.csv', 'a', encoding="utf-8", newline="") as fd:
			struct.to_csv(fd, header=False, index=index_set)
	else:
		struct.to_csv(CSV_DIR + fileName + '.csv', index=index_set, index_label = index_label_usr)

def RESAMPLE_DATA(df, RESAMPLE_PERIOD = '60min'):
	#df.index.names = ['Datetime']
	df = df.resample(RESAMPLE_PERIOD).mean()	# Resample data to an average over a defined period
	df = df.reindex(pd.date_range(df.index.min(), df.index.max(), freq=RESAMPLE_PERIOD))
	return df

def RENAME_COLUMNS(df, valName, inplaceB = True):
	df.rename(columns={"PollTimeStamp": "Datetime", "VarValue": valName}, inplace = inplaceB)	# Rename used columns to more appropriate names
	return df


for file in file_list:
	with open(file) as json_file:
		jsonL = json.load(json_file)
	#jsonL = json.loads(file)
	data = json_normalize(jsonL)
	#data = pd.read_json(file, lines=True)
	temp = temp.append(data, ignore_index = True)

temp['received_at'] = pd.to_datetime(temp['received_at'])
temp = temp.set_index('received_at')
csvDump("mc_ws", RESAMPLE_DATA(temp), index_set = True, index_label_usr = "Datetime")

#temp.to_csv(CSV_DIR + "mc_ws" + '.csv')

print("Pause")