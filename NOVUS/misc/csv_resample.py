import os
import glob
import pandas as pd

CSV_DIR = os.getcwd() + "\\CSV\\"


def RESAMPLE_DATA(df, RESAMPLE_PERIOD = '10T'):
	df.index.names = ['DATE_TIME']
	df = df.resample(RESAMPLE_PERIOD).mean()	# Resample data to an average over a defined period
	df = df.reindex(pd.date_range(df.index.min(), df.index.max(), freq=RESAMPLE_PERIOD))
	return df


csv = pd.read_csv('novus_combined_csv.csv', low_memory = False)

csv["DATE_TIME"] = pd.to_datetime(csv["DATE_TIME"], errors='coerce')
csv.dropna(subset = ["DATE_TIME"], inplace = True)
csv.set_index("DATE_TIME", inplace = True)

csv = RESAMPLE_DATA(csv)
csv.to_csv( "novus_combined_csv_re.csv", index=True, encoding='utf-8', sep=',')
