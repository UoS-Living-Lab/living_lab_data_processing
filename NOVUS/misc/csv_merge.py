import os
import re
import glob
import pandas as pd
from tqdm import tqdm
from io import StringIO

CSV_DIR = os.getcwd() + "\\CSV\\"


def RESAMPLE_DATA(df, RESAMPLE_PERIOD = '10T'):
	df.index.names = ['DATE_TIME']
	df = df.resample(RESAMPLE_PERIOD).mean()	# Resample data to an average over a defined period
	df = df.reindex(pd.date_range(df.index.min(), df.index.max(), freq=RESAMPLE_PERIOD))
	return df


extension = 'csv'
all_filenames = [i for i in glob.glob(CSV_DIR + '*.{}'.format(extension))]

combined_csv = pd.DataFrame()

for filename in tqdm(all_filenames):
    with open(filename, 'r') as f:
        data = re.sub('\"', '', f.read(), flags=re.M)
        if combined_csv.empty:        
            combined_csv = pd.read_csv(StringIO(data), escapechar='\\', sep = ';', low_memory = False, parse_dates = [['DATE', 'TIME']], dayfirst = True)
        else:
            combined_csv = combined_csv.append(pd.read_csv(StringIO(data), escapechar='\\', sep = ';', low_memory = False, parse_dates = [['DATE', 'TIME']], dayfirst = True))


#combine all files in the list
#combined_csv = pd.concat([pd.read_csv(f, sep = ';', parse_dates = [['DATE', 'TIME']], dayfirst = True, low_memory = False) for f in all_filenames ])

#combined_csv = combined_csv[["10668", "10670", "10671", "10673", "10674", "10675", "10677"]]

combined_csv["DATE_TIME"] = pd.to_datetime(combined_csv["DATE_TIME"], errors='coerce')
combined_csv.dropna(subset = ["DATE_TIME"], inplace = True)
combined_csv.set_index("DATE_TIME", inplace = True)

combined_csv.to_csv( "novus_combined_csv.csv", index=True, encoding='utf-8', sep=',')
combined_csv = RESAMPLE_DATA(combined_csv)
combined_csv.to_csv( "novus_combined_csv_re.csv", index=True, index_label = "DATE_TIME", encoding='utf-8', sep=',')
