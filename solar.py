import requests
from datetime import datetime, timedelta, date
import json
import os
import csv
import glob
import shutil
from copy import deepcopy
import pandas as pd
from bs4 import BeautifulSoup
import pathlib

#*******************************************solar***************************************************
today = date.today()
d1 = today - timedelta(days=1)
url = 'https://api0.solar.sheffield.ac.uk/pvlive/v2?start=%sT00:00:00&end=%sT23:00:00&data_format=csv' %(d1,d1)
r = requests.get(url, allow_redirects=True)
open('solar_pre.csv', 'wb').write(r.content)
data=pd.read_csv(os.getcwd() + r'/solar_pre.csv') 
for i in range(data.shape[0]): 
    if i%2 != 0:
                 data = data.drop([i], axis = 0)
data.to_csv('d1.csv', encoding='utf-8', index=False)
Data = pd.read_csv(os.getcwd() + r'/d1.csv', parse_dates=['datetime_gmt'], infer_datetime_format=True)
Data['Date1'] = Data.datetime_gmt.dt.date
Data['Time1'] = Data.datetime_gmt.dt.time
Data.drop(['datetime_gmt', 'pes_id'], inplace=True, axis=1)
Data.rename(columns={ "Date1" : "date", "Time1" : "time" }, inplace = True)
Data.to_csv('solar.csv', mode='w', index=False)
df = pd.read_csv(os.getcwd() + r'/solar.csv')
df.insert(0, 'id', range(1, 1 + len(df)))
df.insert(0, column = "model", value = "solar.solar") 
df.to_csv('solar.csv', index=False)
os.remove(os.getcwd() + r'/solar_pre.csv')
os.remove(os.getcwd() + r'/d1.csv')

#*************************************** csv to JSON  for solar****************************************************
with open(os.getcwd() + r'/solar.csv') as csvfile:
    labels = []
    mappings = []
    csvreader = csv.reader(csvfile, delimiter=',')
    # skips your header
    next(csvreader)
    for row in csvreader:
        labels.append({
            'model' : row[0],
            'id' : row[1],
            'fields': {'Energy Generated': row[2], 'Date': row[3], 'Time': row[4]}       
        })
    
with open('solar.json', 'w') as f:
    # indent does the prettify
    json.dump(labels, f, indent=4)
#******************************************************************
folder = os.getcwd() + r'/solar.csv'

if os.path.exists(folder):
    os.remove(folder)

