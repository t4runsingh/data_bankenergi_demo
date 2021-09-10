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

#*********************************************************Frequency_elexon***********************************

url = 'https://api.bmreports.com/BMRS/FREQ/v1?APIKey=3pyyl4iymgrn812&ServiceType=xml'
r = requests.get(url, allow_redirects=True)
open('freq_elexon.xml', 'wb').write(r.content)


# Open XML file
file = open(os.getcwd() + r'/freq_elexon.xml', 'r')
  
# Read the contents of that file
contents = file.read()
  
soup = BeautifulSoup(contents, 'xml')
  
# Extracting the data
reportSnapshotTime = soup.find_all('reportSnapshotTime')
spotTime = soup.find_all('spotTime')
frequency = soup.find_all('frequency')

data = []
  
# Loop to store the data in a list named 'data'
for i in range(0, len(frequency)):
    rows = [ reportSnapshotTime[i].get_text(), spotTime[i].get_text(), frequency[i].get_text()]
    data.append(rows)
  
# Converting the list into dataframe
df = pd.DataFrame(data, columns=['reportSnapshotTime', 'spotTime', 
                                 'frequency' ], dtype = float)
df.rename(columns={ "reportSnapshotTime" : "date", "spotTime" : "time", "frequency": "frequency" }, inplace = True)

df.to_csv('freq_elexon.csv', index=False)
df = pd.read_csv(os.getcwd() + r'/freq_elexon.csv')
df.insert(0, 'id', range(1, 1 + len(df)))
df.insert(0, column = "model", value = "freq_elexon.freq") 
df.to_csv('freq_elexon.csv', index=False)

os.remove(os.getcwd() + r'/freq_elexon.xml')

#*************************************** csv to JSON  for frequency****************************************************
with open(os.getcwd() + r'/freq_elexon.csv') as csvfile:
    labels = []
    mappings = []
    csvreader = csv.reader(csvfile, delimiter=',')
    # skips your header
    next(csvreader)
    for row in csvreader:
        labels.append({
            'model' : row[0],
            'id' : row[1],
            'fields': {'Date': row[2], 'Time': row[3], 'Frequency': row[4]}       
        })
    
with open('freq_elexon.json', 'w') as f:
    # indent does the prettify
    json.dump(labels, f, indent=4)
#*********************************************************
folder = os.getcwd() + r'/freq_elexon.csv'

if os.path.exists(folder):
    os.remove(folder)