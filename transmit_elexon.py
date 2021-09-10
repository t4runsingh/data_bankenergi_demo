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

#******************************************************Energy Transmitted ****************************************************

today = date.today()
d = today - timedelta(days=30)

url = ('https://api.bmreports.com/BMRS/DEVINDOD/v1?APIKey=3pyyl4iymgrn812&FromDate=%s&ToDate=%s&ServiceType=xml') %(d,today)
r = requests.get(url, allow_redirects=True)
open('transmit_elexon.xml', 'wb').write(r.content)

file = open(os.getcwd() + r'/transmit_elexon.xml', 'r')
  
# Read the contents of that file
contents = file.read()
  
soup = BeautifulSoup(contents, 'xml')
  
# Extracting the data
settlementDay = soup.find_all('settlementDay')
volume = soup.find_all('volume')


data = []
  
# Loop to store the data in a list named 'data'
for i in range(0, len(volume)):
    rows = [ settlementDay[i].get_text(), volume[i].get_text() ]
    data.append(rows)
  
# Converting the list into dataframe
df = pd.DataFrame(data, columns=['settlementDay', 'volume'], dtype = float)
df.rename(columns={ "settlementDay" : "date", "volume" : "volume" }, inplace = True)

df.to_csv('transmit_elexon.csv', index=False)
df = pd.read_csv(os.getcwd() + r'/transmit_elexon.csv')
df.insert(0, 'id', range(1, 1 + len(df)))
df.insert(0, column = "model", value = "transmit_elexon.transmit") 
df.to_csv('transmit_elexon.csv', index=False)

os.remove(os.getcwd() + r'/transmit_elexon.xml')

#*************************************** csv to JSON  for transmit_elexon****************************************************
with open(os.getcwd() + r'/transmit_elexon.csv') as csvfile:
    labels = []
    mappings = []
    csvreader = csv.reader(csvfile, delimiter=',')
    # skips your header
    next(csvreader)
    for row in csvreader:
        labels.append({
            'model' : row[0],
            'id' : row[1],
            'fields': {'Date': row[2], 'Volume': row[3]}       
        })
    
with open('transmit_elexon.json', 'w') as f:
    # indent does the prettify
    json.dump(labels, f, indent=4)

#*********************************************************
folder = os.getcwd() + r'/transmit_elexon.csv'

if os.path.exists(folder):
    os.remove(folder)