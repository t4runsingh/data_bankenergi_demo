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

#*******************************************Demand_elexon***************************************


url = ('https://api.bmreports.com/BMRS/ROLSYSDEM/v1?APIKey=3pyyl4iymgrn812&ServiceType=xml') 

r = requests.get(url, allow_redirects=True)
open('demand_elexon.xml', 'wb').write(r.content)

# Open XML file
file = open(os.getcwd() + r'/demand_elexon.xml', 'r')
  
# Read the contents of that file
contents = file.read()
  
soup = BeautifulSoup(contents, 'xml')
  
# Extracting the data
settDate = soup.find_all('settDate')
publishingPeriodCommencingTime = soup.find_all('publishingPeriodCommencingTime')
fuelTypeGeneration = soup.find_all('fuelTypeGeneration')

data = []
  
# Loop to store the data in a list named 'data'
for i in range(0, len(fuelTypeGeneration)):
    rows = [ settDate[i].get_text(), publishingPeriodCommencingTime[i].get_text(), fuelTypeGeneration[i].get_text()]
    data.append(rows)
  
# Converting the list into dataframe
df = pd.DataFrame(data, columns=['settDate', 'publishingPeriodCommencingTime', 
                                 'fuelTypeGeneration' ], dtype = float)
df.rename(columns={ "publishingPeriodCommencingTime" : "Time", "settDate" : "Date", "fuelTypeGeneration": "Demand" }, inplace = True)
df['Time'] = df['Time'].apply(lambda x: x[:5])
df.to_csv('demand_elexon.csv', index=False)
df = pd.read_csv(os.getcwd() + r'/demand_elexon.csv')
df.insert(0, 'id', range(1, 1 + len(df)))
df.insert(0, column = "model", value = "demand_elexon.Demand") 
df.to_csv('demand_elexon.csv', index=False)

os.remove(os.getcwd() + r'/demand_elexon.xml')

#*************************************** csv to JSON  for demand****************************************************
with open(os.getcwd() + r'/demand_elexon.csv') as csvfile:
    labels = []
    mappings = []
    csvreader = csv.reader(csvfile, delimiter=',')
    # skips your header
    next(csvreader)
    for row in csvreader:
        labels.append({
            'model' : row[0],
            'id' : row[1],
            'fields': {'Date': row[2], 'Time': row[3], 'Demand': row[4]}       
        })
    
with open('demand_elexon.json', 'w') as f:
    # indent does the prettify
    json.dump(labels, f, indent=4)
#*********************************************************
folder = os.getcwd() + r'/demand_elexon.csv'

if os.path.exists(folder):
    os.remove(folder)