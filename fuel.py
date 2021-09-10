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

#*******************************************************Generation fuel************************************

url = ('https://api.bmreports.com/BMRS/FUELINST/v1?APIKey=3pyyl4iymgrn812&ServiceType=xml') 


r = requests.get(url, allow_redirects=True)
open('fuel_elexon.xml', 'wb').write(r.content)


file = open(os.getcwd() + r'/fuel_elexon.xml', 'r')
  
# Read the contents of that file
contents = file.read()
  
soup = BeautifulSoup(contents, 'xml')
  
# Extracting the data
publishingPeriodCommencingTime = soup.find_all('publishingPeriodCommencingTime')
ccgt = soup.find_all('ccgt')
coal = soup.find_all('coal')
nuclear = soup.find_all('nuclear')
npshyd = soup.find_all('npshyd') #hydro
other = soup.find_all('other')

data = []
  
# Loop to store the data in a list named 'data'
for i in range(0, len(other)):
    rows = [ publishingPeriodCommencingTime[i].get_text(), ccgt[i].get_text(), coal[i].get_text(), nuclear[i].get_text(), npshyd[i].get_text(), other[i].get_text() ]
    data.append(rows)
  
# Converting the list into dataframe
df = pd.DataFrame(data, columns=['publishingPeriodCommencingTime', 'ccgt', 'coal', 'nuclear', 'npshyd', 'other'], dtype = float)
df.rename(columns={ "publishingPeriodCommencingTime" : "date", "ccgt" : "gas turbine", "npshyd" : "hydro" }, inplace = True)
df.to_csv('fuel_elexon.csv', index=False)
df = pd.read_csv(os.getcwd() + r'/fuel_elexon.csv')
df.insert(0, 'id', range(1, 1 + len(df)))
df.insert(0, column = "model", value = "fuel_elexon.fuel") 
df.to_csv('fuel_elexon.csv', index=False)

os.remove(os.getcwd() + r'/fuel_elexon.xml')
#*************************************** csv to JSON  fuel type**********************************************************
with open(os.getcwd() + r'/fuel_elexon.csv') as csvfile:
    labels = []
    mappings = []
    csvreader = csv.reader(csvfile, delimiter=',')
    # skips your header
    next(csvreader)
    for row in csvreader:
        labels.append({
            'model' : row[0],
            'id' : row[1],
            'fields': {'Date': row[2], 'Gas Turbine': row[3], 'Coal': row[4], 'Nuclear': row[5], 'Hydro': row[6]}       
        })
    
with open('fuel.json', 'w') as f:
    # indent does the prettify
    json.dump(labels, f, indent=4)
#*********************************************************
folder = os.getcwd() + r'/fuel_elexon.csv'

if os.path.exists(folder):
    os.remove(folder)