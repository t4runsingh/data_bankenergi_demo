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

#**************************************************Pricing Data**********************************************
today = date.today()
d = today - timedelta(days=1)
url = ('https://api.bmreports.com/BMRS/DERSYSDATA/v1?APIKey=3pyyl4iymgrn812&FromSettlementDate=%s&ToSettlementDate=%s&SettlementPeriod=*&ServiceType=xml') %(d,today)
r = requests.get(url, allow_redirects=True)
open('price_elexon.xml', 'wb').write(r.content)

# Open XML file
file = open(os.getcwd() + r'/price_elexon.xml', 'r')
  
# Read the contents of that file
contents = file.read()
  
soup = BeautifulSoup(contents, 'xml')
  
# Extracting the data
settlementDate = soup.find_all('settlementDate')
settlementPeriod = soup.find_all('settlementPeriod')
systemSellPrice = soup.find_all('systemSellPrice')

data = []
  
# Loop to store the data in a list named 'data'
for i in range(0, len(systemSellPrice)):
    rows = [ settlementDate[i].get_text(), settlementPeriod[i].get_text(), systemSellPrice[i].get_text()]
    data.append(rows)
  
# Converting the list into dataframe
df = pd.DataFrame(data, columns=['settlementDate', 'settlementPeriod', 'systemSellPrice' ], dtype = float)
df.rename(columns={ "settlementDate" : "date", "settlementPeriod" : "period", "systemSellPrice": "price" }, inplace = True)

df.to_csv('price_elexon.csv', index=False)
df = pd.read_csv(os.getcwd() + r'/price_elexon.csv')
df.insert(0, 'id', range(1, 1 + len(df)))
df.insert(0, column = "model", value = "price_elexon.Price") 
df.to_csv('price_elexon.csv', index=False)

os.remove(os.getcwd() + r'/price_elexon.xml')

#*************************************** csv to JSON  for price****************************************************
with open(os.getcwd() + r'/price_elexon.csv') as csvfile:
    labels = []
    mappings = []
    csvreader = csv.reader(csvfile, delimiter=',')
    # skips your header
    next(csvreader)
    for row in csvreader:
        labels.append({
            'model' : row[0],
            'id' : row[1],
            'fields': {'Date': row[2], 'Period': row[3], 'Price': row[4]}       
        })
    
with open('price_elexon.json', 'w') as f:
    # indent does the prettify
    json.dump(labels, f, indent=4)

#*********************************************************
folder = os.getcwd() + r'/price_elexon.csv'

if os.path.exists(folder):
   os.remove(folder)