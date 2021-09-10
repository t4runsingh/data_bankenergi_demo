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

#********************************************wind***********************************************************

today = date.today()
d2 = today - timedelta(days=1)
url = 'https://api.bmreports.com/BMRS/B1440/v1?APIKey=3pyyl4iymgrn812&SettlementDate=%s&Period=*&ServiceType=xml' %(d2)
r = requests.get(url, allow_redirects=True)
open('wind.xml', 'wb').write(r.content)


# Open XML file
file = open(os.getcwd() + r'/wind.xml', 'r')
  
# Read the contents of that file
contents = file.read()
  
soup = BeautifulSoup(contents, 'xml')
  
# Extracting the data
businessType = soup.find_all('businessType')
powerSystemResourceType = soup.find_all('powerSystemResourceType')
settlementDate = soup.find_all('settlementDate')
settlementPeriod = soup.find_all('settlementPeriod')
quantity = soup.find_all('quantity')

data = []

# Loop to store the data in a list named 'data'
for i in range(0, len(quantity)):
    rows = [ businessType[i].get_text(), powerSystemResourceType[i].get_text(), settlementDate[i].get_text(), settlementPeriod[i].get_text(), quantity[i].get_text()]
    data.append(rows)
  
# Converting the list into dataframe
df = pd.DataFrame(data, columns=['businessType', 'powerSystemResourceType', 'settlementDate' , 'settlementPeriod', 'quantity'
                                 'settlementDate' ], dtype = float)
df.rename(columns={ "powerSystemResourceType" : "wind_type", "businessType" : "type", "settlementDate": "date", "settlementPeriod": "period", "quantity" : "quantity" }, inplace = True)
df.to_csv('wind.csv', index=False)
df = pd.read_csv(os.getcwd() + r'/wind.csv')
df = df.replace(['\"','\"'], ['',''], regex=True)
df.to_csv('wind.csv', index=False)
data=pd.read_csv(os.getcwd() + r'/wind.csv') 
for i in range(data.shape[0]): 
    if i%3 == 0:
                 data = data.drop([i], axis = 0)
data.to_csv('wind1.csv', encoding='utf-8', index=False)
data=pd.read_csv(os.getcwd() + r'/wind1.csv', nrows=96) 
for i in range(data.shape[0]): 
    if i%2 == 0:
                 data = data.drop([i], axis = 0)
data.rename(columns={ "quantitysettlementDate" : "Energy Value" }, inplace = True)
data.to_csv('wind_onshore.csv', encoding='utf-8', index=False)
df = pd.read_csv(os.getcwd() + r'/wind_onshore.csv')
df.insert(0, 'id', range(1, 1 + len(df)))
df.insert(0, column = "model", value = "wind_onshore.wind_on") 
df.to_csv('wind_onshore.csv', index=False)
data=pd.read_csv(os.getcwd() + r'/wind1.csv', nrows=96) 
for i in range(data.shape[0]): 
    if i%2 != 0:
                 data = data.drop([i], axis = 0)
data.rename(columns={ "quantitysettlementDate" : "Energy Value" }, inplace = True)
data.to_csv('wind_offshore.csv', encoding='utf-8', index=False)
df = pd.read_csv(os.getcwd() + r'/wind_offshore.csv')
df.insert(0, 'id', range(1, 1 + len(df)))
df.insert(0, column = "model", value = "wind_onshore.wind_off") 
df.to_csv('wind_offshore.csv', index=False)
os.remove(os.getcwd() + r'/wind.csv')
os.remove(os.getcwd() + r'/wind1.csv')
os.remove(os.getcwd() + r'/wind.xml')

#*************************************** csv to JSON  for wind onshore****************************************************
with open(os.getcwd() + r'/wind_onshore.csv') as csvfile:
    labels = []
    mappings = []
    csvreader = csv.reader(csvfile, delimiter=',')
    # skips your header-
    next(csvreader)
    for row in csvreader:
        labels.append({
            'model' : row[0],
            'id' : row[1],
            'fields': {'wind type': row[3], 'Date': row[4], 'Period': row[5], 'Energy Generated': row[6]}       
        })
    
with open('wind_onshore.json', 'w') as f:
    # indent does the prettify
    json.dump(labels, f, indent=4)

#*************************************** csv to JSON  for wind offshore****************************************************
with open(os.getcwd() + r'/wind_offshore.csv') as csvfile:
    labels = []
    mappings = []
    csvreader = csv.reader(csvfile, delimiter=',')
    # skips your header
    next(csvreader)
    for row in csvreader:
        labels.append({
            'model' : row[0],
            'id' : row[1],
            'fields': {'wind type': row[3], 'Date': row[4], 'Period': row[5], 'Energy Generated': row[6]}       
        })
    
with open('wind_offshore.json', 'w') as f:
    # indent does the prettify
    json.dump(labels, f, indent=4)

#******************************************************************
folder = os.getcwd() + r'/wind_offshore.csv'

if os.path.exists(folder):
    os.remove(folder)

#*********************************************************
folder = os.getcwd() + r'/wind_onshore.csv'

if os.path.exists(folder):
    os.remove(folder)