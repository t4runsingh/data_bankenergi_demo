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

#**************************************************************************carbon intensity****************************************************************
today = date.today()
d = today - timedelta(days=1)
url = 'https://api.carbonintensity.org.uk/regional/intensity/%s/%s/regionid/13' %(d, today)
r = requests.get(url, allow_redirects=True)
open('carbon_intensity.json', 'wb').write(r.content)

from copy import deepcopy
import pandas


def cross_join(left, right):
    new_rows = [] if right else left
    for left_row in left:
        for right_row in right:
            temp_row = deepcopy(left_row)
            for key, value in right_row.items():
                temp_row[key] = value
            new_rows.append(deepcopy(temp_row))
    return new_rows


def flatten_list(data):
    for elem in data:
        if isinstance(elem, list):
            yield from flatten_list(elem)
        else:
            yield elem


def json_to_dataframe(data_in):
    def flatten_json(data, prev_heading=''):
        if isinstance(data, dict):
            rows = [{}]
            for key, value in data.items():
                rows = cross_join(rows, flatten_json(value, prev_heading + '.' + key))
        elif isinstance(data, list):
            rows = []
            for i in range(len(data)):
                [rows.append(elem) for elem in flatten_list(flatten_json(data[i], prev_heading))]
        else:
            rows = [{prev_heading[1:]: data}]
        return rows

    return pandas.DataFrame(flatten_json(data_in))

with open(os.getcwd() + r'/carbon_intensity.json') as json_file:
    json_data = json.load(json_file)

df = json_to_dataframe(json_data)
df.drop(['data.regionid', 'data.dnoregion', 'data.shortname', 'data.data.intensity.index'], inplace=True, axis=1)
#, 'data.data.generationmix.fuel','data.data.generationmix.perc'
df.rename(columns = {'data.data.from':'From', 'data.data.to':'To', 'data.data.intensity.forecast':'Intensity','data.data.generationmix.fuel':'Fuel','data.data.generationmix.perc':'Value'}, inplace = True)
df.to_csv('carbon_intensity1.csv', mode='w', index=False)
data = pd.read_csv(os.getcwd() + r'/carbon_intensity1.csv')
for i in range(data.shape[0]): 
    if i%9 != 0:
                 data = data.drop([i], axis = 0)
pd.to_datetime(df['To'])
data.drop(['Fuel', 'Value'] , inplace=True, axis=1)
data.to_csv('carbon_intensity.csv', encoding='utf-8', index=False)
df = pd.read_csv(os.getcwd() + r'/carbon_intensity.csv')
df.insert(0, 'id', range(1, 1 + len(df)))
df.insert(0, column = "model", value = "carbon_intensity.Intensity")
df.to_csv('carbon_intensity.csv', index=False)
os.remove(os.getcwd() + r'/carbon_intensity.json')
os.remove(os.getcwd() + r'/carbon_intensity1.csv')


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



#******************************************************* making csv folder ************************************************************

pathlib.Path(os.getcwd() + r'/bankenergi_demodata_csv').mkdir(parents=True, exist_ok=True)

src_folder = os.getcwd()
dst_folder = (os.getcwd() + r'/bankenergi_demodata_csv/')

# Search files with .txt extension in source directory
pattern = "/*.csv"
files = glob.glob(src_folder + pattern)

# move the files with txt extension
for file in files:
    # extract file name form file path
    file_name = os.path.basename(file)
    shutil.move(file, dst_folder + file_name)


#*************************************** csv to JSON  for carbon intensity************************************************
with open(os.getcwd() + r'/bankenergi_demodata_csv/carbon_intensity.csv') as csvfile:
    labels = []
    mappings = []
    csvreader = csv.reader(csvfile, delimiter=',')
    # skips your header
    next(csvreader)
    for row in csvreader:
        labels.append({
            'model' : row[0],
            'id' : row[1],
            'fields': {'From': row[2], 'To': row[3], 'Intensity': row[4]}       
        })
    
with open('carbon_intensity.json', 'w') as f:
    # indent does the prettify
    json.dump(labels, f, indent=4)

#*************************************** csv to JSON  for demand****************************************************
with open(os.getcwd() + r'/bankenergi_demodata_csv/demand_elexon.csv') as csvfile:
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

#*************************************** csv to JSON  for frequency****************************************************
with open(os.getcwd() + r'/bankenergi_demodata_csv/freq_elexon.csv') as csvfile:
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

#*************************************** csv to JSON  for price****************************************************
with open(os.getcwd() + r'/bankenergi_demodata_csv/price_elexon.csv') as csvfile:
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

#*************************************** csv to JSON  for solar****************************************************
with open(os.getcwd() + r'/bankenergi_demodata_csv/solar.csv') as csvfile:
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

#*************************************** csv to JSON  for transmit_elexon****************************************************
with open(os.getcwd() + r'/bankenergi_demodata_csv/transmit_elexon.csv') as csvfile:
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

#*************************************** csv to JSON  for wind onshore****************************************************
with open(os.getcwd() + r'/bankenergi_demodata_csv/wind_onshore.csv') as csvfile:
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
with open(os.getcwd() + r'/bankenergi_demodata_csv/wind_offshore.csv') as csvfile:
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

#*************************************** csv to JSON  fuel type**********************************************************
with open(os.getcwd() + r'/bankenergi_demodata_csv/fuel_elexon.csv') as csvfile:
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

#********************************************************* making json folder **********************************************************


pathlib.Path(os.getcwd() + r'/bankenergi_demodata_json').mkdir(parents=True, exist_ok=True)

src_folder = os.getcwd()
dst_folder = (os.getcwd() + r'/bankenergi_demodata_json/')

# Search files with .txt extension in source directory
pattern = "/*.json"
files = glob.glob(src_folder + pattern)

# move the files with txt extension
for file in files:
    # extract file name form file path
    file_name = os.path.basename(file)
    shutil.move(file, dst_folder + file_name)

#****************************************************** deleting csv folder ************************************************

folder = os.getcwd() + r'/bankenergi_demodata_csv'

if os.path.exists(folder):
    shutil.rmtree(folder)
