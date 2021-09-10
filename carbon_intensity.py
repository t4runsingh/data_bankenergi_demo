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

#****************************************************************************************************
with open(os.getcwd() + r'/carbon_intensity.csv') as csvfile:
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
    
#************************************************************************************************************
folder = os.getcwd() + r'/carbon_intensity.csv'

if os.path.exists(folder):
    os.remove(folder)

