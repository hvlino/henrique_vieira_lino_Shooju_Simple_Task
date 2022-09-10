import csv
import urllib.request
import zipfile
from pathlib import Path
import json

with urllib.request.urlopen('https://www.jodidata.org/_resources/files/downloads/gas-data/jodi_gas_csv_beta.zip') as f:
    with open(f'jodi_gas_csv_beta.zip', 'wb') as out_file:
        out_file.write(f.read())
with zipfile.ZipFile(f'jodi_gas_csv_beta.zip', 'r') as zip_ref:
    zip_ref.extractall(Path(__file__).parent)

with open(f'jodi_gas_beta.csv', 'r') as file:
    reader = csv.DictReader(file)
    dataList = []

    for row in reader:
        currentId = [row['REF_AREA'], row['ENERGY_PRODUCT'], row['FLOW_BREAKDOWN'], row['UNIT_MEASURE']]
        timeObsValue = [row['TIME_PERIOD'] + '-01', row['OBS_VALUE']]
        if not any(d['series_id'] == currentId for d in dataList):
            data = {
                'series_id': currentId,
                'points': [timeObsValue],
                'fields': {
                    'REF_AREA': row['REF_AREA'],
                    'ENERGY_PRODUCT': row['ENERGY_PRODUCT'],
                    'FLOW_BREAKDOWN': row['FLOW_BREAKDOWN'],
                    'UNIT_MEASURE': row['UNIT_MEASURE'],
                    'ASSESSMENT_CODE': row['ASSESSMENT_CODE'],
                    'SOURCE': 'https://www.jodidata.org/_resources/files/downloads/gas-data/jodi_gas_csv_beta.zip'
                }
            }
            dataList.append(data)
        else:
            for dic in dataList:
                if dic['series_id'] == currentId:
                    dic['points'].append(timeObsValue)
                    dic['points'] = dic['points'][-5:]

for d in dataList:
    d['series_id'] = '\\'.join(d['series_id'])
finalJson = '[\n' + ',\n'.join(map(json.dumps, dataList)) + '\n]'
with open('seriesOutput.json', 'w') as f:
    f.write(finalJson)
print(finalJson)
