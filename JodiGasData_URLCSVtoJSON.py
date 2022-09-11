import csv
import json
from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile

with urlopen('https://www.jodidata.org/_resources/files/downloads/gas-data/jodi_gas_csv_beta.zip') as zipresp:
    with ZipFile(BytesIO(zipresp.read())) as zfile:
        zfile.extractall()

with open(f'jodi_gas_beta.csv', 'r') as file:
    reader = csv.DictReader(file)
    list_of_dicts = []

    for row in reader:
        current_id = [
            row['REF_AREA'],
            row['ENERGY_PRODUCT'],
            row['FLOW_BREAKDOWN'],
            row['UNIT_MEASURE']
        ]
        current_time_obsvalue = [
            row['TIME_PERIOD'] + '-01',
            row['OBS_VALUE']
        ]
        if not any(i_dict['series_id'] == current_id for i_dict in list_of_dicts):
            data = {
                'series_id': current_id,
                'points': [current_time_obsvalue],
                'fields': {
                    'REF_AREA': row['REF_AREA'],
                    'ENERGY_PRODUCT': row['ENERGY_PRODUCT'],
                    'FLOW_BREAKDOWN': row['FLOW_BREAKDOWN'],
                    'UNIT_MEASURE': row['UNIT_MEASURE'],
                    'ASSESSMENT_CODE': row['ASSESSMENT_CODE'],
                    'SOURCE': 'https://www.jodidata.org/_resources/files/downloads/gas-data/jodi_gas_csv_beta.zip'
                }
            }
            list_of_dicts.append(data)
        else:
            for j_dict in list_of_dicts:
                if j_dict['series_id'] == current_id:
                    j_dict['points'].append(current_time_obsvalue)
                    j_dict['points'] = j_dict['points'][-5:]

for k_dict in list_of_dicts:
    k_dict['series_id'] = '\\'.join(k_dict['series_id'])

parsed = '[\n' + ',\n'.join(map(json.dumps, list_of_dicts)) + '\n]'

with open('seriesOutput.json', 'w') as file_out:
    file_out.write(parsed)

print(parsed)
