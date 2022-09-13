import csv
import json
from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile
import time
from collections import defaultdict

SOURCE_URL = 'https://www.jodidata.org/_resources/files/downloads/gas-data/GAS_world_NewFormat.zip'


def download_source():
    with urlopen(SOURCE_URL) as zip_resp:
        with ZipFile(BytesIO(zip_resp.read())) as z_file:
            z_file.extractall()


def current_series_id(row):
    return '\\'.join([
        row['REF_AREA'],
        row['ENERGY_PRODUCT'],
        row['FLOW_BREAKDOWN'],
        row['UNIT_MEASURE']
    ])


def current_time_obs_value(row):
    return [
        row['TIME_PERIOD'] + '-01',
        float(row['OBS_VALUE'])
    ]


def data(row, series_id, time_period_obs_value):
    return {
        'series_id': series_id,
        'points': time_period_obs_value,
        'fields': {
            'REF_AREA': row['REF_AREA'],
            'ENERGY_PRODUCT': row['ENERGY_PRODUCT'],
            'FLOW_BREAKDOWN': row['FLOW_BREAKDOWN'],
            'UNIT_MEASURE': row['UNIT_MEASURE'],
            'ASSESSMENT_CODE': row['ASSESSMENT_CODE'],
            'SOURCE': SOURCE_URL
        }
    }


def write_series_output_to_json(test_list):
    parsed = json.dumps(test_list, indent=4)
    with open('seriesOutput.json', 'w') as file_out:
        file_out.write(parsed)
        return parsed


def handle_csv():
    download_source()
    data_default_dict_list = defaultdict(list)
    data_list = []
    with open(f'STAGING_world_NewFormat.csv', 'r') as f_input:
        reader = csv.DictReader(f_input)
        for row in reader:
            data_default_dict_list[current_series_id(row)].append(current_time_obs_value(row))
        for series_id, time_period_obs_value in data_default_dict_list.items():
            data_list.append(data(row, series_id, time_period_obs_value))
    return write_series_output_to_json(data_list)


start_time = time.time()
print(handle_csv())
print("This took %s seconds" % (time.time() - start_time))
