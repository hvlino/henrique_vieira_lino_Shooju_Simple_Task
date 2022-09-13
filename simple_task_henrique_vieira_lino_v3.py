

import csv
import json
from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile

SOURCE_URL = 'https://www.jodidata.org/_resources/files/downloads/gas-data/jodi_gas_csv_beta.zip'


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


def data(row):
    return {
        'series_id': current_series_id(row),
        'points': current_time_obs_value(row),
        'fields': {
            'REF_AREA': row['REF_AREA'],
            'ENERGY_PRODUCT': row['ENERGY_PRODUCT'],
            'FLOW_BREAKDOWN': row['FLOW_BREAKDOWN'],
            'UNIT_MEASURE': row['UNIT_MEASURE'],
            'ASSESSMENT_CODE': row['ASSESSMENT_CODE'],
            'SOURCE': SOURCE_URL
        }
    }


def get_dict_index_by_series_id(data_list, row):
    for index, i_dict in enumerate(data_list):
        if i_dict['series_id'] == current_series_id(row):
            return index
    return -1


def write_series_output_to_json(data_list):
    parsed_data_list = '[\n' + ',\n'.join(map(json.dumps, data_list)) + '\n]'
    with open('seriesOutput.json', 'w') as file_out:
        file_out.write(parsed_data_list)
    return parsed_data_list


def handle_csv():
    download_source()
    with open(f'jodi_gas_beta.csv', 'r') as file:
        reader = csv.DictReader(file)
        data_list = []
        for row in reader:
            index = get_dict_index_by_series_id(data_list, row)
            if index == -1:
                data_list.append(data(row))
            else:
                data_list[index]['points'].append(current_time_obs_value(row))
                data_list[index]['points'] = data_list[index]['points'][-5:]
    return write_series_output_to_json(data_list)


print(handle_csv())