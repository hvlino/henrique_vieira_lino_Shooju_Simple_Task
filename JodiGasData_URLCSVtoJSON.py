import csv
import json
from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile


def download_source():
    with urlopen(source_url) as zip_resp:
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
        row['OBS_VALUE']
    ]


def data(row):
    return {
        'series_id': current_series_id(row),
        'points': [current_time_obs_value(row)],
        'fields': {
            'REF_AREA': row['REF_AREA'],
            'ENERGY_PRODUCT': row['ENERGY_PRODUCT'],
            'FLOW_BREAKDOWN': row['FLOW_BREAKDOWN'],
            'UNIT_MEASURE': row['UNIT_MEASURE'],
            'ASSESSMENT_CODE': row['ASSESSMENT_CODE'],
            'SOURCE': source_url
        }
    }


def handle_csv():
    download_source()
    with open(f'jodi_gas_beta.csv', 'r') as file:
        reader = csv.DictReader(file)
        data_list = []
        for row in reader:
            if not any(i_dict['series_id'] == current_series_id(row) for i_dict in data_list):
                data_list.append(data(row))
            else:
                for j_dict in data_list:
                    if j_dict['series_id'] == current_series_id(row):
                        j_dict['points'].append(current_time_obs_value(row))
                        j_dict['points'] = j_dict['points'][-5:]
    write_series_output_to_json(data_list)


def write_series_output_to_json(data_list):
    parsed_data_list = '[\n' + ',\n'.join(map(json.dumps, data_list)) + '\n]'
    with open('seriesOutput.json', 'w') as file_out:
        file_out.write(parsed_data_list)
    return print(parsed_data_list)


source_url = 'https://www.jodidata.org/_resources/files/downloads/gas-data/jodi_gas_csv_beta.zip'
handle_csv()
