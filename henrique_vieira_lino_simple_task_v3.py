import csv
import json
from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile
import time
from collections import defaultdict
from datetime import date
import datetime
import sys


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
        row['UNIT_MEASURE'],
        row['ASSESSMENT_CODE']
    ])


def current_points_fields(row):
    return [
        row['TIME_PERIOD'] + '-01',
        float(row['OBS_VALUE']),
        {
            'REF_AREA': row['REF_AREA'],
            'ENERGY_PRODUCT': row['ENERGY_PRODUCT'],
            'FLOW_BREAKDOWN': row['FLOW_BREAKDOWN'],
            'UNIT_MEASURE': row['UNIT_MEASURE'],
            'ASSESSMENT_CODE': row['ASSESSMENT_CODE'],
            'SOURCE': SOURCE_URL
        }
    ]


def append_recent_data_to_temp_list(row, date_one_year_ago, data_temp_list):
    time_period_of_row = time.strptime(row["TIME_PERIOD"], "%Y-%m")
    if time_period_of_row >= date_one_year_ago:
        data_temp_list[current_series_id(row)].append(current_points_fields(row))


def extract_points_fields(points_and_fields):
    extract = {
        'points': [],
        'fields': points_and_fields[0][2]
    }
    for values in points_and_fields:
        extract['points'].append([values[0], values[1]])
    return extract


def data(series_id, extracted_points_fields):
    return {
        'series_id': series_id,
        'points': extracted_points_fields['points'],
        'fields': extracted_points_fields["fields"]
    }


def write_output_to_jsonl_and_stdout(data_list):
    with open('seriesOutput.jsonl', 'w') as file_output:
        for series in data_list:
            file_output.write(json.dumps(series) + "\n")
            sys.stdout.write((json.dumps(series) + "\n"))


def handle_csv():
    download_source()
    data_temp_list = defaultdict(list)
    data_list = []
    date_one_year_ago = time.strptime(str(date.today() - datetime.timedelta(days=365))[:-3], '%Y-%m')
    with open(f'STAGING_world_NewFormat.csv', 'r') as file_input:
        reader = csv.DictReader(file_input)
        for row in reader:
            append_recent_data_to_temp_list(row, date_one_year_ago, data_temp_list)
        for series_id, points_fields in data_temp_list.items():
            data_list.append(data(series_id, extract_points_fields(points_fields)))
    return write_output_to_jsonl_and_stdout(data_list)


start_time = time.time()
handle_csv()
print("This took %s seconds" % (time.time() - start_time))
