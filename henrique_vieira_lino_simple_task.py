from collections import defaultdict
from datetime import date
from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile
import csv
import datetime
import json
import sys
import time

SOURCE_URL = "https://www.jodidata.org/_resources/files/downloads/gas-data/GAS_world_NewFormat.zip"
RANGE_DAYS = 365


def download_source():
    with urlopen(SOURCE_URL) as zip_resp:
        with ZipFile(BytesIO(zip_resp.read())) as z_file:
            z_file.extractall()


def current_series_id(row):
    return "\\".join([
        row["REF_AREA"],
        row["ENERGY_PRODUCT"],
        row["FLOW_BREAKDOWN"],
        row["UNIT_MEASURE"],
        row["ASSESSMENT_CODE"]
    ])


def current_points_fields(row):
    return [
        row["TIME_PERIOD"] + "-01",
        float(row["OBS_VALUE"]),
        {
            "REF_AREA": row["REF_AREA"],
            "ENERGY_PRODUCT": row["ENERGY_PRODUCT"],
            "FLOW_BREAKDOWN": row["FLOW_BREAKDOWN"],
            "UNIT_MEASURE": row["UNIT_MEASURE"],
            "ASSESSMENT_CODE": row["ASSESSMENT_CODE"],
            "SOURCE": SOURCE_URL
        }
    ]


def append_row_to_dict(row, limit_date, data_dict):
    time_period_of_row = time.strptime(row["TIME_PERIOD"], "%Y-%m")
    if time_period_of_row >= limit_date:
        data_dict[current_series_id(row)].append(current_points_fields(row))


def extract_points_fields(points_fields):
    extract = {
        "points": [],
        "fields": points_fields[0][2]
    }
    for values in points_fields:
        extract["points"].append([values[0], values[1]])
    return extract


def generate_data(series_id, extracted_points_fields):
    return {
        "series_id": series_id,
        "points": extracted_points_fields["points"],
        "fields": extracted_points_fields["fields"]
    }


def date_ago(days):
    return time.strptime(str(date.today() - datetime.timedelta(days))[:-3], "%Y-%m")


def get_dict_from_csv(filename):
    date_limit = date_ago(RANGE_DAYS)
    data_dict = defaultdict(list)
    with open(filename, "r") as file_input:
        reader = csv.DictReader(file_input)
        for row in reader:
            append_row_to_dict(row, date_limit, data_dict)
    return data_dict


def write_output_to_json_and_stdout(json_list):
    with open("seriesOutput.json", "w") as file_output:
        for series in json_list:
            file_output.write(json.dumps(series) + "\n")
            sys.stdout.write((json.dumps(series) + "\n"))


def append_data_to_json():
    download_source()
    json_output = []
    data_dict = get_dict_from_csv("STAGING_world_NewFormat.csv")
    for series_id, points_fields in data_dict.items():
        data = generate_data(series_id, extract_points_fields(points_fields))
        json_output.append(data)
    return write_output_to_json_and_stdout(json_output)


start_time = time.time()
append_data_to_json()
print("\n This took %s seconds" % (time.time() - start_time))
