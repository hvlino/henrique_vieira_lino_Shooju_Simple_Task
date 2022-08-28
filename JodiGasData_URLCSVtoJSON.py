import csv
import json
import urllib.request
import zipfile
from pathlib import Path


def download_url(url, save_path):
    with urllib.request.urlopen(url) as dl_file:
        with open(save_path, 'wb') as out_file:
            out_file.write(dl_file.read())


dir_to_download_to = Path(__file__).parent

download_url("https://www.jodidata.org/_resources/files/downloads/gas-data/jodi_gas_csv_beta.zip", f"{dir_to_download_to}/example.zip")

with zipfile.ZipFile(f"{dir_to_download_to}/example.zip", 'r') as zip_ref:
    zip_ref.extractall(dir_to_download_to)

with open(f"{dir_to_download_to}/jodi_gas_beta.csv", 'r') as file:
    reader = csv.DictReader(file)
    next(reader)
    data = {"series_id": [], "points": [], "fields": {"country": "", "concept": "", "units": [], "source": ""}}
    countriesList = []
    datesList = []

    for i, row in enumerate(reader):
        if row["REF_AREA"] == 'BR':
            datesList.append(row["TIME_PERIOD"])
            for j, el in enumerate(datesList):
                key = "series_id"
                data.setdefault(key, [])
                [data["series_id"].append(row['REF_AREA']) for i in [row['REF_AREA']] if i not in data[key]]

                key = "points"
                if [datesList[j]] not in data[key]:
                    data[key].append([datesList[j]])

                key = "fields"
                data[key]['country'] = ([row["REF_AREA"]])
                data[key]['concept'] = "Natural Gas"
                if row['UNIT_MEASURE'] not in data[key]['units']:
                    data[key]['units'].append(row['UNIT_MEASURE'])
                data[key]['source'] = "The JODI Gas World Database"

parsed = json.dumps(data, indent=4)
print(parsed)
