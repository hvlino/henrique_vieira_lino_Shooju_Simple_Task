import csv
import json

with open('jodi_gas_beta.csv', 'r') as file:
    reader = csv.DictReader(file)
    next(reader)
    data = {"series_id": [], "points": [], "fields": {"country": "", "concept": "", "units": [], "source": ""}}
    countriesList = []
    datesList = []

    for i, row in enumerate(reader):
        if row["REF_AREA"] == 'BR':
            datesList.append(row["TIME_PERIOD"])
            datesList = datesList[-5:]
            for j, el in enumerate(datesList):
                key = "series_id"
                data.setdefault(key, [])
                [data["series_id"].append(row['REF_AREA']) for i in [row['REF_AREA']] if i not in data[key]]

                key = "points"
                data[key].append([datesList[j], row["OBS_VALUE"]])

                key = "fields"
                data[key]['country'] = ([row["REF_AREA"]])
                data[key]['concept'] = "Natural Gas"
                if row['UNIT_MEASURE'] not in data[key]['units']:
                    data[key]['units'].append(row['UNIT_MEASURE'])
                data[key]['source'] = "The JODI Gas World Database"

parsed = json.dumps(data, indent=4)
print(parsed)
