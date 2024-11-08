import json

with open('/home/ruben/Projects/finfacts/beursrally/assets.json', 'r') as file:
    data = json.load(file)

assets = dict()
for item in data:
    if item["AsseType"] not in assets.keys():
        assets[item["AsseType"]] = dict()
    if item["ISIN"] not in assets[item["AsseType"]].keys():
        assets[item["AsseType"]][item["ISIN"]] = dict()
    for key in ["Name", "Ticker"]:
        assets[item["AsseType"]][item["ISIN"]][key] = item[key]

with open('/home/ruben/Projects/finfacts/beursrally/assets_filtered.json', 'w+') as file:
    json.dump(assets, file)