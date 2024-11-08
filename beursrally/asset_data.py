import json

with open('/home/ruben/Projects/finfacts/beursrally/assets.json', 'r') as file:
    data = json.load(file)

# for item in data:
#     print(item['Name'])
#     print(item['ISIN'])

for k,v in data[0].items():
    print(f"{k}: {v}")