import json

import numpy as np

a = np.array([[10, 7, 4], [3, 2, 1]])
print(a)

x = np.percentile(a, 50)
print(x)

x = np.percentile(a, 50, axis=0)
print(x)

with open('metrics-result.json') as json_file:
    metrics_conf_json = json.load(json_file)