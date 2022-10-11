import sys
import warnings
from datetime import datetime
import timeit

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, update, text, Integer, Table

# Create engine
engine = create_engine('mysql://root:sqladmin@localhost:3306/atc_alchemy')
connection = engine.connect()

connection.execute("SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));")

cum_dist_query = '''
SELECT contributorid, SUM(count) as role_count
FROM career c
WHERE length > 2
AND count > 2
AND roleid IN (3, 5, 6, 7, 9, 11, 14)
AND contributorid IN (
	SELECT id
	FROM contributor c
	WHERE stateid NOT IN (9)
)
GROUP BY contributorid 
ORDER BY role_count DESC
'''

data = pd.read_sql(cum_dist_query, connection)['role_count']

total = sum(data.values)

count = len(data.values)

print(total)
print(count)

n = 1
old_val = 0
prac_cum = []
count_cum = []

for val in data.values:
    percent_practitioners = n / count * 100
    prac_cum.append(percent_practitioners)
    accumulator = val + old_val
    percent_count = accumulator / total * 100
    old_val = accumulator
    count_cum.append(percent_count)
    n += 1

prac_cum = pd.Series(prac_cum)
count_cum = pd.Series(count_cum)

print(data)
print(prac_cum)
print(count_cum)
df = pd.concat([data, prac_cum, count_cum], axis=1).set_axis(['rolecount', 'percent_practitioners', 'percent_roles'], axis=1)

print(df)

df = df[['percent_practitioners', 'percent_roles']]

df.plot()

plt.show()

# count, bins_count = np.histogram(data, bins=100)
#
# pdf = count /sum(count)
#
# cdf = np.cumsum(pdf)
#
# print(data)
#
# # plotting PDF and CDF
# plt.plot(bins_count[1:], pdf, color="red", label="PDF")
# plt.plot(bins_count[1:], cdf, label="CDF")
# plt.xlim(0,100)
# plt.legend()
# plt.show()
