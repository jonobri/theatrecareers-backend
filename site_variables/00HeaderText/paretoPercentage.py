import pandas as pd
from sqlalchemy import create_engine

# Create engine
engine = create_engine('mysql://root:sqladmin@localhost:3306/atc_alchemy')
connection = engine.connect()
connection.execute("SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));")

cumulative_distribution_query = '''
SELECT contributorid, SUM(count) as role_count
FROM career c
WHERE roleid IN (3, 5, 6, 7, 9, 11, 14)
AND contributorid IN (
	SELECT id
	FROM contributor c
	WHERE first_year >= 1950
    AND first_year <= 1999
)
GROUP BY contributorid 
ORDER BY role_count DESC
'''

data = pd.read_sql(cumulative_distribution_query, connection)['role_count']

total = sum(data.values)
count = len(data.values)

print(total)
print(count)

accumulator = 0
practitioners_cumulative = []
credits_cumulative = []

for index, val in enumerate(data.values, start=1):
    percent_practitioners = index / count * 100
    practitioners_cumulative.append(percent_practitioners)
    accumulator += val
    percent_count = accumulator / total * 100
    credits_cumulative.append(percent_count)

practitioners_cumulative = pd.Series(practitioners_cumulative)
credits_cumulative = pd.Series(credits_cumulative)

df = pd.concat([data, practitioners_cumulative, credits_cumulative], axis=1).set_axis(['rolecount', 'percent_practitioners', 'percent_roles'], axis=1)

df = df[['percent_practitioners', 'percent_roles']]

out = df.loc[(df['percent_roles'] > 79.99) & (df['percent_roles'] < 80.01)]['percent_practitioners'].mean()

paretoPercentage = round(out)

print(paretoPercentage)