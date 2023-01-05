import pandas as pd
from sqlalchemy import create_engine

# Create engine
engine = create_engine('mysql://root:sqladmin@localhost:3306/atc_alchemy')
connection = engine.connect()
connection.execute("SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));")

female_performers_query = '''
SELECT COUNT(contributorid) AS all_actors
FROM career c
WHERE length > 2
AND count > 2
AND roleid IN (9)
AND contributorid IN (
	SELECT id
	FROM contributor c
	WHERE first_year >= 1950
    AND first_year <= 1999
    AND stateid NOT IN (9)
    AND genderid IN (2)
)

UNION ALL

SELECT COUNT(contributorid) AS all_contributors
FROM career c
WHERE length > 2
AND count > 2
AND roleid IN (9)
AND contributorid IN (
	SELECT id
	FROM contributor c
	WHERE first_year >= 1950
    AND first_year <= 1999
    AND stateid NOT IN (9)
    AND genderid IN (1, 2)
)
'''

data = pd.read_sql(female_performers_query, connection)

numberPerformersFemale = data.values[0][0]

numberPerformersTotal = data.values[1][0]

performerFemalePercentage = round(numberPerformersFemale / numberPerformersTotal * 100)

print(performerFemalePercentage)
