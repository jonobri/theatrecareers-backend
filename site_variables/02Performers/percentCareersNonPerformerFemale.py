import pandas as pd
from sqlalchemy import create_engine

# Create engine
engine = create_engine('mysql://root:sqladmin@localhost:3306/atc_alchemy')
connection = engine.connect()
connection.execute("SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));")

female_performers_query = '''
SELECT COUNT(contributorid) AS all_females
FROM career c
WHERE length > 2
AND count > 2
AND roleid IN (3, 5, 6, 7, 11, 14)
AND contributorid IN (
	SELECT id
	FROM contributor c
	WHERE first_year >= 1950
    AND first_year <= 2004
    AND stateid NOT IN (9)
    AND genderid IN (2)
)

UNION ALL

SELECT COUNT(contributorid) AS all_females_males
FROM career c
WHERE length > 2
AND count > 2
AND roleid IN (3, 5, 6, 7, 11, 14)
AND contributorid IN (
	SELECT id
	FROM contributor c
	WHERE first_year >= 1950
    AND first_year <= 2004
    AND stateid NOT IN (9)
    AND genderid IN (1, 2)
)
'''

data = pd.read_sql(female_performers_query, connection)

numberCareersFemale = data.values[0][0]

numberCareersTotal = data.values[1][0]

percentCareersFemale = round(numberCareersFemale / numberCareersTotal * 100)

print(percentCareersFemale)
