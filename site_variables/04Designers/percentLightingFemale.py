import pandas as pd
from sqlalchemy import create_engine

# Create engine
engine = create_engine('mysql://root:sqladmin@localhost:3306/atc_alchemy')
connection = engine.connect()
connection.execute("SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));")

female_lighting_query = '''
SELECT COUNT(contributorid) AS all_actors
FROM career c
WHERE length > 2
AND count > 2
AND roleid IN (7)
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
AND roleid IN (7)
AND contributorid IN (
	SELECT id
	FROM contributor c
	WHERE first_year >= 1950
    AND first_year <= 1999
    AND stateid NOT IN (9)
    AND genderid IN (1, 2)
)
'''

data = pd.read_sql(female_lighting_query, connection)

numberLightingFemale = data.values[0][0]

numberLightingTotal = data.values[1][0]

lightingFemalePercentageOut = round(numberLightingFemale / numberLightingTotal * 100)

lightingFemalePercentage = {
    'Female': numberLightingFemale,
    'Total': numberLightingTotal,
    'Percent Female': lightingFemalePercentageOut
}

print(lightingFemalePercentage)