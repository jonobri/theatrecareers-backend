import pandas as pd
from sqlalchemy import create_engine

# Create engine
engine = create_engine('mysql://root:sqladmin@localhost:3306/atc_alchemy')
connection = engine.connect()
connection.execute("SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));")

n_actors_query = '''
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
)

UNION ALL

SELECT COUNT(contributorid) AS all_contributors
FROM career c
WHERE length > 2
AND count > 2
AND roleid IN (3, 5, 6, 7, 9, 11, 14)
AND contributorid IN (
	SELECT id
	FROM contributor c
	WHERE first_year >= 1950
    AND first_year <= 1999
    AND stateid NOT IN (9)
)
'''

data = pd.read_sql(n_actors_query, connection)

performerPercentage = round(data.values[0][0] / data.values[1][0] * 100)