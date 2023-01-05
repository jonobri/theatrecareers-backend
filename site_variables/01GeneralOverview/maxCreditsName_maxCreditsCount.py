import pandas as pd

from sqlalchemy import create_engine

# Create engine
engine = create_engine('mysql://root:sqladmin@localhost:3306/atc_alchemy')
connection = engine.connect()
connection.execute("SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));")

query = '''
SELECT c.display_name, SUM(count) AS count
FROM (
    SELECT contributorid, count
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
) t1
JOIN contributor c ON t1.contributorid = c.id
GROUP BY contributorid
ORDER BY count DESC
LIMIT 1
'''

data = pd.read_sql(query, connection)

maxCreditsName = data.loc[0]['display_name']
maxCreditsCount = data.loc[0]['count']

print(maxCreditsName, maxCreditsCount)
