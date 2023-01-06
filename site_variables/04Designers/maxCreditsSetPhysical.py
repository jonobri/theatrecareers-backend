import pandas as pd

from sqlalchemy import create_engine

# Create engine
engine = create_engine('mysql://root:sqladmin@localhost:3306/atc_alchemy')
connection = engine.connect()
connection.execute("SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));")

query = '''
SELECT c.display_name, c.genderid, SUM(count) AS count, c.last_year - c.first_year AS length
FROM (
    SELECT contributorid, count, length
	FROM career c
	JOIN role r ON c.roleid = r.id
	WHERE length > 2
	AND count > 2
	AND roleid IN (5)
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
LIMIT 5
'''

data = pd.read_sql(query, connection)

print(data)

maxCreditsSetPhysical = {
    'Top Overall': data.loc[0],
    'Top Female': data.loc[data['genderid'] == 2]
}

print(maxCreditsSetPhysical)

print(maxCreditsSetPhysical['Top Overall']['display_name'])