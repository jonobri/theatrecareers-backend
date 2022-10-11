import pandas as pd
from sqlalchemy import create_engine, update, text, Integer, Table
import matplotlib.pyplot as plt

# Create engine
engine = create_engine('mysql://root:sqladmin@localhost:3306/atc_alchemy')
connection = engine.connect()

query = '''
SELECT c.length, c.count, c.length / c.count AS `role every n years`
FROM career c
WHERE length >= 3
AND count >= 3
AND roleid IN (3, 5, 6, 7, 9, 11, 14)
AND first_year >= 1950
AND first_year <= 2005
AND contributorid IN (
	SELECT id
	FROM contributor c
	WHERE stateid NOT IN (9)
	AND genderid IN (1)
)
'''

query2 = '''
SELECT c.display_name, COUNT(DISTINCT e.event_name) AS works, COUNT(DISTINCT e.id) AS stagings, COUNT(DISTINCT e.id) / COUNT(DISTINCT e.event_name) AS stagings_per_work
FROM event e 
JOIN link_table lt ON e.id = lt.eventid 
JOIN contributor c ON lt.contributorid = c.id 
JOIN career car ON car.contributorid = c.id 
WHERE lt.roleid IN (14)
AND car.roleid IN (14)
AND length >= 3
AND count >= 3
AND car.first_year >= 1950
AND car.first_year <= 2005
AND c.id IN (
	SELECT id
	FROM contributor c
	WHERE stateid NOT IN (9)
)
GROUP BY c.display_name
ORDER BY stagings_per_work DESC, stagings DESC;
'''

query_md = '''
SELECT c.length, c.count, c.length / c.count AS `role every n years`
FROM career c
WHERE length >= 3
AND count >= 3
AND roleid IN (3, 5, 6, 7, 9, 11, 14)
AND first_year >= 1950
AND first_year <= 2005
AND contributorid IN (
	SELECT c2.id
	FROM career c
	JOIN `role` r ON r.id = c.roleid 
	JOIN contributor c2 ON c.contributorid = c2.id
	WHERE c.length > 2
	AND c.count > 2
	AND roleid IN (3, 5, 6, 7, 9, 11, 14)  
	AND contributorid IN (
		SELECT id
		FROM contributor c
		WHERE stateid NOT IN (9)
		AND first_year >= 1950  
    	AND first_year <= 2005  
	)
	GROUP BY contributorid 
	HAVING COUNT(contributorid) > 1
);
'''

out = pd.read_sql(query, connection).describe()

print(out)