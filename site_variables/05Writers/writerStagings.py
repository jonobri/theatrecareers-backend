import pandas as pd

from sqlalchemy import create_engine

# Create engine
engine = create_engine('mysql://root:sqladmin@localhost:3306/atc_alchemy')
connection = engine.connect()
connection.execute("SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));")

no_filter_generic_query = '''
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

data = pd.read_sql(no_filter_generic_query, connection)

medianWorks = data.describe().loc['50%']['works']
medianStagings = data.describe().loc['50%']['stagings']
medianStagingsPerWork = data.describe().loc['50%']['stagings_per_work']

print(data.describe())
print(medianWorks, medianStagings, medianStagingsPerWork)

writerStagings = {
    'Works': medianWorks,
    'Stagings': medianStagings,
    'Stagings per Work': medianStagingsPerWork
}
