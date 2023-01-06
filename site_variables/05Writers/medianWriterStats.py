import pandas as pd

from sqlalchemy import create_engine

# Create engine
engine = create_engine('mysql://root:sqladmin@localhost:3306/atc_alchemy')
connection = engine.connect()
connection.execute("SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));")

no_filter_generic_query = '''
SELECT MAX(last_year) - MIN(first_year) AS length, SUM(count) AS count, 
    (MAX(last_year) - MIN(first_year)) / SUM(count) AS 'role every n years'
FROM (
    SELECT contributorid, first_year, first_year + length AS last_year, length, count 
	FROM career c 
	WHERE length > 2
	AND count > 2
	AND roleid IN (14)
    AND contributorid IN (
        SELECT id
        FROM contributor c 
        WHERE first_year >= 1950
        AND first_year <= 1999
        AND stateid NOT IN (9)
    )
) t1
GROUP BY contributorid 
'''

data = pd.read_sql(no_filter_generic_query, connection)

medianCreditsWriter = data.describe().loc['50%']['count']
medianLengthWriter = data.describe().loc['50%']['length']

print(data.describe())
print(medianLengthWriter, medianCreditsWriter)

medianWriterStats = {
    'length': medianLengthWriter,
    'credits': medianCreditsWriter
}