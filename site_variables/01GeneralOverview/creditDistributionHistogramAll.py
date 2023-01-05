import sys
import pandas as pd
from sqlalchemy import create_engine

sys.path.append('../01site_variables')

from site_variables.staticVariables import staticDatasetStart, staticDatasetEnd

# Create engine
engine = create_engine('mysql://root:sqladmin@localhost:3306/atc_alchemy')
connection = engine.connect()
connection.execute("SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));")

role_query = '''
SELECT count, COUNT(count) AS count_of_counts
FROM career c
WHERE roleid IN (3, 5, 6, 7, 9, 11, 14)
AND contributorid IN (
    SELECT id
    FROM contributor c
    WHERE first_year >= %(year0)s
    AND first_year <= %(year1)s
    AND stateid NOT IN (9)
)
GROUP BY count
ORDER BY count;
'''

query_params = {
    'year0': staticDatasetStart,
    'year1': staticDatasetEnd
}

data = pd.read_sql(role_query, connection, params=query_params)

print(data)