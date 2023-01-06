import sys
import pandas as pd
from sqlalchemy import create_engine

sys.path.append('../01site_variables')

from site_variables.staticVariables import staticDatasetStart, staticDatasetEnd

# Create engine
engine = create_engine('mysql://root:sqladmin@localhost:3306/atc_alchemy')
connection = engine.connect()
connection.execute("SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));")

performer_genders_query = '''
SELECT MAX(last_year) - MIN(first_year) AS length, SUM(count) AS count, 
    (MAX(last_year) - MIN(first_year)) / SUM(count) AS 'role every n years'
FROM (
    SELECT contributorid, first_year, first_year + length AS last_year, length, count 
	FROM career c 
	WHERE length > 2
    AND count > 2
    AND roleid IN (3)
    AND contributorid IN (
        SELECT id
        FROM contributor c 
        WHERE first_year >= %(year)s
        AND first_year <= %(year)s + 4
        AND stateid NOT IN (9)
        AND genderid IN (%(gender)s)
    )
) t1
GROUP BY contributorid 
'''

genders = [(1), (2), '1, 2']

years = range(1950, 1996, 5)

genders_dict = pd.read_sql("SELECT * FROM gender", connection).set_index('id').gender.to_dict()

all_data = {}



for gender in genders:
    if gender in genders_dict:
        title = genders_dict[gender]
    else: title = 'All'

    print('Doing: ' + title)

    output = pd.DataFrame(columns=['year','length_median','credits_median', 'credits_every_n_years_median'])
    query_params = {}

    for year in years:
        query_params['year'] = year
        query_params['gender'] = gender

        data = pd.read_sql(performer_genders_query, connection, params=query_params)
        out = [year]

        for col in data:
            out = out + data[col].describe().tolist()[5:6]

        series = pd.Series(out, index=output.columns)

        output = output.append(series, ignore_index=True)

    all_data[f'{title}'] = output

print(all_data)