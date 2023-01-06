import sys
import pandas as pd
from sqlalchemy import create_engine

# Create engine
engine = create_engine('mysql://root:sqladmin@localhost:3306/atc_alchemy')
connection = engine.connect()
connection.execute("SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));")

gender_percentages = '''
SELECT
    COALESCE(ROUND(pracF / (pracM + pracF) * 100, 2), 0) AS female_prac_percentage,
    COALESCE(ROUND(pracM / (pracM + pracF) * 100, 2), 0) AS male_prac_percentage,
    COALESCE(ROUND(rolesF / (rolesM + rolesF) * 100, 2), 0) AS female_role_percentage,
    COALESCE(ROUND(rolesM / (rolesM + rolesF) * 100, 2), 0) AS male_role_percentage
FROM
(
    SELECT
    COUNT(DISTINCT CASE WHEN genderid = 1 THEN contributorid END) as pracM,
    COUNT(DISTINCT CASE WHEN genderid = 2 THEN contributorid END) as pracF,
    COUNT(CASE WHEN genderid = 1 THEN CONCAT(contributorid, workid, organisationid, year) END) as rolesM,
    COUNT(CASE WHEN genderid = 2 THEN CONCAT(contributorid, workid, organisationid, year) END) as rolesF
    FROM
    (
        SELECT DISTINCT event.year, contributorid, contributor.genderid, workid, organisationid
        FROM atc_alchemy.link_table
        JOIN atc_alchemy.contributor ON contributor.id = link_table.contributorid
        JOIN atc_alchemy.event ON event.id = link_table.eventid
        WHERE contributor.genderid IN (1, 2)
        AND link_table.roleid IN (3, 5, 6, 7, 9, 11, 14)
        AND event.stateid NOT IN (9)
        AND event.year BETWEEN %(year0)s AND %(year0)s + 4
    ) AS counter_inner
) AS counter;
'''

years = range(1950, 1996)

query_params = {}

output = pd.DataFrame(columns=['year', 'female_prac_percentage', 'male_prac_percentage',
                               'female_role_percentage', 'male_role_percentage'])

for year in years:
    print(year)
    query_params['year0'] = year

    data = pd.read_sql(gender_percentages, connection, params=query_params)

    data['year'] = year
    output = output.append(data)

percentCareersFemaleGraph = output[['year', 'female_prac_percentage']]

print(percentCareersFemaleGraph)