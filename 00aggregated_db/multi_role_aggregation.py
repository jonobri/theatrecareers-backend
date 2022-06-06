import pandas as pd
from sqlalchemy import create_engine, update, text, Integer, Table

# Create engine
engine = create_engine('mysql://root:sqladmin@localhost:3306/atc_alchemy')
connection = engine.connect()

all_median_query = '''
SELECT total_length, total_count, total_length / total_count AS `role every n years`
FROM (
SELECT (MAX(con.last_year) - MIN(con.first_year)) AS total_length, SUM(count) AS total_count
FROM career c
JOIN contributor con ON con.id = c.contributorid 
WHERE c.length > 2
AND c.count > 2
AND roleid IN (3, 5, 6, 7, 9, 11, 14)
AND contributorid IN (
	SELECT id
	FROM contributor c
	WHERE first_year >= %(year0)s
	AND first_year <= %(year1)s
	AND stateid NOT IN (9)
)
GROUP BY contributorid) subquery1
'''

all_gender_median_query = '''
SELECT total_length, total_count, total_length / total_count AS `role every n years`
FROM (
SELECT (MAX(con.last_year) - MIN(con.first_year)) AS total_length, SUM(count) AS total_count
FROM career c
JOIN contributor con ON con.id = c.contributorid 
WHERE c.length > 2
AND c.count > 2
AND roleid IN (3, 5, 6, 7, 9, 11, 14)
AND contributorid IN (
	SELECT id
	FROM contributor c
	WHERE first_year >= %(year0)s
	AND first_year <= %(year1)s
	AND genderid IN (%(gender)s)
	AND stateid NOT IN (9)
)
GROUP BY contributorid) subquery1
'''

all_role_median_query = '''
SELECT total_length, total_count, total_length / total_count AS `role every n years`
FROM (
SELECT (MAX(con.last_year) - MIN(con.first_year)) AS total_length, SUM(count) AS total_count
FROM career c
JOIN contributor con ON con.id = c.contributorid 
WHERE c.length > 2
AND c.count > 2
AND roleid IN (3, 5, 6, 7, 9, 11, 14)
AND contributorid IN (
	SELECT con.id
	FROM contributor con
	JOIN career c ON c.contributorid = con.id
	WHERE first_year >= %(year0)s
	AND first_year <= %(year1)s
	AND c.roleid IN (%(role)s)
	AND stateid NOT IN (9)
) 
GROUP BY contributorid) subquery1
'''

median_query = '''
SELECT total_length, total_count, total_length / total_count AS `role every n years`
FROM (
SELECT (MAX(con.last_year) - MIN(con.first_year)) AS total_length, SUM(count) AS total_count
FROM career c
JOIN contributor con ON con.id = c.contributorid 
WHERE c.length > 2
AND c.count > 2
AND roleid IN (%(role)s)
AND contributorid IN (
	SELECT con.id
	FROM contributor con
	JOIN career c ON c.contributorid = con.id
	WHERE first_year >= %(year0)s
	AND first_year <= %(year1)s
	AND c.roleid IN (%(role)s)
	AND stateid NOT IN (9)
) 
GROUP BY contributorid) subquery1
'''

decades = [1950, 1960, 1970, 1980, 1990]

roles = [3, 5, 6, 7, 9, 11, 14]
roles_dict = pd.read_sql("SELECT * FROM role", connection).set_index('id').role.to_dict()

genders = [1, 2]
genders_dict = pd.read_sql("SELECT * FROM gender", connection).set_index('id').gender.to_dict()

query_params = {}

# all for decade
query_params['year0'] = 1950
query_params['year1'] = 2000

data = pd.read_sql(all_median_query, connection, params=query_params)
print(f'Career Data for full data all roles')
print(data.describe())
print('\n')


for decade in decades:
    query_params['year0'] = decade
    query_params['year1'] = decade + 9

    # all for decade
    data = pd.read_sql(all_median_query, connection, params=query_params)
    print(f'Career Data for all {decade}s roles')
    print(data.describe())
    print('\n')

    # for g in genders:
    #     # decade split by gender
    #     query_params['gender'] = g
    #
    #     data = pd.read_sql(all_gender_median_query, connection, params=query_params)
    #     print(f'Career Data for all {decade}s {genders_dict[g]} roles')
    #     print(data.describe())
    #     print('\n')

    # for r in roles:
    #     # split by role, all genders
    #     query_params['role'] = r
    #
    #     data = pd.read_sql(all_role_median_query, connection, params=query_params)
    #     print(f'Career Data for all gender {decade}s {roles_dict[r]}')
    #     print(data.describe())
    #     print('\n')

    #     for g in genders:
    #         # split by role, split by gender
    #         query_params['gender'] = g
    #
    #         data = pd.read_sql(median_query, connection, params=query_params)
    #         print(f'Career Data for all {decade}s {genders_dict[g]} {roles_dict[r]}')
    #         print(data.describe())
    #         print('\n')
