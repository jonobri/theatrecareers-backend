import pandas as pd
from sqlalchemy import create_engine

import allMediansGraph_queries as queries

# Create engine
engine = create_engine('mysql://root:sqladmin@localhost:3306/atc_alchemy')
connection = engine.connect()
connection.execute("SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));")

years = range(1950, 1996, 5)
roles = ['3, 5, 6, 7, 9, 11, 14', (3), (5), (6), (7), (9), (11), (14)]
genders = [(1), (2)]

roles_dict = pd.read_sql("SELECT * FROM role", connection).set_index('id').role.to_dict()
genders_dict = pd.read_sql("SELECT * FROM gender", connection).set_index('id').gender.to_dict()

all_data = {}

def get_data(query, query_params):
    data = pd.read_sql(query, connection, params=query_params)
    out = [year]

    for col in data:
        out = out + data[col].describe().tolist()[5:6]

    series = pd.Series(out, index=output.columns)
    return series


for role in roles:
    if role not in roles_dict:
        title = 'All'
    else:
        title = roles_dict[role]
    print('Doing: ' + title)

    output = pd.DataFrame(columns=['year','length_median','credits_median', 'credits_every_n_years_median'])
    query_params = {}

    for year in years:
        query_params['year'] = year
        query_params['role'] = role

        series = get_data(queries.career_by_contributor, query_params)

        output = output.append(series, ignore_index=True)

    all_data[f'{title}'] = output

for gender in genders:
    title = genders_dict[gender]
    print('Doing: ' + title)

    output = pd.DataFrame(columns=['year','length_median','credits_median', 'credits_every_n_years_median'])
    query_params = {}

    for year in years:
        query_params['year'] = year
        query_params['gender'] = gender

        series = get_data(queries.career_by_gender, query_params)

        output = output.append(series, ignore_index=True)

    all_data[f'{title}s'] = output

# Multicareers
title = 'Multicareers'
print('Doing: ' + title)

output = pd.DataFrame(columns=['year','length_median','credits_median', 'credits_every_n_years_median'])
query_params = {}

for year in years:
    query_params['year'] = year

    series = get_data(queries.multi_career_query, query_params)

    output = output.append(series, ignore_index=True)

all_data[f'{title}'] = output

allMediansGraph = all_data