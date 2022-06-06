import sys
import warnings
from datetime import datetime
import timeit

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from sqlalchemy import create_engine, update, text, Integer, Table

# Insert absolute path to backend files
sys.path.insert(1, '/Users/jonathanobrien/PycharmProjects/austheatrecareers/00backend_db')

# import from python files
# base
from backend_base import Base

# functions
from backend_functions import query_to_df, create_career_table

# variables
from backend_variables import FIRST_YEAR, LAST_YEAR, YEAR_TUPLE, TABLE_DICT_LIST, LINK_ROLE_STATEMENTS, \
    atc_engine, as_engine

# query strings
import backend_queries as bq

# classes
# Dunno why the IDE doesn't recognise this as being in-use, but please do not delete lol
import backend_classes

import get_data_22

import query

# TODO: This query currently doesn't do a weighted average for female data
#  or career medians, which will need to be fixed w/ count records in the
#  previous generator (get_data_22.py)
query = '''
SELECT year0, prac_count, role_count,
       single_role_count / prac_count AS single_role_perc,
       two_role_count / prac_count AS two_role_perc,
       three_role_count / prac_count AS three_role_perc,
       four_role_count / prac_count AS four_role_perc,
       four_role_count_roles / role_count AS four_role_roles_perc,
       female_prac_percentage, female_role_percentage,
       median_career_length, median_career_count
FROM
    (
    SELECT year0, SUM(prac_count) AS prac_count, SUM(role_count) AS role_count,
            -- Role distributions
            SUM(single_role_count) AS single_role_count,
            SUM(two_role_count) AS two_role_count,
            SUM(three_role_count) AS three_role_count,
            SUM(four_plus_role_count) AS four_role_count,
            SUM(four_plus_role_roles_count) AS four_role_count_roles,
            -- Gender splits
            AVG(female_prac_percentage) AS female_prac_percentage, AVG(female_role_percentage) AS female_role_percentage,
            -- Career lengths
            AVG(median_career_length) AS median_career_length, AVG(median_career_count) AS median_career_count
    FROM data_table
    WHERE role IN (%(role)s)
    AND state IN (%(state)s)
    AND gender IN (%(gender)s)
    GROUP BY year0
) t1
GROUP BY year0;
'''

# Create engine
engine = create_engine('mysql://root:sqladmin@localhost:3306/atc_alchemy')
connection = engine.connect()

states = [1, 2, 3, 4, 5, 6]
states_dict = pd.read_sql("SELECT * FROM state", connection).set_index('id').state.to_dict()

roles = [3, 5, 6, 7, 9, 10, 11, 12, 13, 14]
roles_dict = pd.read_sql("SELECT * FROM role", connection).set_index('id').role.to_dict()

genders = [1, 2]
genders_dict = pd.read_sql("SELECT * FROM gender", connection).set_index('id').gender.to_dict()


output = []

query_params = {'role': 4,
                'gender': 1,
                'state': 4}

columns = pd.read_sql(query, connection, params=query_params).columns[1:]

x_scale = pd.read_sql("SELECT DISTINCT year0 FROM data_table", connection).year0.tolist()

# TODO: This currently doesn't work for multiple items, but I might be able to fix
#  by inserting tuples into the lists above
i = 0
for col in columns:
    for gender in genders:
        query_params['gender'] = gender
        for role in roles:
            query_params['role'] = role

            chart_title = f'Role: {roles_dict[role]}, Gender: {genders_dict[gender]}, {col}'
            chart_file = f'{i}_{col}_{role}_{gender}.png'

            plt.title(chart_title)
            plt.xlabel('decade')
            plt.ylabel(col)

            for state in states:
                query_params['state'] = state

                data = pd.read_sql(query, connection, params=query_params)[col]

                plt.plot(x_scale, data, label = states_dict[state])

            plt.legend()

            plt.savefig('out/' + chart_file)

            # Clear the figure
            plt.clf()

            print(f'{chart_file} printed!')

    i += 1
