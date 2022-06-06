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

role_query = '''
SELECT role_count AS count, COUNT(role_count) AS count_of_counts
FROM ( 
    SELECT c.contributorid, c.count AS role_count
    FROM career c 
    JOIN contributor c2 ON c.contributorid = c2.id
    WHERE c2.genderid IN (%(gender)s)
    AND c2.stateid IN (%(state)s)
    AND c.roleid IN (%(role)s)
    AND c2.first_year >= %(year0)s
    AND c2.first_year <= %(year1)s
    AND c.count > 2
) t1
GROUP BY role_count
ORDER BY role_count;
'''

length_query = '''
SELECT career_length AS count, COUNT(career_length) AS count_of_counts
FROM ( 
    SELECT c.contributorid, c.length AS career_length
    FROM career c 
    JOIN contributor c2 ON c.contributorid = c2.id
    WHERE c2.genderid IN (%(gender)s)
    AND c2.stateid IN (%(state)s)
    AND c.roleid IN (%(role)s)
    AND c2.first_year >= %(year0)s
    AND c2.first_year <= %(year1)s
    AND length > 2
) t1
GROUP BY career_length
ORDER BY career_length;
'''

query_list = [role_query, length_query]
bar_list = ['Career roles', 'Career length']

# Create engine
engine = create_engine('mysql://root:sqladmin@localhost:3306/atc_alchemy')
connection = engine.connect()

states = [(1), (2), (3), (4), (5), (6), (1, 2, 3, 4, 5, 6)]
states_dict = pd.read_sql("SELECT * FROM state", connection).set_index('id').state.to_dict()

roles = [(3), (5), (6), (7), (9), (10), (11), (12), (13), (14), (3, 5, 6, 7, 9, 10, 11, 12, 13, 14)]
roles_dict = pd.read_sql("SELECT * FROM role", connection).set_index('id').role.to_dict()

genders = [1, 2, (1, 2)]
genders_dict = pd.read_sql("SELECT * FROM gender", connection).set_index('id').gender.to_dict()

decades = [1950, 1960, 1970, 1980, 1990, 2000, 2010]

output = []

query_params = {'role': 4,
                'gender': 1,
                'state': 4}

x_scale = pd.read_sql("SELECT DISTINCT year0 FROM data_table", connection).year0.tolist()

# TODO: This currently doesn't work for multiple items, but I might be able to fix
#  by inserting tuples into the lists above
i = 0
for gender in genders:
    query_params['gender'] = gender
    for role in roles:
        query_params['role'] = role
        for state in states:
            query_params['state'] = state
            for decade in decades:
                query_params['year0'] = decade
                query_params['year1'] = decade + 9
                decade_str = str(decade)

                if genders_dict[gender]:
                    gender_string = genders_dict[gender]
                else:
                    gender_string = 'Male and Female'

                if states_dict[state]:
                    state_string = states_dict[state]
                else:
                    state_string = 'all states'

                if roles_dict[role]:
                    role_string = roles_dict[role]
                else:
                    role_string = 'in any roles'

                chart_title = f'{decade_str}s {genders_dict[gender]} {roles_dict[role]} from {states_dict[state]}'
                chart_file = f'career_bars {decade_str}_{role}_{state}_{gender}.png'
                fig, axs = plt.subplots(2, sharex=True, sharey=True)
                fig.suptitle(chart_title)

                i = 0

                for bar, query in zip(bar_list, query_list):
                    sub = axs[i]
                    data = pd.read_sql(query, connection, params=query_params)
                    x_vals = data['count']
                    y_vals = data['count_of_counts']
                    sub.set_title(bar)
                    sub.set(ylabel='No. practitioners')
                    sub.bar(x_vals, y_vals, label=bar)

                    i += 1

                plt.show()
                plt.savefig('out/' + chart_file)

                # Clear the figure
                plt.clf()

                print(f'{chart_file} printed!')
