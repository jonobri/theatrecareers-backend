import sys
import warnings
from datetime import datetime
import timeit

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec

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

role_length_query = '''
SELECT career_length AS count, COUNT(career_length) AS count_of_counts
FROM ( 
    SELECT c.contributorid, c.length AS career_length
    FROM career c 
    JOIN contributor c2 ON c.contributorid = c2.id
    WHERE c.roleid IN (%(role)s)
    AND c2.genderid IN (1, 2)
    AND c2.stateid IN (1, 2, 3, 4, 5, 6)
    AND c.first_year >= %(year0)s
    AND c.first_year <= %(year1)s
    AND length > 2
) t1
GROUP BY career_length
ORDER BY career_length;
'''

all_length_query = '''
SELECT career_length AS count, COUNT(career_length) AS count_of_counts
FROM ( 
    SELECT c.contributorid, c.length AS career_length
    FROM career c 
    JOIN contributor c2 ON c.contributorid = c2.id
    WHERE c.roleid IN (3, 5, 6, 7, 9, 10, 11, 12, 13, 14)
    AND c2.genderid IN (1, 2)
    AND c2.stateid IN (1, 2, 3, 4, 5, 6)
    AND c.first_year >= %(year0)s
    AND c.first_year <= %(year1)s
    AND length > 2
) t1
GROUP BY career_length
ORDER BY career_length;
'''

length_query = '''
SELECT career_length AS count, COUNT(career_length) AS count_of_counts
FROM ( 
    SELECT c.contributorid, c.length AS career_length
    FROM career c 
    JOIN contributor c2 ON c.contributorid = c2.id
    WHERE c2.genderid IN (1, 2)
    AND c2.stateid IN (1, 2, 3, 4, 5, 6)
    AND c.roleid IN (%(role)s)
    AND c2.first_year >= %(year0)s
    AND c2.first_year <= %(year1)s
    AND length > 2
) t1
GROUP BY career_length
ORDER BY career_length;
'''

# query_list = [role_query, length_query]
bar_list = ['Career roles', 'Career length']

# Create engine
engine = create_engine('mysql://root:sqladmin@localhost:3306/atc_alchemy')
connection = engine.connect()

states = [(1), (2), (3), (4), (5), (6), (1, 2, 3, 4, 5, 6)]
states_dict = pd.read_sql("SELECT * FROM state", connection).set_index('id').state.to_dict()

roles = [(3), (5), (6), (7), (9), (10), (11), (13), (14)]
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
for decade in decades:
    query_params['year0'] = decade
    query_params['year1'] = decade + 9
    decade_str = str(decade)

    # if genders_dict[gender]:
    #     gender_string = genders_dict[gender]
    # else:
    #     gender_string = 'Male and Female'
    #
    # if states_dict[state]:
    #     state_string = states_dict[state]
    # else:
    #     state_string = 'all states'

    figure = plt.figure(figsize=(10, 12), dpi=300)
    # grid = gridspec.GridSpec(6, 3)

    all_plot = plt.subplot2grid((6, 3), (0, 0), rowspan=3, colspan=3, fig=figure)

    all_data = pd.read_sql(all_length_query, connection, params=query_params)
    x_vals = all_data['count']
    y_vals = all_data['count_of_counts']

    all_plot.set_title(f'{decade_str}s Career Length count')

    all_plot.set(ylabel='No. practitioners')

    all_plot.bar(x_vals, y_vals, label='Career length')

    # Plot all roles here

    n = 0
    xpoint = 3
    ypoint = 0

    roleplots = list(range(10))

    for role in roles:
        query_params['role'] = role
        if roles_dict[role]:
            role_string = roles_dict[role]
        else:
            role_string = 'in any role'

        roleplots[n] = plt.subplot2grid((6, 3), (xpoint, ypoint), fig=figure)

        roleplots_title = f'{decade_str}s {roles_dict[role]}'

        roleplots[n].set_title(roleplots_title)

        data = pd.read_sql(role_length_query, connection, params=query_params)
        x_vals = data['count']
        y_vals = data['count_of_counts']

        roleplots[n].set(ylabel='No. practitioners')

        roleplots[n].bar(x_vals, y_vals, label='Career length')

        n += 1

        if xpoint == 5:
            ypoint += 1

        if xpoint < 5:
            xpoint += 1
        else:
            xpoint = 3

    # plt.tight_layout()
    # set the spacing between subplots
    plt.subplots_adjust(left=0.1,
                        bottom=0.1,
                        right=0.9,
                        top=0.9,
                        wspace=0.4,
                        hspace=0.4)

    plt.savefig('out/' + f'{decade_str}s_career_length.png')
    plt.show()







