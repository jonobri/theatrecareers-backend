import sys
import warnings
from datetime import datetime
import timeit

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec

from sqlalchemy import create_engine, update, text, Integer, Table

role_count_query = '''
SELECT role_count AS count, COUNT(role_count) AS count_of_counts
FROM ( 
    SELECT c.contributorid, c.count AS role_count
    FROM career c 
    JOIN contributor c2 ON c.contributorid = c2.id
    WHERE c.roleid IN (%(role)s)
    AND c2.genderid IN (1, 2)
    AND c2.stateid IN (1, 2, 3, 4, 5, 6)
    AND c2.first_year >= %(year0)s
    AND c2.first_year <= %(year1)s
    AND c.count > 2
) t1
GROUP BY role_count
ORDER BY role_count;
'''

all_count_query = '''
SELECT role_count AS count, COUNT(role_count) AS count_of_counts
FROM ( 
    SELECT c.contributorid, c.count AS role_count
    FROM career c 
    JOIN contributor c2 ON c.contributorid = c2.id
    WHERE c.roleid IN (3, 5, 6, 7, 9, 10, 11, 12, 13, 14)
    AND c2.genderid IN (1, 2)
    AND c2.stateid IN (1, 2, 3, 4, 5, 6)
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

roles = [(3), (5), (6), (7), (9), (11), (14)]
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

    all_data = pd.read_sql(all_count_query, connection, params=query_params)
    x_vals = all_data['count']
    y_vals = all_data['count_of_counts']

    all_plot.set_title(f'{decade_str}s Role count')

    all_plot.set(ylabel='No. practitioners')
    all_plot.set_xlim(0,100)

    line_total = sum(all_data['count_of_counts'])
    line_vals = []

    for val in y_vals:
        line_val = (val / line_total + sum(line_vals)) * 100
        line_vals.append(line_val)
    
    # print(line_vals)
    # sys.exit('Printed.')

    # all_plot.plot(x_vals, line_vals)

    all_plot.bar(x_vals, y_vals, label='Role count')



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

        data = pd.read_sql(role_count_query, connection, params=query_params)
        x_vals = data['count']
        y_vals = data['count_of_counts']

        roleplots[n].set(ylabel='No. practitioners')
        roleplots[n].set_xlim(0, 100)

        roleplots[n].bar(x_vals, y_vals, label='Role count')

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

    plt.savefig(f'{decade_str}s_role_count.png')
    plt.show()







