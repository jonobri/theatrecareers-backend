import sys
import warnings
from datetime import datetime
import timeit

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec

from sqlalchemy import create_engine, update, text, Integer, Table

# Create engine
engine = create_engine('mysql://root:sqladmin@localhost:3306/atc_alchemy')
connection = engine.connect()

connection.execute("SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));")

gender_role_query = '''
SELECT c.length, c.count, c.length / c.count AS `role every n years`
FROM career c 
WHERE length > 2
AND count > 2
AND roleid IN (%(role)s)
AND first_year >= %(year)s
AND first_year <= (%(year)s + 5)
AND contributorid IN (
	SELECT id
	FROM contributor c
	WHERE stateid NOT IN (9)
	AND genderid IN (%(gender)s)
)
'''

years = range(1950, 2001, 5)
roles = [(3), (5), (6), (7), (9), (11), (14)]
genders = [1, 2]
states = [1, 2, 3, 4, 5, 6]
file_number = 0

states_dict = pd.read_sql("SELECT * FROM state", connection).set_index('id').state.to_dict()
genders_dict = pd.read_sql("SELECT * FROM gender", connection).set_index('id').gender.to_dict()
roles_dict = pd.read_sql("SELECT * FROM role", connection).set_index('id').role.to_dict()


query_params = {}

output = pd.DataFrame(columns=['year',
                               'len_25%', 'length_med', 'len_75%',
                               'gigs_25%', 'gigs_med', 'gigs_75%',
                               'gig_every_n_years_25%', 'gig_every_n_years_med', 'gig_every_n_years_75%'])

for role in roles:
    query_params['role'] = role
    for gender in genders:
        query_params['gender'] = gender
        # create blank dataframe
        output = pd.DataFrame(columns=['year',
                                       'len_25%', 'length_med', 'len_75%',
                                       'gigs_25%', 'gigs_med', 'gigs_75%',
                                       'gig_every_n_years_25%', 'gig_every_n_years_med', 'gig_every_n_years_75%'])
        for year in years:
            query_params['year'] = year

            data = pd.read_sql(gender_role_query, connection, params=query_params)

            out = [year]

            for col in data:
                out = out + data[col].describe().tolist()[4:7]

            series = pd.Series(out, index=output.columns)

            output = output.append(series, ignore_index=True)

        print(output)

        x_axis = output['year']

        print(x_axis)

        length = output.iloc[:, np.r_[0, 1:4]]

        gigs = output.iloc[:, np.r_[0, 4:7]]

        gig_gap = output.iloc[:, np.r_[0, 7:10]]

        print(length)
        print(gigs)
        print('here is gig_gap')
        print(gig_gap)

        fig, axs = plt.subplots(1, 3, figsize=(12, 4), sharey=False, constrained_layout=False)

        length.plot.line(ax=axs[0], x='year', color=['red', 'black', 'red'], legend=None)
        axs[0].set_title('length')
        axs[0].set_ylim([0, 35])
        gigs.plot.line(ax=axs[1], x='year', color=['red', 'black', 'red'], legend=None)
        axs[1].set_title('gigs')
        axs[1].set_ylim([0, 18])
        gig_gap.plot.line(ax=axs[2], x='year', color=['red', 'black', 'red'], legend=None)
        axs[2].set_title('time between gigs')
        axs[2].set_ylim([0, 5])
        fig.suptitle(f'Median {genders_dict[gender]} {roles_dict[role]} career data over time')
        # plt.subplots_adjust(left=0.1,
        #                         bottom=0.1,
        #                         right=0.9,
        #                         top=0.9,
        #                         wspace=0.4,
        #                         hspace=0.4)

        file_number += 1
        plt.savefig('out/' + str(file_number) + genders_dict[gender] + ' ' + roles_dict[role])
        plt.show()

        fig.clf()

sys.exit('gender done')
