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

generic_query = '''
SELECT c.length, c.count, c.length / c.count AS `role every n years`
FROM career c 
WHERE length > 2
AND count > 2
AND roleid IN (3, 5, 6, 7, 9, 11, 14)
AND first_year >= %(year)s
AND first_year <= (%(year)s + 5)
AND contributorid IN (
	SELECT id
	FROM contributor c
	WHERE stateid NOT IN (9)
)
'''

role_query = '''
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
	AND genderid IN (1,2)
)
'''

gender_query = '''
SELECT c.length, c.count, c.length / c.count AS `role every n years`
FROM career c 
WHERE length > 2
AND count > 2
AND roleid IN (3, 5, 6, 7, 9, 11, 14)
AND first_year >= %(year)s
AND first_year <= (%(year)s + 5)
AND contributorid IN (
	SELECT id
	FROM contributor c
	WHERE stateid NOT IN (9)
	AND genderid IN (%(gender)s)
)
'''

state_query = '''
SELECT c.length, c.count, c.length / c.count AS `role every n years`
FROM career c 
WHERE length > 2
AND count > 2
AND roleid IN (3, 5, 6, 7, 9, 11, 14)
AND first_year >= %(year)s
AND first_year <= (%(year)s + 5)
AND contributorid IN (
	SELECT id
	FROM contributor c
	WHERE stateid IN (%(state)s)
)
'''

single_roletype_query = '''
SELECT c.length, c.count, c.length / c.count AS `role every n years`
FROM career c 
WHERE length > 2
AND count > 2
AND roleid IN (3, 5, 6, 7, 9, 11, 14)
AND first_year >= %(year)s
AND first_year <= (%(year)s + 5)
AND contributorid IN (
	SELECT id
	FROM contributor c
	WHERE stateid NOT IN (9)
)
AND contributorid IN (
	SELECT contributorid
	FROM career c
	GROUP BY contributorid 
	HAVING COUNT(contributorid) = 1
)
'''

multi_roletype_query = '''
SELECT c.length, c.count, c.length / c.count AS `role every n years`
FROM career c 
WHERE length > 2
AND count > 2
AND roleid IN (3, 5, 6, 7, 9, 11, 14)
AND first_year >= %(year)s
AND first_year <= (%(year)s + 5)
AND contributorid IN (
	SELECT id
	FROM contributor c
	WHERE stateid NOT IN (9)
)
AND contributorid IN (
	SELECT contributorid
	FROM career c
	GROUP BY contributorid 
	HAVING COUNT(contributorid) > 1
)
'''

full_multi_career_query = '''
SELECT MAX(last_year) - MIN(first_year) AS length, SUM(count) AS count, 
	    (MAX(last_year) - MIN(first_year)) / SUM(count) AS 'role every n years'
		FROM (
		    SELECT contributorid, first_year, first_year + length AS last_year, length, count 
			FROM career c 
			WHERE length > 2
		    AND count > 2
		    AND roleid IN (3, 5, 6, 7, 9, 11, 14)
		    AND contributorid IN (
				SELECT contributorid
				FROM career c
				WHERE c.length > 2
				AND c.count > 2
				AND roleid IN (3, 5, 6, 7, 9, 11, 14)
				AND contributorid IN (
					SELECT id
					FROM contributor c 
					WHERE first_year >= %(year)s
                    AND first_year <= (%(year)s + 5)
					AND stateid NOT IN (9)
				)
				GROUP BY contributorid
				HAVING COUNT(contributorid) > 1
			)
		) t1
		GROUP BY contributorid 
'''



career_by_contributor_query = '''
SELECT MAX(last_year) - MIN(first_year) AS length, SUM(count) AS count, 
    (MAX(last_year) - MIN(first_year)) / SUM(count) AS 'role every n years'
FROM (
    SELECT contributorid, first_year, first_year + length AS last_year, length, count 
	FROM career c 
	WHERE length > 2
    AND count > 2
    AND roleid IN (3, 5, 6, 7, 9, 11, 14)
    AND first_year >= %(year)s
    AND first_year <= (%(year)s + 5)
) t1
GROUP BY contributorid 
'''

career_by_contributor_generic_query = '''
SELECT MAX(last_year) - MIN(first_year) AS length, SUM(count) AS count, 
    (MAX(last_year) - MIN(first_year)) / SUM(count) AS 'role every n years'
FROM (
    SELECT contributorid, first_year, first_year + length AS last_year, length, count 
	FROM career c 
	WHERE length > 2
    AND count > 2
    AND roleid IN (3, 5, 6, 7, 9, 11, 14)
    AND first_year >= 1950
    AND first_year <= 2005
) t1
GROUP BY contributorid 
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

# LOOP QUERIES HERE
# Roles
for role in roles:
    output = pd.DataFrame(columns=['year',
                                   'len_25%', 'length_med', 'len_75%',
                                   'gigs_25%', 'gigs_med', 'gigs_75%',
                                   'gig_every_n_years_25%', 'gig_every_n_years_med', 'gig_every_n_years_75%'])
    query_params['role'] = role

    for year in years:
        query_params['year'] = year

        data = pd.read_sql(role_query, connection, params=query_params)

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

    fig, axs = plt.subplots(1, 3, figsize=(12,4), sharey=False, constrained_layout=False)

    length.plot.line(ax=axs[0], x='year',color=['red', 'black', 'red'], legend=None)
    axs[0].set_title('length')
    axs[0].set_ylim([0,35])
    gigs.plot.line(ax=axs[1], x='year',color=['red', 'black', 'red'], legend=None)
    axs[1].set_title('gigs')
    axs[1].set_ylim([0,18])
    gig_gap.plot.line(ax=axs[2], x='year',color=['red', 'black', 'red'], legend=None)
    axs[2].set_title('time between gigs')
    axs[2].set_ylim([0,5])
    fig.suptitle(f'Median {roles_dict[role]} career data over time')
    # plt.subplots_adjust(left=0.1,
    #                         bottom=0.1,
    #                         right=0.9,
    #                         top=0.9,
    #                         wspace=0.4,
    #                         hspace=0.4)

    file_number += 1
    plt.savefig('out/' + str(file_number) + roles_dict[role])
    plt.show()

    fig.clf()

sys.exit('roles done')
# Genders
# for gender in genders:
#     query_params['gender'] = gender
#     # create blank dataframe
#     output = pd.DataFrame(columns=['year',
#                                    'len_25%', 'length_med', 'len_75%',
#                                    'gigs_25%', 'gigs_med', 'gigs_75%',
#                                    'gig_every_n_years_25%', 'gig_every_n_years_med', 'gig_every_n_years_75%'])
#     for year in years:
#         query_params['year'] = year
#
#         data = pd.read_sql(gender_query, connection, params=query_params)
#
#         out = [year]
#
#         for col in data:
#             out = out + data[col].describe().tolist()[4:7]
#
#         series = pd.Series(out, index=output.columns)
#
#         output = output.append(series, ignore_index=True)
#
#     print(output)
#
#     x_axis = output['year']
#
#     print(x_axis)
#
#     length = output.iloc[:, np.r_[0, 1:4]]
#
#     gigs = output.iloc[:, np.r_[0, 4:7]]
#
#     gig_gap = output.iloc[:, np.r_[0, 7:10]]
#
#     print(length)
#     print(gigs)
#     print('here is gig_gap')
#     print(gig_gap)
#
#     fig, axs = plt.subplots(1, 3, figsize=(12,4), sharey=False, constrained_layout=False)
#
#     length.plot.line(ax=axs[0], x='year',color=['red', 'black', 'red'], legend=None)
#     axs[0].set_title('length')
#     axs[0].set_ylim([0,35])
#     gigs.plot.line(ax=axs[1], x='year',color=['red', 'black', 'red'], legend=None)
#     axs[1].set_title('gigs')
#     axs[1].set_ylim([0,18])
#     gig_gap.plot.line(ax=axs[2], x='year',color=['red', 'black', 'red'], legend=None)
#     axs[2].set_title('time between gigs')
#     axs[2].set_ylim([0,5])
#     fig.suptitle(f'Median {genders_dict[gender]} career data over time')
#     # plt.subplots_adjust(left=0.1,
#     #                         bottom=0.1,
#     #                         right=0.9,
#     #                         top=0.9,
#     #                         wspace=0.4,
#     #                         hspace=0.4)
#
#     file_number += 1
#     plt.savefig('out/' + str(file_number))
#     plt.show()
#
#     fig.clf()
#
# sys.exit('gender done')
#
# # States
# for state in states:
#     query_params['state'] = state
#     # create blank dataframe
#     output = pd.DataFrame(columns=['year',
#                                    'len_25%', 'length_med', 'len_75%',
#                                    'gigs_25%', 'gigs_med', 'gigs_75%',
#                                    'gig_every_n_years_25%', 'gig_every_n_years_med', 'gig_every_n_years_75%'])
#     for year in years:
#         query_params['year'] = year
#
#         data = pd.read_sql(state_query, connection, params=query_params)
#
#         out = [year]
#
#         for col in data:
#             out = out + data[col].describe().tolist()[4:7]
#
#         series = pd.Series(out, index=output.columns)
#
#         output = output.append(series, ignore_index=True)
#
#     print(output)
#
#     x_axis = output['year']
#
#     print(x_axis)
#
#     length = output.iloc[:, np.r_[0, 1:4]]
#
#     gigs = output.iloc[:, np.r_[0, 4:7]]
#
#     gig_gap = output.iloc[:, np.r_[0, 7:10]]
#
#     print(length)
#     print(gigs)
#     print('here is gig_gap')
#     print(gig_gap)
#
#     fig, axs = plt.subplots(1, 3, figsize=(12,4), sharey=False, constrained_layout=False)
#
#     length.plot.line(ax=axs[0], x='year',color=['red', 'black', 'red'], legend=None)
#     axs[0].set_title('length')
#     axs[0].set_ylim([0,35])
#     gigs.plot.line(ax=axs[1], x='year',color=['red', 'black', 'red'], legend=None)
#     axs[1].set_title('gigs')
#     axs[1].set_ylim([0,18])
#     gig_gap.plot.line(ax=axs[2], x='year',color=['red', 'black', 'red'], legend=None)
#     axs[2].set_title('time between gigs')
#     axs[2].set_ylim([0,5])
#     fig.suptitle(f'Median {states_dict[state]} career data over time')
#     # plt.subplots_adjust(left=0.1,
#     #                         bottom=0.1,
#     #                         right=0.9,
#     #                         top=0.9,
#     #                         wspace=0.4,
#     #                         hspace=0.4)
#
#     file_number += 1
#     plt.savefig('out/' + str(file_number))
#     plt.show()
#
#     fig.clf()


# Single only
output = pd.DataFrame(columns=['year',
                               'len_25%', 'length_med', 'len_75%',
                               'gigs_25%', 'gigs_med', 'gigs_75%',
                               'gig_every_n_years_25%', 'gig_every_n_years_med', 'gig_every_n_years_75%'])
for year in years:
    query_params['year'] = year

    data = pd.read_sql(single_roletype_query, connection, params=query_params)

    out = [year]

    for col in data:
        out = out + data[col].describe().tolist()[4:7]

    series = pd.Series(out, index=output.columns)

    output = output.append(series, ignore_index=True)

x_axis = output['year']

length = output.iloc[:, np.r_[0, 1:4]]

gigs = output.iloc[:, np.r_[0, 4:7]]

gig_gap = output.iloc[:, np.r_[0, 7:10]]

fig, axs = plt.subplots(1, 3, figsize=(12,4), sharey=False, constrained_layout=False)

length.plot.line(ax=axs[0], x='year',color=['red', 'black', 'red'], legend=None)
axs[0].set_title('length')
axs[0].set_ylim([0,35])
gigs.plot.line(ax=axs[1], x='year',color=['red', 'black', 'red'], legend=None)
axs[1].set_title('gigs')
axs[1].set_ylim([0,18])
gig_gap.plot.line(ax=axs[2], x='year',color=['red', 'black', 'red'], legend=None)
axs[2].set_title('time between gigs')
axs[2].set_ylim([0,5])
fig.suptitle(f'Median career data (single-role practioners only) over time')
# plt.subplots_adjust(left=0.1,
#                         bottom=0.1,
#                         right=0.9,
#                         top=0.9,
#                         wspace=0.4,
#                         hspace=0.4)

file_number += 1
plt.savefig('out/' + str(file_number))
plt.show()

fig.clf()


# Multi only single careers
output = pd.DataFrame(columns=['year',
                               'len_25%', 'length_med', 'len_75%',
                               'gigs_25%', 'gigs_med', 'gigs_75%',
                               'gig_every_n_years_25%', 'gig_every_n_years_med', 'gig_every_n_years_75%'])
for year in years:
    query_params['year'] = year

    data = pd.read_sql(multi_roletype_query, connection, params=query_params)

    out = [year]

    for col in data:
        out = out + data[col].describe().tolist()[4:7]

    series = pd.Series(out, index=output.columns)

    output = output.append(series, ignore_index=True)

x_axis = output['year']

length = output.iloc[:, np.r_[0, 1:4]]

gigs = output.iloc[:, np.r_[0, 4:7]]

gig_gap = output.iloc[:, np.r_[0, 7:10]]

fig, axs = plt.subplots(1, 3, figsize=(12,4), sharey=False, constrained_layout=False)

length.plot.line(ax=axs[0], x='year',color=['red', 'black', 'red'], legend=None)
axs[0].set_title('length')
axs[0].set_ylim([0,35])
gigs.plot.line(ax=axs[1], x='year',color=['red', 'black', 'red'], legend=None)
axs[1].set_title('gigs')
axs[1].set_ylim([0,18])
gig_gap.plot.line(ax=axs[2], x='year',color=['red', 'black', 'red'], legend=None)
axs[2].set_title('time between gigs')
axs[2].set_ylim([0,5])
fig.suptitle(f'Median single career lengths (multi-role practioners) over time')
# plt.subplots_adjust(left=0.1,
#                         bottom=0.1,
#                         right=0.9,
#                         top=0.9,
#                         wspace=0.4,
#                         hspace=0.4)

file_number += 1
plt.savefig('out/' + str(file_number))
plt.show()

fig.clf()


# Multi-role combined careers
output = pd.DataFrame(columns=['year',
                               'len_25%', 'length_med', 'len_75%',
                               'gigs_25%', 'gigs_med', 'gigs_75%',
                               'gig_every_n_years_25%', 'gig_every_n_years_med', 'gig_every_n_years_75%'])
for year in years:
    query_params['year'] = year

    data = pd.read_sql(full_multi_career_query, connection, params=query_params)

    out = [year]

    for col in data:
        out = out + data[col].describe().tolist()[4:7]

    series = pd.Series(out, index=output.columns)

    output = output.append(series, ignore_index=True)

x_axis = output['year']

length = output.iloc[:, np.r_[0, 1:4]]

gigs = output.iloc[:, np.r_[0, 4:7]]

gig_gap = output.iloc[:, np.r_[0, 7:10]]

fig, axs = plt.subplots(1, 3, figsize=(12,4), sharey=False, constrained_layout=False)

length.plot.line(ax=axs[0], x='year',color=['red', 'black', 'red'], legend=None)
axs[0].set_title('length')
axs[0].set_ylim([0,40])
gigs.plot.line(ax=axs[1], x='year',color=['red', 'black', 'red'], legend=None)
axs[1].set_title('gigs')
axs[1].set_ylim([0,40])
gig_gap.plot.line(ax=axs[2], x='year',color=['red', 'black', 'red'], legend=None)
axs[2].set_title('time between gigs')
axs[2].set_ylim([0,5])
fig.suptitle(f'Median combined multi-role career data over time')
# plt.subplots_adjust(left=0.1,
#                         bottom=0.1,
#                         right=0.9,
#                         top=0.9,
#                         wspace=0.4,
#                         hspace=0.4)

file_number += 1
plt.savefig('out/' + str(file_number))
plt.show()

fig.clf()

# All compiled careers
output = pd.DataFrame(columns=['year',
                               'len_25%', 'length_med', 'len_75%',
                               'gigs_25%', 'gigs_med', 'gigs_75%',
                               'gig_every_n_years_25%', 'gig_every_n_years_med', 'gig_every_n_years_75%'])
for year in years:
    query_params['year'] = year

    data = pd.read_sql(career_by_contributor_query, connection, params=query_params)

    out = [year]

    for col in data:
        out = out + data[col].describe().tolist()[4:7]

    series = pd.Series(out, index=output.columns)

    output = output.append(series, ignore_index=True)

x_axis = output['year']

length = output.iloc[:, np.r_[0, 1:4]]

gigs = output.iloc[:, np.r_[0, 4:7]]

gig_gap = output.iloc[:, np.r_[0, 7:10]]

fig, axs = plt.subplots(1, 3, figsize=(12,4), sharey=False, constrained_layout=False)

length.plot.line(ax=axs[0], x='year',color=['red', 'black', 'red'], legend=None)
axs[0].set_title('length')
axs[0].set_ylim([0,35])
gigs.plot.line(ax=axs[1], x='year',color=['red', 'black', 'red'], legend=None)
axs[1].set_title('gigs')
axs[1].set_ylim([0,18])
gig_gap.plot.line(ax=axs[2], x='year',color=['red', 'black', 'red'], legend=None)
axs[2].set_title('time between gigs')
axs[2].set_ylim([0,5])
fig.suptitle(f'Median career data by single contributor over time')
# plt.subplots_adjust(left=0.1,
#                         bottom=0.1,
#                         right=0.9,
#                         top=0.9,
#                         wspace=0.4,
#                         hspace=0.4)

file_number += 1
plt.savefig('out/' + str(file_number))
plt.show()

fig.clf()


# Definitive data per person.
data = pd.read_sql(career_by_contributor_generic_query, connection, params=query_params)

print('This should be definitive')
print(data.describe())



no_filter_query = '''
SELECT MAX(last_year) - MIN(first_year) AS length, SUM(count) AS count, 
    (MAX(last_year) - MIN(first_year)) / SUM(count) AS 'role every n years'
FROM (
    SELECT contributorid, first_year, first_year + length AS last_year, length, count 
	FROM career c 
    WHERE first_year >= %(year)s
    AND first_year <= (%(year)s + 5)
) t1
GROUP BY contributorid 
'''

no_filter_generic_query = '''
SELECT MAX(last_year) - MIN(first_year) AS length, SUM(count) AS count, 
    (MAX(last_year) - MIN(first_year)) / SUM(count) AS 'role every n years'
FROM (
    SELECT contributorid, first_year, first_year + length AS last_year, length, count 
	FROM career c 
    WHERE first_year >= 1950
    AND first_year <= 2005
) t1
GROUP BY contributorid 
'''


# No Filter
output = pd.DataFrame(columns=['year',
                               'len_25%', 'length_med', 'len_75%',
                               'gigs_25%', 'gigs_med', 'gigs_75%',
                               'gig_every_n_years_25%', 'gig_every_n_years_med', 'gig_every_n_years_75%'])
for year in years:
    query_params['year'] = year

    data = pd.read_sql(no_filter_query, connection, params=query_params)

    out = [year]

    for col in data:
        out = out + data[col].describe().tolist()[4:7]

    series = pd.Series(out, index=output.columns)

    output = output.append(series, ignore_index=True)

x_axis = output['year']

length = output.iloc[:, np.r_[0, 1:4]]

gigs = output.iloc[:, np.r_[0, 4:7]]

gig_gap = output.iloc[:, np.r_[0, 7:10]]

fig, axs = plt.subplots(1, 3, figsize=(12,4), sharey=False, constrained_layout=False)

length.plot.line(ax=axs[0], x='year',color=['red', 'black', 'red'], legend=None)
axs[0].set_title('length')
axs[0].set_ylim([0,35])
gigs.plot.line(ax=axs[1], x='year',color=['red', 'black', 'red'], legend=None)
axs[1].set_title('gigs')
axs[1].set_ylim([0,18])
gig_gap.plot.line(ax=axs[2], x='year',color=['red', 'black', 'red'], legend=None)
axs[2].set_title('time between gigs')
axs[2].set_ylim([0,5])
fig.suptitle(f'Median career data (no filter)')
# plt.subplots_adjust(left=0.1,
#                         bottom=0.1,
#                         right=0.9,
#                         top=0.9,
#                         wspace=0.4,
#                         hspace=0.4)

file_number += 1
plt.savefig('out/' + str(file_number))
plt.show()

fig.clf()


# Definitive data per person.
data = pd.read_sql(no_filter_generic_query, connection, params=query_params)

print('Definitive no filter')
print(data.describe())

# Single role type generic
generic_generic_query = '''
SELECT c.length, c.count, c.length / c.count AS `role every n years`
FROM career c 
WHERE length > 2
AND count > 2
AND roleid IN (3, 5, 6, 7, 9, 11, 14)
AND first_year >= 1950
AND first_year <= 2005
AND contributorid IN (
	SELECT id
	FROM contributor c
	WHERE stateid NOT IN (9)
)
'''

# General
output = pd.DataFrame(columns=['year',
                               'len_25%', 'length_med', 'len_75%',
                               'gigs_25%', 'gigs_med', 'gigs_75%',
                               'gig_every_n_years_25%', 'gig_every_n_years_med', 'gig_every_n_years_75%'])
for year in years:
    query_params['year'] = year

    data = pd.read_sql(generic_query, connection, params=query_params)

    out = [year]

    for col in data:
        out = out + data[col].describe().tolist()[4:7]

    series = pd.Series(out, index=output.columns)

    output = output.append(series, ignore_index=True)

x_axis = output['year']

length = output.iloc[:, np.r_[0, 1:4]]

gigs = output.iloc[:, np.r_[0, 4:7]]

gig_gap = output.iloc[:, np.r_[0, 7:10]]

fig, axs = plt.subplots(1, 3, figsize=(12,4), sharey=False, constrained_layout=False)

length.plot.line(ax=axs[0], x='year',color=['red', 'black', 'red'], legend=None)
axs[0].set_title('length')
axs[0].set_ylim([0,35])
gigs.plot.line(ax=axs[1], x='year',color=['red', 'black', 'red'], legend=None)
axs[1].set_title('gigs')
axs[1].set_ylim([0,18])
gig_gap.plot.line(ax=axs[2], x='year',color=['red', 'black', 'red'], legend=None)
axs[2].set_title('time between gigs')
axs[2].set_ylim([0,5])
fig.suptitle(f'Median career data in any single role')
# plt.subplots_adjust(left=0.1,
#                         bottom=0.1,
#                         right=0.9,
#                         top=0.9,
#                         wspace=0.4,
#                         hspace=0.4)

file_number += 1
plt.savefig('out/' + str(file_number))
plt.show()

fig.clf()

# Definitive data per person.
data = pd.read_sql(generic_generic_query, connection, params=query_params)

print('Definitive any single career')
print(data.describe())


multi_roletype_generic_query = '''
SELECT MAX(last_year) - MIN(first_year) AS length, SUM(count) AS count, 
	    (MAX(last_year) - MIN(first_year)) / SUM(count) AS 'role every n years'
		FROM (
		    SELECT contributorid, first_year, first_year + length AS last_year, length, count 
			FROM career c 
			WHERE length > 2
		    AND count > 2
		    AND roleid IN (3, 5, 6, 7, 9, 11, 14)
		    AND contributorid IN (
				SELECT contributorid
				FROM career c
				WHERE c.length > 2
				AND c.count > 2
				AND roleid IN (3, 5, 6, 7, 9, 11, 14)
				AND contributorid IN (
					SELECT id
					FROM contributor c 
					WHERE first_year >= 1950
					AND first_year <= 2005
					AND stateid NOT IN (9)
				)
				GROUP BY contributorid
				HAVING COUNT(contributorid) > 1
			)
		) t1
		GROUP BY contributorid 
'''

# Definitive data per person.
data = pd.read_sql(multi_roletype_generic_query, connection, params=query_params)

print('Definitive any multi-career')
print(data.describe())

