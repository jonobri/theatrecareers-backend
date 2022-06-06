import sys
import warnings
from datetime import datetime
import timeit

import pandas as pd
import numpy as np

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

# Variables for looping
decades = [1950, 1960, 1970, 1980, 1990, 2000, 2010]

states = [1, 2, 3, 4, 5, 6, 7, 8]

roles = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]

genders = [1, 2]

# Create engine
engine = create_engine('mysql://root:sqladmin@localhost:3306/atc_alchemy')
connection = engine.connect()

query_params = {}

output = []

for decade in decades:
    query_params['year0'] = decade
    query_params['year1'] = decade + 9
    for state in states:
        query_params['state'] = state
        for role in roles:
            query_params['role'] = role
            for gender in genders:
                query_params['gender'] = gender

                # Get new row of datatop
                new_row = get_data_22.get_all_data(engine, query_params)
                # The | operator concatenates dicts
                all_data = query_params | new_row
                print(all_data)
                output.append(all_data)

print(output)
output_DF = pd.DataFrame(output)


print(output_DF)

output_DF.to_csv(f'new_table {datetime.now()}.csv')

# Note: currently manually generating this table from CSV import.
#  I do not recommend this moving forward.
