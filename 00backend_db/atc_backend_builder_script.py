import sys
import warnings
from datetime import datetime
import timeit

import pandas as pd
import numpy as np

from sqlalchemy import create_engine, update, text, Integer, Table

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

if __name__ == '__main__':
    print('Initialising...')
    start = timeit.default_timer()
    now = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    # Create connection for writing to ATC
    atc_connection = atc_engine.connect()
    as_connection = as_engine.connect()

    # Turn off FULL_GROUP_BY
    atc_connection.execute("SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));")
    print('SQL sessions configured')

    # Drop all tables if they exist
    Base.metadata.drop_all(atc_engine)
    print('Tables dropped')

    # Create tables
    Base.metadata.create_all(atc_engine)
    print('Empty tables created')

    # Get table, write table in loop, with exceptions for special cases
    print('Writing tables...')
    for t in TABLE_DICT_LIST:
        # Write raw role values
        if t['table_name'] == 'role':
            atc_connection.execute(t['query'])

        # Write link_table
        elif t['table_name'] == 'link_table':
            table = query_to_df(t['query'], t['engine'], t['sql_input'])
            table.to_sql(t['table_name'], con=atc_connection, if_exists='append', index=False)

            # Update role codes according to function
            # link_role_update(atc_connection, LINK_ROLE_STATEMENTS)

            for s in LINK_ROLE_STATEMENTS:
                atc_connection.execute(text(s))

        elif t['table_name'] == 'link_table_old':
            table = query_to_df(t['query'], t['engine'], t['sql_input'])
            table.to_sql(t['table_name'], con=atc_connection, if_exists='append', index=False)

            # TODO: Get roles for this old table, preferably in a smooth way
            #  (SQLAlchemy doesn't let you pass table names to text as params; it reads it as a string)
            #  In all fairness, this is broadly happening because I'm not using SQLAlchemy fully and correctly

        elif t['table_name'] == 'career':
            table = query_to_df(t['query'], t['engine'], t['sql_input'])

            # Run ancillary function and write
            table = create_career_table(table, t['engine'])
            table.to_sql(t['table_name'], con=atc_connection, if_exists='replace', dtype=Integer, index=False)

            # Alter the table to include foreign keys
            atc_connection.execute(text(bq.career_config), )

        # Else do standard table creation
        else:
            table = query_to_df(t['query'], t['engine'], t['sql_input'])
            table.to_sql(t['table_name'], con=atc_connection, if_exists='append', index=False)

        print(f"{t['table_name']} table written!")

    # Additional data update queries
    print('Writing additional data...')
    # Get event.contributor_count
    atc_connection.execute(text(bq.contributor_count_query))

    # Get contributor.first_year, last_year columns
    atc_connection.execute(text(bq.first_last_year_query))

    print("Additional data written!")








