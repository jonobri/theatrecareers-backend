import pandas as pd

from sqlalchemy import create_engine, text
from sqlalchemy.types import Integer
from sqlalchemy.orm import sessionmaker


# Query with optional input param
def query_to_df(query, engine, sql_input=False):
    # create connection using with statement to close automatically
    with engine.connect() as connection:
        # check for params, run accordingly
        # Turn off FULL_GROUP_BY
        # connection.execute("SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));")
        if sql_input:
            output = pd.read_sql(query, connection, params=sql_input)
        else:
            output = pd.read_sql(query, connection)

    return output


# Create career table
def create_career_table(table, engine):
    # Pivot the dataframe, create multi-index, and then transpose so years are rows, then reset the index
    table = table.pivot_table(index=['contributorid', 'roleid'],
                    columns='year', values='count', fill_value=0).T.reset_index().drop(columns='year')

    # Get the first year of that role.

    # Get the position of the first non-zero value in each column using ne(0).idxmax()
    # This selects the first year in all careers (columns), places it in a series of ints
    first_year = pd.Series(table.ne(0).idxmax()).astype(int)

    # Then, we normalise the data, shifting columns up until every career's first year is in the first row
    # TODO: This is extraordinarily slow and I know it can be done better, but probably not with pandas
    #  Since it's a one-off operation, I'll let it be slow for now
    for col in table.columns:
        table[col] = table[col].shift((first_year[col] * -1), fill_value=0)

    # Next, we get career show count by summing the columns
    show_count = pd.Series(table.sum()).astype(int)

    # Next, we get career length by finding the last non-zero value in each column
    last_year = []

    for col in table.columns:
        last_year.append(table[col].index[table[col].to_numpy().nonzero()[0][-1]] + 1)

    # Transpose the dataframe to tabular form
    table = table.T

    # Add the new columns
    table['length'] = last_year
    table['count'] = show_count

    # Reorder the columns
    cols = table.columns.tolist()
    cols = cols[-2:] + cols[:-2]
    table = table[cols]

    # Transpose again, so years are columns, and rename to make that clear
    # We're resetting the index twice here so we can write the index to MySQL using datatype Int instead of Bigint
    # There may be a more elegant way to do this, but it's not worth figuring out at this time
    table = table.add_prefix('year').reset_index().reset_index().rename(
        columns={'id': 'contributorid', 'index': 'id', 'yearlength': 'length', 'yearcount': 'count'})

    return table


