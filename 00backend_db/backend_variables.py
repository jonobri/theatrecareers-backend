import pandas as pd
from sqlalchemy import create_engine, text

import backend_queries as bq

# Create engines
atc_engine = create_engine('mysql://root:sqladmin@localhost:3306/atc_alchemy')
as_engine = create_engine('mysql://root:sqladmin@localhost:3306/ausstage')

# SET DATABASE CONSTRAINTS
FIRST_YEAR = 1950
LAST_YEAR = 2019
YEAR_TUPLE = (FIRST_YEAR, LAST_YEAR)

# LIST OF DICTS FOR TABLE-CREATION LOOP
TABLE_DICT_LIST = [
    {
        'table_name': 'gender',
        'query': bq.gender_query,
        'engine': as_engine,
        'sql_input': False
    },
    {
        'table_name': 'state',
        'query': bq.state_query,
        'engine': as_engine,
        'sql_input': False
    },
    {
        'table_name': 'function_list',
        'query': bq.function_list_query,
        'engine': as_engine,
        'sql_input': False
    },
    {
        'table_name': 'role',
        'query': bq.role_query,
        'engine': atc_engine,
        'sql_input': False
    },
    {
        'table_name': 'event',
        'query': bq.event_query,
        'engine': as_engine,
        'sql_input': YEAR_TUPLE
    },
    {
        'table_name': 'contributor',
        'query': bq.contributor_query,
        'engine': as_engine,
        'sql_input': False
    },
    {
        'table_name': 'link_table_old',
        'query': bq.link_table_old_query,
        'engine': as_engine,
        'sql_input': False
    },
    {
        'table_name': 'link_table',
        'query': bq.link_table_query,
        'engine': as_engine,
        'sql_input': False
    },
    {
        'table_name': 'career',
        'query': bq.career_query,
        'engine': atc_engine,
        'sql_input': False
    },
]

# Role Filter Categories
LINK_ROLE_STATEMENTS = ['''
UPDATE link_table
    SET roleid = 1
    WHERE functionid IN (6,65,66,68,71,72,104,122,337,338,339,343,360,375,468,497,5);
''',
                        '''
UPDATE link_table
    SET roleid = 2
    WHERE functionid IN (59,89,94,98,120,140,315,370,379);
''',
                        '''
UPDATE link_table
    SET roleid = 3
    WHERE functionid IN (47,48,49,86,87,340,342,359,463,473,488,490,492);
''',
                        '''
UPDATE link_table
    SET roleid = 4
    WHERE functionid IN (25,53,54,55);
''',
                        '''
UPDATE link_table
    SET roleid = 5
    WHERE functionid IN (13,18,21,56,57,107,108,113,118,125,126,127,128,149,311,357,367,371,388,466,471,483,487,489,
    491,501,502);
''',
                        '''
UPDATE link_table
    SET roleid = 6
    WHERE functionid IN (9,10,11,12,14,16,19,20,22,27,32,44,50,60,63,64,69,96,103,123,150,313,314,347,366,465,472);
''',
                        '''
UPDATE link_table
    SET roleid = 7
    WHERE functionid IN (15,81,82,372,467);
''',
                        '''
UPDATE link_table
    SET roleid = 8
    WHERE functionid IN (1,26,30,31,33,34,43,95,97,99,101,102,121,129,130,152,312,317,318,319,320,321,322,324,325,326,
    327,328,329,330,331,332,333,334,335,336,349,350,351,352,353,354,362,380,381,382,389,390,391,392,393,394,395,436,
    449,450,459,460,464,503);
''',
                        '''
UPDATE link_table
    SET roleid = 9
    WHERE functionid IN (2,8,36,37,38,41,62,70,74,75,76,77,78,84,85,88,90,91,100,105,106,107,111,119,134,138,139,146,
    153,341,348,373,387,398,399,401,402,403,404,405,406,407,408,409,410,411,412,413,414,415,416,417,418,419,420,421,
    422,423,424,425,426,427,428,429,430,431,432,433,434,435,440,441,442,443,444,445,446,448,453,456,457,458,462,469,
    475,476,477,478);
''',
                        '''
UPDATE link_table
    SET roleid = 10
    WHERE functionid IN (3,7,17,39,40,45,46,67,112,345,374,452,484);
''',
                        '''
UPDATE link_table
    SET roleid = 11
    WHERE functionid IN (42,61,151,132,133,385,485);
''',
                        '''
UPDATE link_table
    SET roleid = 12
    WHERE functionid IN (114,115,116,136,137,141,142,316,346,358,376,377,378,383,396,438,454,455,461,474,479,480,481,
    482,486,493,494,495,496,498,499,500);
''',
                        '''
UPDATE link_table
    SET roleid = 13
    WHERE functionid IN (23,35,147,148,361,365,386,437,470);
''',
                        '''
UPDATE link_table
    SET roleid = 14
    WHERE functionid IN (4,24,28,29,51,52,58,64,73,79,80,83,92,93,109,110,124,131,143,155,369,384,397,400,451);
''',
                        '''
UPDATE link_table
    SET roleid = 15
    WHERE functionid IN (0,144,145,344,356,363,364);
''']
