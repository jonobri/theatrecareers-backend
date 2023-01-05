import pandas as pd
from sqlalchemy import create_engine

# Create engine
engine = create_engine('mysql://root:sqladmin@localhost:3306/atc_alchemy')
connection = engine.connect()
connection.execute("SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));")

query = '''
SELECT year, COUNT(event_name) AS show_count
FROM event e 
GROUP BY year
ORDER BY year ASC
'''

data = pd.read_sql(query, connection)

showCountAnnual = data