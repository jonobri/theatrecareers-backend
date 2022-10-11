import pandas as pd
from sqlalchemy import create_engine, update, text, Integer, Table
import matplotlib.pyplot as plt

# Create engine
engine = create_engine('mysql://root:sqladmin@localhost:3306/atc_alchemy')
connection = engine.connect()

query = '''
SELECT COUNT(*) AS count, year0, year1, year2, year3, year4, year5, year6, year7, year8, year9, year10, year11,
    year12, year13, year14, year15, year16, year17, year18, year19, year20, year21, year22, year23, year24
FROM career c
WHERE length >= 3
AND count >= 3
AND roleid IN (3, 5, 6, 7, 9, 11, 14)
AND first_year >= 1950
AND first_year <= 2005
AND contributorid IN (
	SELECT id
	FROM contributor c
	WHERE stateid NOT IN (9)
	AND genderid IN (1)
)
GROUP BY year0, year1, year2, year3, year4, year5, year6, year7, year8, year9, year10, year11,
    year12, year13, year14, year15, year16, year17, year18, year19, year20, year21, year22, year23, year24
'''

