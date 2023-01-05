career_by_contributor = '''
SELECT MAX(last_year) - MIN(first_year) AS length, SUM(count) AS count, 
    (MAX(last_year) - MIN(first_year)) / SUM(count) AS 'role every n years'
FROM (
    SELECT contributorid, first_year, first_year + length AS last_year, length, count 
	FROM career c 
	WHERE length > 2
    AND count > 2
    AND roleid IN (%(role)s)
    AND contributorid IN (
        SELECT id
        FROM contributor c 
        WHERE first_year >= %(year)s
        AND first_year <= %(year)s + 4
        AND stateid NOT IN (9)
    )
) t1
GROUP BY contributorid 
'''

career_by_gender = '''
SELECT MAX(last_year) - MIN(first_year) AS length, SUM(count) AS count, 
    (MAX(last_year) - MIN(first_year)) / SUM(count) AS 'role every n years'
FROM (
    SELECT contributorid, first_year, first_year + length AS last_year, length, count 
	FROM career c 
	WHERE length > 2
    AND count > 2
    AND roleid IN (3, 5, 6, 7, 9, 11, 14)
    AND contributorid IN (
        SELECT id
        FROM contributor c 
        WHERE first_year >= %(year)s
        AND first_year <= %(year)s + 4
        AND stateid NOT IN (9)
        AND genderid IN (%(gender)s)
    )
) t1
GROUP BY contributorid 
'''

multi_career_query = '''
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
            AND first_year <= %(year)s + 4
            AND stateid NOT IN (9)
        )
        GROUP BY contributorid
        HAVING COUNT(contributorid) > 1
    )
) t1
GROUP BY contributorid 
'''