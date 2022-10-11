



multi_role_query = '''
SELECT c2.display_name, c2.genderid, COUNT(DISTINCT r.role) AS n_different_roles, GROUP_CONCAT(r.role ORDER BY c.first_year SEPARATOR ', ') AS multirole, c2.last_year - c2.first_year AS length, SUM(count) as role_count, (c2.last_year - c2.first_year) / SUM(count) AS gig_every_n_years
FROM career c
JOIN `role` r ON r.id = c.roleid 
JOIN contributor c2 ON c.contributorid = c2.id
WHERE c.length > 2
AND c.count > 2
AND c2.first_year >= 1950  
AND c2.first_year <= 2005  
AND roleid IN (3, 5, 6, 7, 9, 11, 14)
AND contributorid IN (
	SELECT id
	FROM contributor c
	WHERE stateid NOT IN (9)
)
GROUP BY contributorid 
HAVING COUNT(contributorid) > 1
'''