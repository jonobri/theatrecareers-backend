gender_query = '''
SELECT genderid AS id, gender
FROM ausstage.gendermenu;
'''

state_query = '''
SELECT stateid AS id, state
FROM ausstage.states;
'''

function_list_query = '''
SELECT contributorfunctpreferredid AS id, contfunction AS function_name
FROM ausstage.contfunct;
'''

role_query = '''
INSERT INTO role (id, role)
VALUES (1,'Administrators'), 
(2,'Coaches'), 
(3,'Costume Designers'),
(4,'Dancers'),
(5,'Set and Physical Designers'),
(6,'Directors & Curators'),
(7,'Lighting Designers'),
(8,'Musicians'),
(9,'Performers'),
(10,'Producers'),
(11,'Sound Designers'),
(12,'Technical Staff'),
(13,'Video Designers'),
(14,'Writers & Creators'),
(15,'Miscellaneous');
'''

contributor_query = '''
SELECT contributorid AS id, last_name, first_name, CONCAT(first_name, ' ', last_name) AS display_name, gender AS genderid, state AS stateid
FROM ausstage.contributor
-- Select only contributors active within year range
WHERE contributorid IN (
    SELECT contributorid FROM ausstage.conevlink
    WHERE eventid IN (
        SELECT id FROM atc_alchemy.event
    )
);
'''

event_query = '''
SELECT eventid AS id, event_name, venue.state AS stateid, 
CAST(events.yyyyfirst_date AS SIGNED) AS year
FROM ausstage.events
JOIN ausstage.venue ON events.venueid = venue.venueid
WHERE events.yyyyfirst_date >= %s
AND events.yyyyfirst_date <= %s;
'''

link_table_old_query = '''
SELECT DISTINCT conevlink.eventid, COALESCE(w.workid, 0) AS workid, 
    COALESCE(o.organisationid, 0) AS organisationid, contributorid, conevlink.function AS functionid
FROM ausstage.conevlink
LEFT JOIN 
(
    SELECT eventid, MIN(workid) AS workid
    FROM ausstage.eventworklink
    GROUP BY eventid
) w ON w.eventid = conevlink.eventid
LEFT JOIN 
(
    SELECT eventid, MIN(organisationid) AS organisationid 
    FROM ausstage.orgevlink 
    GROUP BY eventid
) o ON o.eventid = conevlink.eventid
WHERE conevlink.eventid IN (SELECT id FROM atc_alchemy.event)
AND contributorid IN (SELECT id FROM atc_alchemy.contributor);
'''

# This has more effective tour filter
# Uses event name and event year to ensure that each person is only in one show of
# the same title in the same role per year This won't be perfect, but because of the cursed way this database works,
# it's probably about the best I can do for now
link_table_query = '''
SELECT contributorid, functionid, t2.eventid, 
COALESCE(w.workid, 0) AS workid, COALESCE(o.organisationid, 0) AS organisationid
FROM (
	SELECT DISTINCT t1.contributorid, functionid, t1.eventid, e.event_name, event_year
	FROM ( 
		SELECT contributorid, cel.`FUNCTION` AS functionid, cel.eventid, e.EVENT_NAME, e.YYYYFIRST_DATE AS event_year
		FROM ausstage.conevlink cel 
		LEFT JOIN events e ON cel.EVENTID = e.EVENTID  
		GROUP BY CONTRIBUTORID, `function`, event_name, event_year 
	) t1
	JOIN events e ON e.EVENTID = t1.eventid
	JOIN contributor c ON c.CONTRIBUTORID = t1.contributorid
) t2
LEFT JOIN 
(
    SELECT eventid, MIN(workid) AS workid
    FROM ausstage.eventworklink
    GROUP BY eventid
) w ON w.eventid = t2.eventid
LEFT JOIN 
(
    SELECT eventid, MIN(organisationid) AS organisationid 
    FROM ausstage.orgevlink 
    GROUP BY eventid
) o ON o.eventid = t2.eventid
WHERE t2.eventid IN (SELECT id FROM atc_alchemy.event)
AND contributorid IN (SELECT id FROM atc_alchemy.contributor);
'''



# TODO: as above. I think this SQL could use a revisit
#  Check: how is first_year last_year data being filtered?
career_query = '''
SELECT contributorid, roleid, year, count(DISTINCT contributorid, event_name, workid, organisationid, e.year) AS count
FROM link_table lt
JOIN event e ON lt.eventid = e.id
JOIN contributor c ON lt.contributorid = c.id
GROUP BY contributorid, roleid, year
ORDER BY contributorid, roleid, year
'''

career_config = '''
ALTER TABLE career
ADD PRIMARY KEY (id),
ADD FOREIGN KEY (contributorid) REFERENCES contributor(id),
ADD FOREIGN KEY (roleid) REFERENCES role(id)
'''

# Update queries for additional data

# event.contributor_count variable for each event
contributor_count_query = '''
UPDATE event,
(SELECT count(*) AS contributor_count, eventid
FROM link_table
GROUP BY eventid)
AS counter
SET event.contributor_count = counter.contributor_count
WHERE counter.eventid = event.id;
'''

# contributor.first_year, .last_year function
first_last_year_query = '''
UPDATE contributor,
(SELECT DISTINCT link_table.contributorid, MIN(event.year) AS yearFirst, MAX(event.year) AS yearLast
FROM link_table
JOIN event ON link_table.eventid = event.id
GROUP BY contributorid)
AS setYear
SET contributor.first_year = setYear.yearFirst, contributor.last_year = setYear.yearLast
WHERE contributor.id = setYear.contributorid;
'''