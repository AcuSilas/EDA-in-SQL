USE md_water_services;

-- Are there any specific provinces, or towns where some sources are more abundant?
-- We identified that tap_in_home_broken taps are easy wins. 
-- Are there any towns where this is a particular problem?

-- To answer question 1, we will need province_name and town_name from the location table.
-- We also need to know type_of_water_source and
-- number_of_people_served from the water_source table.
-- The problem is that the location table uses location_id while water_source only has source_id.
-- So we won't be able to join these tables directly. But the visits table maps location_id and source_id.
-- So if we use visits as the table we query from, we can join location where
-- the location_id matches, and water_source where the source_id matches.
-- Start by joining location to visits.
SELECT
location.town_name,
location.province_name,
location.location_id,
visits.visit_count
FROM
visits
RIGHT JOIN
location
ON location.location_id = visits.location_id;

-- Now, we can join the water_source table on the key shared between water_source and visits.
SELECT
location.town_name,
location.province_name,
location.location_id,
visits.visit_count,
water_source.type_of_water_source,
water_source.number_of_people_served
FROM
visits
RIGHT JOIN
location
ON location.location_id = visits.location_id
RIGHT JOIN
water_source
ON water_source.source_id = visits.source_id;


SELECT
water_source.type_of_water_source,
location.town_name,
location.province_name,
location.location_type,
water_source.number_of_people_served,
visits.time_in_queue,
well_pollution.results
FROM
visits
LEFT JOIN
well_pollution
ON well_pollution.source_id = visits.source_id
INNER JOIN 
location
ON location.location_id = visits.location_id
INNER JOIN 
water_source
ON water_source.source_id = visits.source_id
WHERE 
visits.visit_count = 1;

CREATE VIEW combined_analysis_table AS
-- This view assembles data from different tables into one to simplify analysis
SELECT
water_source.type_of_water_source AS source_type,
location.town_name,
location.province_name,
location.location_type,
water_source.number_of_people_served AS people_served,
visits.time_in_queue,
well_pollution.results
FROM
visits
LEFT JOIN
well_pollution
ON well_pollution.source_id = visits.source_id
INNER JOIN
location
ON location.location_id = visits.location_id
INNER JOIN
water_source
ON water_source.source_id = visits.source_id
WHERE
visits.visit_count = 1;

USE md_water_services;
-- The main query selects the province names,
-- we create a bunch of columns for each type of water source with CASE statements,
-- sum each of them together, and calculate percentages.
-- We join the province_totals table to our combined_analysis_table 
-- so that the correct value for each province's pt.total_ppl_serv value is used
-- Finally we group by province_name to get the provincial percentages.

WITH province_totals AS (-- This CTE calculates the population of each province
SELECT
province_name,
SUM(people_served) AS total_ppl_serv
FROM
combined_analysis_table
GROUP BY
province_name
)
SELECT
ct.province_name,
-- These case statements create columns for each type of source.
-- The results are aggregated and percentages are calculated
ROUND((SUM(CASE WHEN source_type = 'river'
THEN people_served ELSE 0 END) * 100.0 / pt.total_ppl_serv), 0) AS river,
ROUND((SUM(CASE WHEN source_type = 'shared_tap'
THEN people_served ELSE 0 END) * 100.0 / pt.total_ppl_serv), 0) AS shared_tap,
ROUND((SUM(CASE WHEN source_type = 'tap_in_home'
THEN people_served ELSE 0 END) * 100.0 / pt.total_ppl_serv), 0) AS tap_in_home,
ROUND((SUM(CASE WHEN source_type = 'tap_in_home_broken'
THEN people_served ELSE 0 END) * 100.0 / pt.total_ppl_serv), 0) AS tap_in_home_broken,
ROUND((SUM(CASE WHEN source_type = 'well'
THEN people_served ELSE 0 END) * 100.0 / pt.total_ppl_serv), 0) AS well
FROM
combined_analysis_table ct
JOIN
province_totals pt ON ct.province_name = pt.province_name
GROUP BY
ct.province_name
ORDER BY
ct.province_name;

-- Let's aggregate the data per town now.
-- we have to group by province first, then by town, 
-- so that the duplicate towns are distinct because they are in different towns.
WITH town_totals AS (
--  This CTE calculates the population of each town
--  Since there are two Harare towns, we have to group by province_name and town_name
SELECT province_name, town_name, SUM(people_served) AS total_ppl_serv
FROM combined_analysis_table
GROUP BY province_name,town_name
)
SELECT
ct.province_name,
ct.town_name,
ROUND((SUM(CASE WHEN source_type = 'river'
THEN people_served ELSE 0 END) * 100.0 / tt.total_ppl_serv), 0) AS river,
ROUND((SUM(CASE WHEN source_type = 'shared_tap'
THEN people_served ELSE 0 END) * 100.0 / tt.total_ppl_serv), 0) AS shared_tap,
ROUND((SUM(CASE WHEN source_type = 'tap_in_home'
THEN people_served ELSE 0 END) * 100.0 / tt.total_ppl_serv), 0) AS tap_in_home,
ROUND((SUM(CASE WHEN source_type = 'tap_in_home_broken'
THEN people_served ELSE 0 END) * 100.0 / tt.total_ppl_serv), 0) AS tap_in_home_broken,
ROUND((SUM(CASE WHEN source_type = 'well'
THEN people_served ELSE 0 END) * 100.0 / tt.total_ppl_serv), 0) AS well
FROM
combined_analysis_table ct
JOIN 
-- Since the town names are not unique, we have to join on a composite key
town_totals tt ON ct.province_name = tt.province_name AND ct.town_name = tt.town_name
GROUP BY 
-- We group by province first, then by town.
ct.province_name,
ct.town_name
ORDER BY
ct.town_name;

CREATE TEMPORARY TABLE town_aggregated_water_access
WITH town_totals AS (
--  This CTE calculates the population of each town
--  Since there are two Harare towns, we have to group by province_name and town_name
SELECT province_name, town_name, SUM(people_served) AS total_ppl_serv
FROM combined_analysis_table
GROUP BY province_name,town_name
)
SELECT
ct.province_name,
ct.town_name,
ROUND((SUM(CASE WHEN source_type = 'river'
THEN people_served ELSE 0 END) * 100.0 / tt.total_ppl_serv), 0) AS river,
ROUND((SUM(CASE WHEN source_type = 'shared_tap'
THEN people_served ELSE 0 END) * 100.0 / tt.total_ppl_serv), 0) AS shared_tap,
ROUND((SUM(CASE WHEN source_type = 'tap_in_home'
THEN people_served ELSE 0 END) * 100.0 / tt.total_ppl_serv), 0) AS tap_in_home,
ROUND((SUM(CASE WHEN source_type = 'tap_in_home_broken'
THEN people_served ELSE 0 END) * 100.0 / tt.total_ppl_serv), 0) AS tap_in_home_broken,
ROUND((SUM(CASE WHEN source_type = 'well' AND results != "Clean"
THEN people_served ELSE 0 END) * 100.0 / tt.total_ppl_serv), 0) AS well
FROM
combined_analysis_table ct
JOIN 
-- Since the town names are not unique, we have to join on a composite key
town_totals tt ON ct.province_name = tt.province_name AND ct.town_name = tt.town_name
GROUP BY 
-- We group by province first, then by town.
ct.province_name,
ct.town_name
ORDER BY
ct.town_name;


-- which town has the highest ratio of people who have taps, but have no running water?
SELECT
province_name,
town_name,
ROUND(tap_in_home_broken / (tap_in_home_broken + tap_in_home) *
100,0) AS Pct_broken_taps
FROM
town_aggregated_water_access
ORDER BY Pct_broken_taps DESC;

SELECT
province_name,
town_name
FROM
town_aggregated_water_access
WHERE well_pollution.results != "Clean";

-- This query creates the Project_progress table:
/* Source_status −− We want to limit the type of information engineers can give us, so we
limit Source_status.
− By DEFAULT all projects are in the "Backlog" which is like a TODO list.
− CHECK() ensures only those three options will be accepted. This helps to maintain clean data.
*/
CREATE TABLE Project_progress (
Project_id SERIAL PRIMARY KEY,
/* Project_id −− Unique key for sources in case we visit the same
source more than once in the future.
*/
source_id VARCHAR(20) NOT NULL REFERENCES water_source(source_id) ON DELETE CASCADE ON UPDATE CASCADE,
/* source_id −− Each of the sources we want to improve should exist,
and should refer to the source table. This ensures data integrity.
*/
Address VARCHAR(50), 
-- Street address
Town VARCHAR(30),
Province VARCHAR(30),
Source_type VARCHAR(50),
Improvement VARCHAR(50),
Source_status VARCHAR(50) DEFAULT 'Backlog' CHECK (Source_status IN ('Backlog', 'In progress', 'Complete')),
Date_of_completion DATE
);

-- Project_progress_query
-- It joins the location, visits, and well_pollution tables to the water_source table
-- Since well_pollution only has data for wells, 
-- we have to join those records to the water_source table with a LEFT JOIN 
-- and we used visits to link the various id's together.
SELECT
location.address,
location.town_name,
location.province_name,
water_source.source_id,
water_source.type_of_water_source,
well_pollution.results
FROM
water_source
LEFT JOIN
well_pollution ON water_source.source_id = well_pollution.source_id
INNER JOIN
visits ON water_source.source_id = visits.source_id
INNER JOIN
location ON location.location_id = visits.location_id;

-- First things first, let's filter the data to only contain sources we want to improve 
-- by thinking through the logic first.
-- 1. Only records with visit_count = 1 are allowed.
-- 2. Any of the following rows can be included:
-- a. Where shared taps have queue times over 30 min.
-- b. Only wells that are contaminated are allowed -- So we exclude wells that are Clean
-- c. Include any river and tap_in_home_broken sources.

SELECT
location.address,
location.town_name,
location.province_name,
water_source.source_id,
water_source.type_of_water_source,
well_pollution.results
FROM
water_source
LEFT JOIN
well_pollution ON water_source.source_id = well_pollution.source_id
INNER JOIN
visits ON water_source.source_id = visits.source_id
INNER JOIN
location ON location.location_id = visits.location_id
WHERE
visits.visit_count = 1 
AND ( well_pollution.results != 'Clean'
OR water_source.type_of_water_source IN ('tap_in_home_broken','river')
OR (water_source.type_of_water_source = 'shared_tap' AND visits.time_in_queue >= '30')
);

USE md_water_services;

SET SQL_SAFE_UPDATES=0;
UPDATE
well_pollution
SET
description = 'Bacteria: E. coli'
WHERE
description = 'Clean Bacteria: E. coli';
UPDATE
well_pollution
SET
description = 'Bacteria: Giardia Lamblia'
WHERE
description = 'Clean Bacteria: Giardia Lamblia';
UPDATE
well_pollution
SET
results = 'Contaminated: Biological'
WHERE
biological > 0.01 AND results = 'Clean';

SELECT
location.address,
location.town_name,
location.province_name,
water_source.source_id,
water_source.type_of_water_source,
well_pollution.results,
CASE
    WHEN well_pollution.results = 'Contaminated: Biological' THEN 'Install UV Filter and RO Filter'
    WHEN well_pollution.results = 'Contaminated: Chemical' THEN 'Install RO Filter'
    WHEN water_source.type_of_water_source = 'river' THEN 'Drill well'
    WHEN water_source.type_of_water_source = 'shared_tap' AND time_in_queue >=30 THEN CONCAT('Install', FLOOR(time_in_queue/30), 'taps nearby')
    WHEN water_source.type_of_water_source = 'tap_in_home_broken' THEN 'Diagnose local infrastructure'
    ELSE 'Null'
END AS Improvement_Column
FROM
water_source
LEFT JOIN
well_pollution ON water_source.source_id = well_pollution.source_id
INNER JOIN
visits ON water_source.source_id = visits.source_id
INNER JOIN
location ON location.location_id = visits.location_id
WHERE
visits.visit_count = 1 
AND ( well_pollution.results != 'Clean'
OR water_source.type_of_water_source IN ('tap_in_home_broken','river')
OR (water_source.type_of_water_source = 'shared_tap' AND visits.time_in_queue >= '30')
);

WITH UVF AS
(SELECT
location.address,
location.town_name,
location.province_name,
water_source.source_id,
water_source.type_of_water_source,
well_pollution.results,
CASE
    WHEN well_pollution.results = 'Contaminated: Biological' THEN 'Install UV Filter and RO Filter'
    WHEN well_pollution.results = 'Contaminated: Chemical' THEN 'Install RO Filter'
    WHEN water_source.type_of_water_source = 'river' THEN 'Drill well'
    WHEN water_source.type_of_water_source = 'shared_tap' AND time_in_queue >=30 THEN CONCAT("Install", FLOOR(time_in_queue/30), "taps nearby")
    WHEN water_source.type_of_water_source = 'tap_in_home_broken' THEN 'Diagnose local infrastructure'
    ELSE 'Null'
END AS Improvement_Column
FROM
water_source
LEFT JOIN
well_pollution ON water_source.source_id = well_pollution.source_id
INNER JOIN
visits ON water_source.source_id = visits.source_id
INNER JOIN
location ON location.location_id = visits.location_id
WHERE
visits.visit_count = 1 
AND ( well_pollution.results != 'Clean'
OR water_source.type_of_water_source IN ('tap_in_home_broken','river')
OR (water_source.type_of_water_source = 'shared_tap' AND visits.time_in_queue >= '30')
))
SELECT 
*
FROM
UVF
WHERE Improvement_Column = 'Install UV Filter and RO Filter';

Create View Compiled_table AS(
SELECT
location.address,
location.town_name,
location.province_name,
water_source.source_id,
water_source.type_of_water_source,
well_pollution.results,
CASE
    WHEN well_pollution.results = 'Contaminated: Biological' THEN 'Install UV Filter and RO Filter'
    WHEN well_pollution.results = 'Contaminated: Chemical' THEN 'Install RO Filter'
    WHEN water_source.type_of_water_source = 'river' THEN 'Drill well'
    WHEN water_source.type_of_water_source = 'shared_tap' AND time_in_queue >=30 THEN CONCAT('Install', FLOOR(time_in_queue/30), 'taps nearby')
    WHEN water_source.type_of_water_source = 'tap_in_home_broken' THEN 'Diagnose local infrastructure'
    ELSE 'Null'
END AS Improvement_Column
FROM
water_source
LEFT JOIN
well_pollution ON water_source.source_id = well_pollution.source_id
INNER JOIN
visits ON water_source.source_id = visits.source_id
INNER JOIN
location ON location.location_id = visits.location_id
WHERE
visits.visit_count = 1 
AND ( well_pollution.results != 'Clean'
OR water_source.type_of_water_source IN ('tap_in_home_broken','river')
OR (water_source.type_of_water_source = 'shared_tap' AND visits.time_in_queue >= '30')
));

INSERT INTO project_progress
(source_id, Town, Address, Province, Source_type, Improvement)
SELECT
source_id,
town_name,
address,
province_name,
type_of_water_source,
Improvement_Column
FROM
compiled_table;

-- Ranking Drill well need basedd on number of people served and province wise
SELECT
project_progress.Project_id, 
project_progress.Town, 
project_progress.Province, 
project_progress.Source_type, 
project_progress.Improvement,
Water_source.number_of_people_served,
RANK() OVER(PARTITION BY Province ORDER BY number_of_people_served)
FROM  project_progress 
JOIN water_source 
ON water_source.source_id = project_progress.source_id
WHERE Improvement = "Drill Well"
ORDER BY Province DESC, number_of_people_served