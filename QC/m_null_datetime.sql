insert into issue
(issue_id, short_name, detail, category_id)
VALUES
(17, 'm_null_datetime', 'With the exception of Covid Swab test (concept 37310255), measurement datetime should not be NULL', 4)
;

insert into marker
(omop_table, omop_id, issue_id)
select 'measurement', measurement.measurement_id, issue_id
from measurement
join issue on issue.short_name = 'm_null_datetime'
WHERE measurement.measurement_datetime IS NULL 
AND measurement.measurement_concept_id <> 37310255
;

