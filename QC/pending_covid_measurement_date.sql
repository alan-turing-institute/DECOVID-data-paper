insert into issue
(issue_id, short_name, detail, category_id)
VALUES
(41, 'pending covid measurement date', 'covid swap pcr test does not have a result value but does have a measurement date or time', 4)
;

insert into marker
(omop_table, omop_id, issue_id)
select 'measurement', measurement.measurement_id, issue_id
from measurement 
join issue on issue.short_name = 'pending covid measurement date'
where measurement_concept_id = 37310255
and value_as_concept_id = 0
and (measurement_datetime is not null or measurement_date is not NULL)
;

