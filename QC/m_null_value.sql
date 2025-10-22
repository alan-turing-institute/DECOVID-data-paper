insert into issue
(issue_id, short_name, detail, category_id)
VALUES
(16, 'm_null_value', 'Both value_as_number and value_as_concept_id are NULL', 4)
;

insert into marker
(omop_table, omop_id, issue_id)
select 'measurement', measurement.measurement_id, issue_id
from measurement
join issue on issue.short_name = 'm_null_value'
WHERE measurement.value_as_concept_id IS NULL AND measurement.value_as_number IS NULL AND measurement.measurement_concept_id <> 37310255
;

