insert into issue
(issue_id, short_name, detail, category_id)
VALUES
(12, 'duplicated measurement', 'measurement record shares the same patient/time/concept/value with another measurement record', 5)
;

insert into marker
(omop_table, omop_id, issue_id)
select 'measurement', measurement.measurement_id, issue_id
from measurement
join issue on issue.short_name = 'duplicated measurement'
where concat(person_id, measurement_concept_id, measurement_date, measurement_datetime, value_as_number, value_as_concept_id) in (
select concat(person_id, measurement_concept_id, measurement_date, measurement_datetime, value_as_number, value_as_concept_id)
from measurement
group by person_id, measurement_concept_id, measurement_date, measurement_datetime, value_as_number, value_as_concept_id
having count(*) > 1
)
;

