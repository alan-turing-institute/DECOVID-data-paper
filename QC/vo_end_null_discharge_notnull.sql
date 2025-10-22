insert into issue
(issue_id, short_name, detail, category_id)
VALUES
(4, 'vo_end_null_discharge_notnull', 'Patient has a value in discharge_to_concept_id, but not visit end date/datetime', 1)
;

insert into marker
(omop_table, omop_id, issue_id)
select 'visit_occurrence', visit_occurrence.visit_occurrence_id, issue_id
from visit_occurrence
join issue on issue.short_name = 'vo_end_null_discharge_notnull'
where (visit_end_date is null
or visit_end_datetime is null)
and discharge_to_concept_id IS NOT NULL
;

