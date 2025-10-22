insert into issue
(issue_id, short_name, detail, category_id)
VALUES
(3, 'visit end vs discharge', 'If visit_end_date is null then discharge_to_concept_id must be null', 4)
;

insert into marker
(omop_table, omop_id, issue_id)
select 'visit_occurrence', visit_occurrence.visit_occurrence_id, issue_id
from visit_occurrence
join issue on issue.short_name = 'visit end vs discharge'
where visit_end_date is null and discharge_to_concept_id is not null
;

