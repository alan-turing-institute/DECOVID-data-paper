insert into issue
(issue_id, short_name, detail, category_id)
VALUES
(8, 'A and E visits one day only', 'Visits to accident and emergency should have less than 24 hours between start and end', 7)
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'visit_occurrence', visit_occurrence.visit_occurrence_id, 'visit_end_datetime', issue_id
from visit_occurrence
join issue on issue.short_name = 'A and E visits one day only'
where visit_concept_id = 9203
and datediff(hour, visit_occurrence.visit_start_datetime, visit_occurrence.visit_end_datetime) > 24
;

