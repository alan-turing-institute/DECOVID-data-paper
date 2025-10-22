insert into issue
(issue_id, short_name, detail, category_id)
VALUES
(38, 'null_cols', 'Column should be NULL, but has a non-NULL entry', 1)
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'condition_occurrence', condition_occurrence.condition_occurrence_id, 'stop_reason', issue_id
from condition_occurrence
join issue on issue.short_name = 'null_cols'
WHERE stop_reason IS NOT NULL 
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'condition_occurrence', condition_occurrence.condition_occurrence_id, 'provider_id', issue_id
from condition_occurrence
join issue on issue.short_name = 'null_cols'
WHERE provider_id IS NOT NULL 
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'condition_occurrence', condition_occurrence.condition_occurrence_id, 'condition_source_value', issue_id
from condition_occurrence
join issue on issue.short_name = 'null_cols'
WHERE condition_source_value IS NOT NULL 
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'condition_occurrence', condition_occurrence.condition_occurrence_id, 'condition_source_concept_id', issue_id
from condition_occurrence
join issue on issue.short_name = 'null_cols'
WHERE condition_source_concept_id IS NOT NULL 
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'condition_occurrence', condition_occurrence.condition_occurrence_id, 'condition_status_source_value', issue_id
from condition_occurrence
join issue on issue.short_name = 'null_cols'
WHERE condition_status_source_value IS NOT NULL 
;

