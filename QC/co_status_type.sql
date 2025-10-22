insert into issue
(issue_id, short_name, detail, category_id)
VALUES
(40, 'co_status_type', 'condition_status_concept_id is not of the correct type. Should be one ofthe following: Admitting diagnosis, First position condition, Preliminary diagnosis, Final diagnosis', 1)
;

insert into marker
(omop_table, omop_id, issue_id)
select 'condition_occurrence', condition_occurrence.condition_occurrence_id, issue_id
from condition_occurrence
join issue ON issue.short_name = 'co_status_type'
WHERE condition_status_concept_id NOT IN ( 4203942, 44786628, 4033240, 4230359 )  -- ( Admitting diagnosis, First position condition, Preliminary diagnosis, Final diagnosis )
AND condition_status_concept_id IS NOT NULL
;

