insert into issue
(issue_id, short_name, detail, category_id)
VALUES
(10, 'vo_death_not_in_death_table', 'Visit discharge_to indicates death, but no record present in death table', 2)
;

insert into marker
(omop_table, omop_id, issue_id)
select 'visit_occurrence', vo.visit_occurrence_id, issue_id
from visit_occurrence as vo
LEFT JOIN death as d
ON vo.person_id = d.person_id
join issue on issue.short_name = 'vo_death_not_in_death_table'
WHERE 
discharge_to_concept_id = 4216643 -- "Patient died"
AND d.person_id IS NULL
;

