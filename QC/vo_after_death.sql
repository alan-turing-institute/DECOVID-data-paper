insert into issue
(issue_id, short_name, detail, category_id)
VALUES
(11, 'vo_after_death', 'Visit occurrence extends at least 1 day past death date', 7)
;

insert into marker
(omop_table, omop_id, issue_id)
select 'visit_occurrence', vo.visit_occurrence_id, issue_id
from visit_occurrence as vo
LEFT JOIN death as d
ON vo.person_id = d.person_id
join issue on issue.short_name = 'vo_after_death'
WHERE 
DATEDIFF(day,vo.visit_start_date,d.death_date) < 0
OR DATEDIFF(day,vo.visit_end_date,d.death_date) < 0
;

