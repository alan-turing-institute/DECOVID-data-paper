insert into issue
(issue_id, short_name, detail, category_id)
VALUES
(9, 'visit_pre_person_mismatch', 'Preceding visit corresponds to a visit of another person_id', 2)
;

insert into marker
(omop_table, omop_id, issue_id)
select 'visit_occurrence', vo1.visit_occurrence_id, issue_id
from visit_occurrence as vo1
LEFT JOIN visit_occurrence as vo2
ON vo1.preceding_visit_occurrence_id = vo2.visit_occurrence_id
join issue on issue.short_name = 'visit_pre_person_mismatch'
WHERE 
vo1.preceding_visit_occurrence_id IS NOT NULL 
AND vo1.person_id <> vo2.person_id
;

insert into marker
(omop_table, omop_id, issue_id)
select 'visit_detail', vd1.visit_detail_id, issue_id
from visit_detail as vd1
LEFT JOIN visit_detail as vd2
ON vd1.preceding_visit_detail_id = vd2.visit_detail_id
join issue on issue.short_name = 'visit_pre_person_mismatch'
WHERE 
vd1.preceding_visit_detail_id IS NOT NULL 
AND vd1.person_id <> vd2.person_id
;

