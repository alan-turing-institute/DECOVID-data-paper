insert into issue
(issue_id, short_name, detail, category_id)
VALUES
(39, 'condition person mismatch', 'condition occurrence person id does not match the person id of associated visit', 2)
;

insert into marker
(omop_table, omop_id, issue_id)
select 'visit_occurrence', visit_occurrence.visit_occurrence_id, issue_id
from visit_occurrence
join condition_occurrence on visit_occurrence.visit_occurrence_id = condition_occurrence.visit_occurrence_id
join issue on issue.short_name = 'condition person mismatch'
where visit_occurrence.person_id != condition_occurrence.person_id
;

insert into marker
(omop_table, omop_id, issue_id)
select 'visit_detail', visit_detail.visit_detail_id, issue_id
from visit_detail
join condition_occurrence on visit_detail.visit_detail_id = condition_occurrence.visit_detail_id
join issue on issue.short_name = 'condition person mismatch'
where visit_detail.person_id != condition_occurrence.person_id
;

