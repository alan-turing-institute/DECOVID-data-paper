insert into issue
(issue_id, short_name, detail, category_id)
VALUES
(1, 'person has no visit', 'person record does not join to any visit_occurrence record', 2)
;

insert into marker
(omop_table, omop_id, issue_id)
select 'person', person.person_id, issue_id
from person
left join visit_occurrence on person.person_id = visit_occurrence.person_id
join issue on issue.short_name = 'person has no visit'
where visit_occurrence.visit_occurrence_id is null
;

