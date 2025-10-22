insert into issue
(issue_id, short_name, detail, category_id)
VALUES
(43, 'short intervisit period', 'visit_occurrences starting less than 60 minutes after a previous visit ends', 7)
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'visit_occurrence', v2.visit_occurrence_id, 'visit_occurrence_start_datetime', issue_id
from visit_occurrence v1
join visit_occurrence v2 on v1.person_id = v2.person_id
and datediff(mi, v1.visit_end_datetime, v2.visit_start_datetime) between 0 and 59
and v1.visit_occurrence_id != v2.visit_occurrence_id
join issue on issue.short_name = 'short intervisit period'
;

