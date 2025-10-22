insert into issue
(issue_id, short_name, detail, category_id)
VALUES
(34, 'co_before_vo', 'Condition Occurrence start date is before corresponding Visit occurrence start date', 7)
;

insert into marker
(omop_table, omop_id, issue_id)
select 'condition_occurrence', co.condition_occurrence_id, issue_id
from condition_occurrence as co
left join visit_occurrence as vo
ON co.visit_occurrence_id = vo.visit_occurrence_id
join issue on issue.short_name = 'co_before_vo'
WHERE co.condition_start_date < vo.visit_start_date
OR co.condition_start_datetime < vo.visit_start_datetime
;

