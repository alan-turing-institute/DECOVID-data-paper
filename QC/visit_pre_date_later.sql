insert into issue
(issue_id, short_name, detail, category_id)
VALUES
(23, 'visit_pre_date_later', 'Preceding visit has later start date than this visit', 7)
;

insert into marker
(omop_table, omop_id, issue_id)
select 'visit_occurrence', vo1.visit_occurrence_id, issue_id
from visit_occurrence as vo1
LEFT JOIN visit_occurrence as vo2
ON vo1.preceding_visit_occurrence_id = vo2.visit_occurrence_id
join issue on issue.short_name = 'visit_pre_date_later'
WHERE 
vo1.preceding_visit_occurrence_id IS NOT NULL 
AND (vo1.visit_start_date < vo2.visit_start_date OR vo1.visit_start_datetime < vo2.visit_start_datetime)
;

insert into marker
(omop_table, omop_id, issue_id)
select 'visit_detail', vd1.visit_detail_id, issue_id
from visit_detail as vd1
LEFT JOIN visit_detail as vd2
ON vd1.preceding_visit_detail_id = vd2.visit_detail_id
join issue on issue.short_name = 'visit_pre_date_later'
WHERE 
vd1.preceding_visit_detail_id IS NOT NULL 
AND (vd1.visit_detail_start_date < vd2.visit_detail_start_date OR vd1.visit_detail_start_datetime < vd2.visit_detail_start_datetime)
;

