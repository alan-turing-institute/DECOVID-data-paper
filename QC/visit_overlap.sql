insert into issue
(issue_id, short_name, detail, category_id)
VALUES
(5, 'visit_overlap', 'Visit start date occurs during another visit', 7)
;

insert into marker
(omop_table, omop_id, issue_id)
select 'visit_occurrence', vo2.visit_occurrence_id, issue_id
from visit_occurrence as vo1
LEFT JOIN visit_occurrence as vo2
ON vo1.person_id = vo2.person_id
join issue on issue.short_name = 'visit_overlap'
WHERE 
(vo2.visit_start_datetime > vo1.visit_start_datetime and vo2.visit_start_datetime < vo1.visit_end_datetime)
OR (vo2.visit_start_date > vo1.visit_start_date and vo2.visit_start_date < vo1.visit_end_date)
;

insert into marker
(omop_table, omop_id, issue_id)
select 'visit_detail', vd2.visit_detail_id, issue_id
from visit_detail as vd1
LEFT JOIN visit_detail as vd2
ON vd1.person_id = vd2.person_id
join issue on issue.short_name = 'visit_overlap'
WHERE 
(vd2.visit_detail_start_datetime > vd1.visit_detail_start_datetime and vd2.visit_detail_start_datetime < vd1.visit_detail_end_datetime)
OR (vd2.visit_detail_start_date > vd1.visit_detail_start_date and vd2.visit_detail_start_date < vd1.visit_detail_end_date)
;

