insert into issue
(issue_id, short_name, detail, category_id)
VALUES
(36, 'date_order', 'End date occurs before start date', 7)
;

insert into marker
(omop_table, omop_id, issue_id)
select 'condition_occurrence', condition_occurrence.condition_occurrence_id, issue_id
from condition_occurrence
join issue on issue.short_name = 'date_order'
WHERE condition_end_date < condition_start_date
;

insert into marker
(omop_table, omop_id, issue_id)
select 'visit_occurrence', visit_occurrence.visit_occurrence_id, issue_id
from visit_occurrence
join issue on issue.short_name = 'date_order'
WHERE visit_end_date < visit_start_date
;

insert into marker
(omop_table, omop_id, issue_id)
select 'visit_detail', visit_detail.visit_detail_id, issue_id
from visit_detail
join issue on issue.short_name = 'date_order'
WHERE visit_detail_end_date < visit_detail_start_date
;

insert into marker
(omop_table, omop_id, issue_id)
select 'drug_exposure', drug_exposure.drug_exposure_id, issue_id
from drug_exposure
join issue on issue.short_name = 'date_order'
WHERE drug_exposure_end_date < drug_exposure_start_date
;

