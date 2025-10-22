insert into issue
(issue_id, short_name, detail, category_id)
VALUES
(35, 'datetime_order', 'End datetime occurs before start datetime', 7)
;

insert into marker
(omop_table, omop_id, issue_id)
select 'condition_occurrence', condition_occurrence.condition_occurrence_id, issue_id
from condition_occurrence
join issue on issue.short_name = 'datetime_order'
WHERE condition_end_datetime < condition_start_datetime
;

insert into marker
(omop_table, omop_id, issue_id)
select 'visit_occurrence', visit_occurrence.visit_occurrence_id, issue_id
from visit_occurrence
join issue on issue.short_name = 'datetime_order'
WHERE visit_end_datetime < visit_start_datetime
;

insert into marker
(omop_table, omop_id, issue_id)
select 'visit_detail', visit_detail.visit_detail_id, issue_id
from visit_detail
join issue on issue.short_name = 'datetime_order'
WHERE visit_detail_end_datetime < visit_detail_start_datetime
;

insert into marker
(omop_table, omop_id, issue_id)
select 'drug_exposure', drug_exposure.drug_exposure_id, issue_id
from drug_exposure
join issue on issue.short_name = 'datetime_order'
WHERE drug_exposure_end_datetime < drug_exposure_start_datetime
;

