insert into issue
(issue_id, short_name, detail, category_id)
VALUES
(31, 'date_datetime_match', 'Date does not match the date part of the datetime', 1)
;

-- MEASUREMENT:
insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'measurement_date', issue_id
from measurement
join issue on issue.short_name = 'date_datetime_match'
WHERE measurement_datetime IS NOT NULL 
AND measurement_date <> CAST(measurement_datetime AS DATE)
;

-- VISIT_OCCURRENCE:
insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'visit_occurrence', visit_occurrence.visit_occurrence_id, 'visit_start_date', issue_id
from visit_occurrence
join issue on issue.short_name = 'date_datetime_match'
WHERE visit_start_datetime IS NOT NULL 
AND visit_start_date <> CAST(visit_start_datetime AS DATE)
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'visit_occurrence', visit_occurrence.visit_occurrence_id, 'visit_end_date', issue_id
from visit_occurrence
join issue on issue.short_name = 'date_datetime_match'
WHERE visit_end_datetime IS NOT NULL 
AND visit_end_date <> CAST(visit_end_datetime AS DATE)
;

-- VISIT_DETAIL:
insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'visit_detail', visit_detail.visit_detail_id, 'visit_detail_start_date', issue_id
from visit_detail
join issue on issue.short_name = 'date_datetime_match'
WHERE visit_detail_start_datetime IS NOT NULL 
AND visit_detail_start_date <> CAST(visit_detail_start_datetime AS DATE)
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'visit_detail', visit_detail.visit_detail_id, 'visit_detail_end_date', issue_id
from visit_detail
join issue on issue.short_name = 'date_datetime_match'
WHERE visit_detail_end_datetime IS NOT NULL 
AND visit_detail_end_date <> CAST(visit_detail_end_datetime AS DATE)
;

-- SPECIMEN:
insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'specimen', specimen.specimen_id, 'specimen_date', issue_id
from specimen
join issue on issue.short_name = 'date_datetime_match'
WHERE specimen_datetime IS NOT NULL 
AND specimen_date <> CAST(specimen_datetime AS DATE)
;

-- DEATH:
insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'death', death.person_id, 'death_date', issue_id
from death
join issue on issue.short_name = 'date_datetime_match'
WHERE death_datetime IS NOT NULL 
AND death_date <> CAST(death_datetime AS DATE)
;

-- DRUG_EXPOSURE:
insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'drug_exposure', drug_exposure.drug_exposure_id, 'drug_exposure_start_date', issue_id
from drug_exposure
join issue on issue.short_name = 'date_datetime_match'
WHERE drug_exposure_start_datetime IS NOT NULL 
AND drug_exposure_start_date <> CAST(drug_exposure_start_datetime AS DATE)
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'drug_exposure', drug_exposure.drug_exposure_id, 'drug_exposure_end_date', issue_id
from drug_exposure
join issue on issue.short_name = 'date_datetime_match'
WHERE drug_exposure_end_datetime IS NOT NULL 
AND drug_exposure_end_date <> CAST(drug_exposure_end_datetime AS DATE)
;

