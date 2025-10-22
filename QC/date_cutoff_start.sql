insert into issue
(issue_id, short_name, detail, category_id)
VALUES
(6, 'date_cutoff_start', 'Date or datetime is before 01/01/2020', 1)
;

-- MEASUREMENT:
insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'measurement_date', issue_id
from measurement
join issue on issue.short_name = 'date_cutoff_start'
WHERE measurement_date < '01/01/2020'
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'measurement_datetime', issue_id
from measurement
join issue on issue.short_name = 'date_cutoff_start'
WHERE measurement_datetime < '01/01/2020'
;

-- VISIT_OCCURRENCE:
insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'visit_occurrence', visit_occurrence.visit_occurrence_id, 'visit_start_date', issue_id
from visit_occurrence
join issue on issue.short_name = 'date_cutoff_start'
WHERE visit_start_date < '01/01/2020'
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'visit_occurrence', visit_occurrence.visit_occurrence_id, 'visit_start_datetime', issue_id
from visit_occurrence
join issue on issue.short_name = 'date_cutoff_start'
WHERE visit_start_datetime < '01/01/2020'
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'visit_occurrence', visit_occurrence.visit_occurrence_id, 'visit_end_date', issue_id
from visit_occurrence
join issue on issue.short_name = 'date_cutoff_start'
WHERE visit_end_date < '01/01/2020'
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'visit_occurrence', visit_occurrence.visit_occurrence_id, 'visit_end_datetime', issue_id
from visit_occurrence
join issue on issue.short_name = 'date_cutoff_start'
WHERE visit_end_datetime < '01/01/2020'
;

-- VISIT_DETAIL:
insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'visit_detail', visit_detail.visit_detail_id, 'visit_detail_start_date', issue_id
from visit_detail
join issue on issue.short_name = 'date_cutoff_start'
WHERE visit_detail_start_date < '01/01/2020'
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'visit_detail', visit_detail.visit_detail_id, 'visit_detail_start_datetime', issue_id
from visit_detail
join issue on issue.short_name = 'date_cutoff_start'
WHERE visit_detail_start_datetime < '01/01/2020'
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'visit_detail', visit_detail.visit_detail_id, 'visit_detail_end_date', issue_id
from visit_detail
join issue on issue.short_name = 'date_cutoff_start'
WHERE visit_detail_end_date < '01/01/2020'
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'visit_detail', visit_detail.visit_detail_id, 'visit_detail_end_datetime', issue_id
from visit_detail
join issue on issue.short_name = 'date_cutoff_start'
WHERE visit_detail_end_datetime < '01/01/2020'
;

-- SPECIMEN:
insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'specimen', specimen.specimen_id, 'specimen_date', issue_id
from specimen
join issue on issue.short_name = 'date_cutoff_start'
WHERE specimen_date < '01/01/2020'
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'specimen', specimen.specimen_id, 'specimen_datetime', issue_id
from specimen
join issue on issue.short_name = 'date_cutoff_start'
WHERE specimen_datetime < '01/01/2020'
;

-- DEATH:
insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'death', death.person_id, 'death_date', issue_id
from death
join issue on issue.short_name = 'date_cutoff_start'
WHERE death_date < '01/01/2020'
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'death', death.person_id, 'death_datetime', issue_id
from death
join issue on issue.short_name = 'date_cutoff_start'
WHERE death_datetime < '01/01/2020'
;

-- DRUG_EXPOSURE:
insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'drug_exposure', drug_exposure.drug_exposure_id, 'drug_exposure_start_date', issue_id
from drug_exposure
join issue on issue.short_name = 'date_cutoff_start'
WHERE drug_exposure_start_date < '01/01/2020'
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'drug_exposure', drug_exposure.drug_exposure_id, 'drug_exposure_start_datetime', issue_id
from drug_exposure
join issue on issue.short_name = 'date_cutoff_start'
WHERE drug_exposure_start_datetime < '01/01/2020'
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'drug_exposure', drug_exposure.drug_exposure_id, 'drug_exposure_end_date', issue_id
from drug_exposure
join issue on issue.short_name = 'date_cutoff_start'
WHERE drug_exposure_end_date < '01/01/2020'
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'drug_exposure', drug_exposure.drug_exposure_id, 'drug_exposure_end_datetime', issue_id
from drug_exposure
join issue on issue.short_name = 'date_cutoff_start'
WHERE drug_exposure_end_datetime < '01/01/2020'
;

