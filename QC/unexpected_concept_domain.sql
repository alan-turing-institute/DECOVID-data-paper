insert into issue
(issue_id, short_name, detail, category_id)
VALUES
(30, 'unexpected concept domain', 'domain of the linked concept is unexpected for records in this table', 2)
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'condition_occurrence', condition_occurrence.condition_occurrence_id, 'condition_concept_id', issue_id
from condition_occurrence
join concept on condition_occurrence.condition_concept_id = concept.concept_id
join issue on short_name = 'unexpected concept domain'
where domain_id not in (
    'Procedure',
    'Gender',
    'Condition',
    'Relationship',
    'Observation',
    'Measurement'
)
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'condition_occurrence', condition_occurrence.condition_occurrence_id, 'condition_type_concept_id', issue_id
from condition_occurrence
join concept on condition_occurrence.condition_type_concept_id = concept.concept_id
join issue on short_name = 'unexpected concept domain'
where vocabulary_id not in (
    'Condition Type',
    'SNOMED'
)
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'person', person.person_id, 'gender_concept_id', issue_id
from person
join concept on person.gender_concept_id = concept.concept_id
join issue on short_name = 'unexpected concept domain'
where domain_id != 'Gender'
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'person', person.person_id, 'race_concept_id', issue_id
from person
join concept on person.race_concept_id = concept.concept_id
join issue on short_name = 'unexpected concept domain'
where domain_id != 'Race'
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'visit_occurrence', visit_occurrence.visit_occurrence_id, 'visit_concept_id', issue_id
from visit_occurrence
join concept on visit_occurrence.visit_concept_id = concept.concept_id
join issue on short_name = 'unexpected concept domain'
where domain_id != 'Visit'
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'visit_occurrence', visit_occurrence.visit_occurrence_id, 'admitting_source_concept_id', issue_id
from visit_occurrence
join concept on visit_occurrence.admitting_source_concept_id = concept.concept_id
join issue on short_name = 'unexpected concept domain'
where domain_id != 'Visit'
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'visit_occurrence', visit_occurrence.visit_occurrence_id, 'discharge_to_concept_id', issue_id
from visit_occurrence
join concept on visit_occurrence.discharge_to_concept_id = concept.concept_id
join issue on short_name = 'unexpected concept domain'
where domain_id != 'Visit'
;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'visit_detail', visit_detail.visit_detail_id, 'visit_detail_concept_id', issue_id
from visit_detail
join concept on visit_detail.visit_detail_concept_id = concept.concept_id
join issue on short_name = 'unexpected concept domain'
where domain_id != 'Visit'
;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'procedure_occurrence', procedure_occurrence.procedure_occurrence_id, 'procedure_concept_id', issue_id
from procedure_occurrence
join concept on procedure_occurrence.procedure_concept_id = concept.concept_id
join issue on short_name = 'unexpected concept domain'
where domain_id != 'Procedure'
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'procedure_occurrence', procedure_occurrence.procedure_occurrence_id, 'procedure_type_concept_id', issue_id
from procedure_occurrence
join concept on procedure_occurrence.procedure_type_concept_id = concept.concept_id
join issue on short_name = 'unexpected concept domain'
where concept_class_id != 'Procedure Type'
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'drug_exposure', drug_exposure.drug_exposure_id, 'drug_concept_id', issue_id
from drug_exposure
join concept on drug_exposure.drug_concept_id = concept.concept_id
join issue on short_name = 'unexpected concept domain'
where domain_id not in ('Drug', 'Device')
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'drug_exposure', drug_exposure.drug_exposure_id, 'drug_type_concept_id', issue_id
from drug_exposure
join concept on drug_exposure.drug_type_concept_id = concept.concept_id
join issue on short_name = 'unexpected concept domain'
where concept_class_id != 'Drug Type'
;


insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'drug_exposure', drug_exposure.drug_exposure_id, 'dose_unit_concept_id', issue_id
from drug_exposure
join concept on drug_exposure.dose_unit_concept_id = concept.concept_id
join issue on short_name = 'unexpected concept domain'
where domain_id != 'Unit'
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'drug_exposure', drug_exposure.drug_exposure_id, 'route_concept_id', issue_id
from drug_exposure
join concept on drug_exposure.route_concept_id = concept.concept_id
join issue on short_name = 'unexpected concept domain'
where domain_id != 'Route'
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'measurement_concept_id', issue_id
from measurement
join concept on measurement.measurement_concept_id = concept.concept_id
join issue on short_name = 'unexpected concept domain'
where domain_id not in ('Measurement', 'Observation')
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'operator_concept_id', issue_id
from measurement
join concept on measurement.operator_concept_id = concept.concept_id
join issue on short_name = 'unexpected concept domain'
where domain_id != 'Meas Value Operator'
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_concept_id', issue_id
from measurement
join concept on measurement.value_as_concept_id = concept.concept_id
join issue on short_name = 'unexpected concept domain'
where domain_id not in (
'Condition',
'Device',
'Meas Value',
'Measurement',
'Metadata',
'Observation',
'Procedure'
)
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'unit_concept_id', issue_id
from measurement
join concept on measurement.unit_concept_id = concept.concept_id
join issue on short_name = 'unexpected concept domain'
where domain_id != 'Unit' and concept.concept_id != 0
;

