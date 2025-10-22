insert into issue
(issue_id, short_name, detail, category_id)
VALUES
(15, 'pending covid specimen date', 'covid swap pcr test has no outcome but the specimen was over 2 weeks old at time of data cutoff', 4)
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'measurement', measurement.measurement_id, 'value_as_concept_id', issue_id from measurement
join fact_relationship on measurement.measurement_id = fact_relationship.fact_id_1 and fact_relationship.domain_concept_id_1 in (1147330, 21) -- measurement
join specimen on fact_relationship.fact_id_2 = specimen.specimen_id and fact_relationship.domain_concept_id_2 in (1147306, 36) -- specimen
join issue on issue.short_name = 'pending covid specimen date'
where measurement_concept_id = 37310255
and value_as_concept_id = 0
and specimen_date < '2020-08-26'

