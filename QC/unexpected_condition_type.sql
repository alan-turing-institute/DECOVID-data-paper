insert into issue
(issue_id, short_name, detail, category_id)
VALUES
(37, 'unexpected condition type', 'condition_type_concept_id is not a concept of the correct type', 2)
;

insert into marker
(omop_table, omop_id, issue_id)
select 'condition_occurrence', condition_occurrence.condition_occurrence_id, issue_id
from condition_occurrence
join issue on issue.short_name = 'unexpected condition type'
where condition_occurrence.condition_type_concept_id not in (
    32019,
    42894222,
    32020,
    38000245,
    40301556,
    45754805,
    32424,
    45905770,
    44786627,
    44786627,
    44786629,
    42898140,
    32535
)
;

