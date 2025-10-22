insert into issue
(issue_id, short_name, detail, category_id)
VALUES
(33, 'co_start_dob_order', 'Condition Occurrence start date is before DoB', 7)
;

insert into marker
(omop_table, omop_id, issue_id)
select 'condition_occurrence', co.condition_occurrence_id, issue_id
from condition_occurrence as co
left join person as p
ON co.person_id = p.person_id
join issue on issue.short_name = 'co_start_dob_order'
WHERE YEAR(co.condition_start_date) <= p.year_of_birth - 1
;

