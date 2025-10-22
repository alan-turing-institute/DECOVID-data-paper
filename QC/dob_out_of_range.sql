insert into issue
(issue_id, short_name, detail, category_id)
VALUES
(2, 'dob out of range', 'Year of birth of person is not between 1901 and 2002', 1)
;

insert into marker
(omop_table, omop_id, omop_column, issue_id)
select 'person', person.person_id, 'year_of_birth', issue_id
from person
join issue on issue.short_name = 'dob out of range'
where year_of_birth not between 1900 and 2002
;

