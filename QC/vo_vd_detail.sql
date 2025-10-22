insert into issue
(issue_id, short_name, detail, category_id)
VALUES
(20, 'vo_vd_detail', 'Visit Occurrence does not have a Visit Detail', 4)
;

insert into marker
(omop_table, omop_id, issue_id)
select 'visit_occurrence', vo.visit_occurrence_id, issue_id
from visit_occurrence as vo
left join visit_detail as vd
ON vo.visit_occurrence_id = vd.visit_occurrence_id
join issue on issue.short_name = 'vo_vd_detail'
WHERE vd.visit_detail_id IS NULL
;

