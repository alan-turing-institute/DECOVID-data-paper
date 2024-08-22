#This is the query used to extract COVID cases
covid_pcr_query <- paste("SELECT a.visit_occurrence_id
                        FROM
                        (SELECT visit_occurrence_id,
                                measurement_id AS fact_id_1
                                FROM measurement
                                WHERE (measurement_concept_id=37310255)
                                AND (value_as_concept_id=37310282)) a
                        INNER JOIN
                        (SELECT fact_id_1,
                              fact_id_2 as specimen_id
                              FROM fact_relationship
                        WHERE domain_concept_id_1=21 AND domain_concept_id_2=36) b
                        ON (a.fact_id_1 = b.fact_id_1)
                        INNER JOIN
                        (SELECT specimen_id,
                                specimen_date
                                FROM specimen) c
                        ON (b.specimen_id = c.specimen_id)
                        INNER JOIN
                        (SELECT visit_occurrence_id,
                                visit_start_date,
                                visit_end_date
                                FROM visit_occurrence) d
                        ON (a.visit_occurrence_id = d.visit_occurrence_id)
                        WHERE ( DATEDIFF(day,  visit_start_date,  specimen_date) <= 14 AND (specimen_date <=visit_end_date))
                        OR ( DATEDIFF(day, visit_start_date, specimen_date) <= 14 AND (visit_end_date IS NULL))")

#Confirmed/suspected COVID-19 Query
covid_obs_all_query <- paste("SELECT a.visit_occurrence_id
                              FROM
                              (SELECT * FROM condition_occurrence
                              WHERE condition_concept_id IN (45590872, 703441,
                              37310287, 45604597, 37311060, 703440, 37310282,
                              439676, 45585955, 37311061, 45756093, 45756094,
                              320651, 37310268)) a
                              INNER JOIN (SELECT visit_start_date,
                                                 visit_end_date,
                                                 visit_occurrence_id
                                                 FROM visit_occurrence) b
                              ON (a.visit_occurrence_id = b.visit_occurrence_id)
                              WHERE (DATEDIFF(day, visit_start_date, condition_start_date) <= 14 AND (condition_start_date <=visit_end_date))
                              OR (DATEDIFF(day, visit_start_date, condition_start_date) <= 14 AND (visit_end_date IS NULL))")


visit_query <- paste0("SELECT visit_occurrence_id,
                              a.person_id,
                              gender_concept_name,
                              race_concept_name,
                              year_of_birth,
                              hospital_site,
                              visit_start_date,
                              visit_end_date,
                              admitting_source_concept_name,
                              discharge_to_concept_name,
                              patient_days,
                              patient_hours
                              FROM
                              (SELECT visit_occurrence_id,
                                      person_id,
                                      visit_start_date,
                                      visit_end_date,
                                      admitting_source_concept_id,
                                      discharge_to_concept_id,
                             (DATEDIFF(minute, visit_start_datetime, visit_end_datetime) / 1440) AS patient_days,
                             (DATEDIFF(minute, visit_start_datetime, visit_end_datetime) / 60) AS patient_hours,
                              visit_occurrence_id %10 AS hospital_site
                              FROM visit_occurrence) a
                              LEFT JOIN
                              (SELECT gender_concept_id,
                                      race_concept_id,
                                      person_id,
                                      year_of_birth
                              FROM person) b
                              ON (a.person_id = b.person_id)
                              LEFT JOIN
                              (SELECT concept_id as gender_concept_id,
                                      concept_name as gender_concept_name
                                FROM concept) c
                                ON (b.gender_concept_id = c.gender_concept_id)
                               LEFT JOIN
                              (SELECT concept_id as race_concept_id,
                                      concept_name as race_concept_name
                              FROM concept) d
                                ON (b.race_concept_id = d.race_concept_id)
                              LEFT JOIN
                              (SELECT concept_id as discharge_to_concept_id,
                                     concept_name as discharge_to_concept_name
                              FROM concept) e
                              ON (a.discharge_to_concept_id = e.discharge_to_concept_id)
                              LEFT JOIN
                              (SELECT concept_id as admitting_source_concept_id,
                                      concept_name as admitting_source_concept_name
                              FROM concept) f
                              ON (a.admitting_source_concept_id = f.admitting_source_concept_id)")

condition_q <- paste0("SELECT a.*,
                              a.visit_occurrence_id %10 AS hospital_site,
                              b.concept_id,
                              b.concept_name,
                              c.concept_id AS concept_id_specific,
                              c.concept_name AS concept_name_specific
                      FROM condition_occurrence a
                      LEFT JOIN concept  b
                      ON a.condition_type_concept_id=b.concept_id
                      LEFT JOIN concept  c
                      ON a.condition_concept_id=c.concept_id")
