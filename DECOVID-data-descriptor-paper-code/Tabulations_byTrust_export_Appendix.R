library(DBI) #for performing SQL queries
library(dplyr) #for data manipulation
library(Hmisc) #for describing variables
library(purrr) #for data manipulation
library(tableone) #for demographics table
library(kableExtra) #for markdown tables
library(reshape2) #for measurement table reshaping
library(lubridate) # for dates
library(aweek) #to get week for the COVID cases time series
library(purrr)

port <- rstudioapi::askForPassword(prompt="Please enter port")
user <- rstudioapi::askForPassword(prompt="Please enter username")
pw <- rstudioapi::askForPassword(prompt="Please enter password")

#Queries to create tables of ids by trust

#Tabulations
drug_concept_id_query <- paste0("SELECT a.drug_exposure_id,
                                        a.drug_concept_id,
                                        b.drug_concept_name, 
                                        b.concept_class_id_drug,
                                        b.vocabulary_id_drug,
                                        a.drug_type_concept_id, 
                                        c.drug_type_concept_name,
                                        c.concept_class_id_drug_type,
                                        c.vocabulary_id_drug_type,
                                        a.route_concept_id, 
                                        d.route_concept_name,
                                        d.concept_class_id_drug_route, 
                                        d.vocabulary_id_drug_route,
                                        a.visit_occurrence_id,
                                        a.person_id,
                                        (CASE WHEN a.visit_occurrence_id IS NOT NULL THEN a.visit_occurrence_id %10
                                          WHEN a.person_id IS NOT NULL THEN a.person_id %10
                                          ELSE NULL END) as hospital_site
                        FROM omop_03082021.drug_exposure a
                        LEFT JOIN 
                        (SELECT concept_id as b_drug_concept_id, 
                               concept_name as drug_concept_name,
                               concept_class_id as concept_class_id_drug,
                               vocabulary_id as vocabulary_id_drug
                          FROM omop_03082021.concept) b
                          ON a.drug_concept_id=b.b_drug_concept_id
                        LEFT JOIN 
                        (SELECT concept_id as c_drug_type_concept_id, 
                                 concept_name as drug_type_concept_name,
                                 concept_class_id as concept_class_id_drug_type,
                                vocabulary_id as vocabulary_id_drug_type
                          FROM omop_03082021.concept) c
                        ON a.drug_type_concept_id=c.c_drug_type_concept_id
                        LEFT JOIN 
                        (SELECT concept_id as d_route_concept_id, 
                                   concept_name as route_concept_name, 
                                    concept_class_id as concept_class_id_drug_route,
                                vocabulary_id as vocabulary_id_drug_route
                        FROM omop_03082021.concept) d
                        ON a.route_concept_id=d.d_route_concept_id")


copd_drugs_tabulation <- dbGetQuery(copd, drug_concept_id_query)
coag_drugs_tabulation<- dbGetQuery(coag, drug_concept_id_query)
vent_drugs_tabulation <- dbGetQuery(vent, drug_concept_id_query)
news_drugs_tabulation <- dbGetQuery(news2, drug_concept_id_query)

drugs_tabulation = rbind(copd_drugs_tabulation, coag_drugs_tabulation, vent_drugs_tabulation, news_drugs_tabulation) %>% 
                    distinct() %>%
                    mutate(hospital_site = case_when(hospital_site=hospital_site==4 ~ "UHB",
                                                    hospital_site==6 ~ "UCLH"))

rm(copd_drugs_tabulation,coag_drugs_tabulation,vent_drugs_tabulation,news_drugs_tabulation)

drugs_concept_table <- drugs_tabulation %>%
                        group_by(drug_concept_id,drug_concept_name, hospital_site) %>%
                        dplyr::summarise(n=n())

drugs_concept_table <- dcast(drugs_concept_table, drug_concept_name + drug_concept_id~hospital_site)

drugs_concept_table = drugs_concept_table %>%
                      mutate(UHB = if_else(is.na(UHB)|UHB<=10,"<=10", as.character(UHB)),
                             UCLH = if_else(is.na(UCLH)|UCLH<=10,"<=10", as.character(UCLH)))

write.csv(drugs_concept_table, "drugs_concept_table_Trust.csv", na="", row.names=FALSE)

drugs_concept_class_table <- drugs_tabulation %>%
                              group_by(concept_class_id_drug, hospital_site) %>%
                              dplyr::summarise(n=n())

drugs_concept_class_table <- dcast(drugs_concept_class_table,  concept_class_id_drug~hospital_site)


drugs_concept_class_table = drugs_concept_class_table %>%
                            mutate(UHB = if_else(is.na(UHB)|UHB<=10,"<=10", as.character(UHB)),
                                   UCLH = if_else(is.na(UCLH)|UCLH<=10,"<=10", as.character(UCLH)))

write.csv(drugs_concept_class_table, "drugs_concept_class_table_Trust.csv", na="", row.names=FALSE)


drugs_concept_vocab_table <- drugs_tabulation %>%
                              group_by(vocabulary_id_drug, hospital_site) %>%
                              dplyr::summarise(n=n())

drugs_concept_vocab_table <- dcast(drugs_concept_vocab_table,  vocabulary_id_drug~hospital_site)


drugs_concept_vocab_table = drugs_concept_vocab_table %>%
                            mutate(UHB = if_else(is.na(UHB)|UHB<=10,"<=10", as.character(UHB)),
                                   UCLH = if_else(is.na(UCLH)|UCLH<=10,"<=10", as.character(UCLH)))

write.csv(drugs_concept_class_table, "drugs_concept_vocab_table_Trust.csv", na="", row.names=FALSE)

drugs_route_concept_table <- drugs_tabulation %>%
                              group_by(route_concept_id,route_concept_name, hospital_site) %>%
                              dplyr::summarise(n=n())

drugs_route_concept_table <- dcast(drugs_route_concept_table, route_concept_name + route_concept_id~hospital_site)

drugs_route_concept_table = drugs_route_concept_table %>%
                             mutate(route_concept_name=ifelse(is.na(route_concept_name), "NULL", as.character(route_concept_name)),
                                    route_concept_id= ifelse(is.na((route_concept_id)),"0", as.character(route_concept_id)),
                                    UHB=if_else(is.na(UHB)|UHB<=10,"<=10", as.character(UHB)),
                                    UCLH = if_else(is.na(UCLH)|UCLH<=10,"<=10", as.character(UCLH)))

write.csv(drugs_route_concept_table, "drugs_route_concep_table_Trust.csv", na="", row.names=FALSE)

rm(drugs_tabulation)

#Condition
condition_concepts_query <- paste0("SELECT a.condition_occurrence_id,
                                        a.condition_concept_id, 
                                        b.condition_concept_name, 
                                        b.concept_class_id_condition,
                                        b.vocabulary_id_condition,
                                        a.condition_type_concept_id, 
                                        c.condition_type_concept_name,
                                        c.concept_class_id_condition_type,
                                        c.vocabulary_id_condition_type,
                                        a.condition_status_concept_id, 
                                        d.condition_status_concept_name,
                                        d.concept_class_id_condition_status,
                                        d.vocabulary_id_condition_status,
                                        a.visit_occurrence_id,
                                        a.person_id,
                                        (CASE WHEN a.visit_occurrence_id IS NOT NULL THEN a.visit_occurrence_id %10
                                          WHEN a.person_id IS NOT NULL THEN a.person_id %10
                                          ELSE NULL END) as hospital_site
                        FROM omop_03082021.condition_occurrence a
                        LEFT JOIN 
                        (SELECT concept_id as b_condition_concept_id, 
                                 concept_name as condition_concept_name,
                                 concept_class_id as concept_class_id_condition,
                                 vocabulary_id as vocabulary_id_condition
                          FROM omop_03082021.concept) b
                          ON a.condition_concept_id=b.b_condition_concept_id
                        LEFT JOIN 
                        (SELECT concept_id as c_condition_type_concept_id, 
                                concept_name as condition_type_concept_name,
                                concept_class_id as concept_class_id_condition_type,
                                 vocabulary_id as vocabulary_id_condition_type
                          FROM omop_03082021.concept) c
                        ON a.condition_type_concept_id=c.c_condition_type_concept_id
                        LEFT JOIN 
                        (SELECT concept_id as d_condition_status_concept_id, 
                                concept_name as condition_status_concept_name,
                                 concept_class_id as concept_class_id_condition_status,
                                 vocabulary_id as vocabulary_id_condition_status
                        FROM omop_03082021.concept) d
                        ON a.condition_status_concept_id=d.d_condition_status_concept_id")


copd_cond_tabulation <- dbGetQuery(copd, condition_concepts_query)
coag_cond_tabulation<- dbGetQuery(coag, condition_concepts_query)
vent_cond_tabulation <- dbGetQuery(vent, condition_concepts_query)
news_cond_tabulation <- dbGetQuery(news2, condition_concepts_query)

conds_tabulation = rbind(copd_cond_tabulation, coag_cond_tabulation, vent_cond_tabulation, news_cond_tabulation) %>% 
                    distinct() %>%
                    mutate(hospital_site = case_when(hospital_site=hospital_site==4 ~ "UHB",
                                                     hospital_site==6 ~ "UCLH"))

rm(copd_cond_tabulation, coag_cond_tabulation, vent_cond_tabulation, news_cond_tabulation)

conds_concept_table <- conds_tabulation %>%
                        group_by(condition_concept_id,condition_concept_name, hospital_site) %>%
                        dplyr::summarise(n=n())

conds_concept_table <- dcast(conds_concept_table, condition_concept_name + condition_concept_id~hospital_site)

conds_concept_table = conds_concept_table %>%
                      mutate(UHB = if_else(is.na(UHB)|UHB<=10,"<=10", as.character(UHB)),
                            UCLH = if_else(is.na(UCLH)|UCLH<=10,"<=10", as.character(UCLH)))

write.csv(conds_concept_table, "conds_concept_table_by_Trust.csv", na="", row.names=FALSE)

conds_concept_vocab_table <- conds_tabulation %>%
                              group_by(vocabulary_id_condition,hospital_site) %>%
                              dplyr::summarise(n=n())

conds_concept_vocab_table <- dcast(conds_concept_vocab_table, vocabulary_id_condition ~hospital_site)

conds_concept_vocab_table = conds_concept_vocab_table %>%
                            mutate(UHB = if_else(is.na(UHB)|UHB<=10,"<=10", as.character(UHB)),
                                  UCLH = if_else(is.na(UCLH)|UCLH<=10,"<=10", as.character(UCLH)))

write.csv(conds_concept_vocab_table, "conds_concept_vocab_table_by_Trust.csv", na="", row.names=FALSE)

conds_concept_class_table <- conds_tabulation %>%
                              group_by(concept_class_id_condition,hospital_site) %>%
                              dplyr::summarise(n=n())

conds_concept_class_table <- dcast(conds_concept_class_table, concept_class_id_condition ~hospital_site)

conds_concept_class_table = conds_concept_class_table %>%
                            mutate(UHB = if_else(is.na(UHB)|UHB<=10,"<=10", as.character(UHB)),
                                   UCLH = if_else(is.na(UCLH)|UCLH<=10,"<=10", as.character(UCLH)))

write.csv(conds_concept_class_table, "conds_concept_class_table_by_Trust.csv", na="", row.names=FALSE)

condition_type_concept_table <- conds_tabulation %>%
                                group_by(condition_type_concept_id,condition_type_concept_name, hospital_site) %>%
                                dplyr::summarise(n=n())

condition_type_concept_table <- dcast(condition_type_concept_table, condition_type_concept_name + condition_type_concept_id~hospital_site)

condition_type_concept_table = condition_type_concept_table %>%
                                    mutate(UHB=if_else(is.na(UHB)|UHB<=10,"<=10", as.character(UHB)),
                                           UCLH = if_else(is.na(UCLH)|UCLH<=10,"<=10", as.character(UCLH)))

write.csv(condition_type_concept_table, "condition_type_concept_table_byTrust.csv", na="", row.names=FALSE)

condition_status_concept_table <- conds_tabulation %>%
                                    group_by(condition_status_concept_id,condition_status_concept_name, hospital_site) %>%
                                    dplyr::summarise(n=n())

condition_status_concept_table <- dcast(condition_status_concept_table, condition_status_concept_name + condition_status_concept_id~hospital_site)

condition_status_concept_table = condition_status_concept_table %>%
                                    mutate(condition_status_concept_name=ifelse(is.na(condition_status_concept_name), "NULL", as.character(condition_status_concept_name)),
                                           condition_status_concept_id=ifelse(is.na(as.character(condition_status_concept_id)),"0", as.character(condition_status_concept_id)),
                                           UHB=if_else(is.na(UHB)|UHB<=10,"<=10", as.character(UHB)),
                                           UCLH = if_else(is.na(UCLH)|UCLH<=10,"<=10", as.character(UCLH)))

write.csv(condition_status_concept_table, "condition_status_concept_table_byTrust.csv", na="", row.names=FALSE)
rm(conds_tabulation)
#Procedure
procedure_concepts_query <- paste0("SELECT a.procedure_occurrence_id,
                                        a.procedure_concept_id, 
                                        b.procedure_concept_name, 
                                        b.concept_class_id_procedure,
                                        b.vocabulary_id_procedure,
                                        a.procedure_type_concept_id, 
                                        c.procedure_type_concept_name,
                                        c.concept_class_id_procedure_type,
                                        c.vocabulary_id_procedure_type,
                                        a.procedure_source_concept_id, 
                                        d.procedure_source_concept_name,
                                        d.concept_class_id_procedure_source,
                                        d.vocabulary_id_procedure_source,
                                        a.visit_occurrence_id,
                                        a.person_id,
                                        (CASE WHEN a.visit_occurrence_id IS NOT NULL THEN a.visit_occurrence_id %10
                                          WHEN a.person_id IS NOT NULL THEN a.person_id %10
                                          ELSE NULL END) as hospital_site
                        FROM omop_03082021.procedure_occurrence a
                        LEFT JOIN 
                        (SELECT concept_id as b_procedure_concept_id, 
                                concept_name as procedure_concept_name,
                                concept_class_id as concept_class_id_procedure,
                                vocabulary_id as vocabulary_id_procedure
                          FROM omop_03082021.concept) b
                          ON a.procedure_concept_id=b.b_procedure_concept_id
                        LEFT JOIN 
                        (SELECT concept_id as c_procedure_type_concept_id, 
                                concept_name as procedure_type_concept_name,
                                concept_class_id as concept_class_id_procedure_type,
                                vocabulary_id as vocabulary_id_procedure_type
                          FROM omop_03082021.concept) c
                        ON a.procedure_type_concept_id=c.c_procedure_type_concept_id
                        LEFT JOIN 
                        (SELECT concept_id as d_procedure_source_concept_id, 
                                concept_name as procedure_source_concept_name,
                                concept_class_id as concept_class_id_procedure_source,
                                vocabulary_id as vocabulary_id_procedure_source
                        FROM omop_03082021.concept) d
                        ON a.procedure_source_concept_id=d.d_procedure_source_concept_id")


copd_proc_tabulation <- dbGetQuery(copd, procedure_concepts_query)
coag_proc_tabulation<- dbGetQuery(coag, procedure_concepts_query)
vent_proc_tabulation <- dbGetQuery(vent, procedure_concepts_query)
news_proc_tabulation <- dbGetQuery(news2, procedure_concepts_query)

proc_tabulation = rbind(copd_proc_tabulation, coag_proc_tabulation, vent_proc_tabulation, news_proc_tabulation) %>% 
  distinct()  %>%
  mutate(hospital_site = case_when(hospital_site=hospital_site==4 ~ "UHB",
                                   hospital_site==6 ~ "UCLH"))

rm(copd_proc_tabulation, coag_proc_tabulation, vent_proc_tabulation, news_proc_tabulation)

proc_concept_table <- proc_tabulation %>%
                      group_by(procedure_concept_id,procedure_concept_name, hospital_site) %>%
                      dplyr::summarise(n=n())

proc_concept_table <- dcast(proc_concept_table, procedure_concept_name + procedure_concept_id~hospital_site)

proc_concept_table = proc_concept_table %>%
                        mutate(UHB=if_else(is.na(UHB)|UHB<=10,"<=10", as.character(UHB)),
                               UCLH = if_else(is.na(UCLH)|UCLH<=10,"<=10", as.character(UCLH)))

write.csv(proc_concept_table, "proc_concept_table_byTrust.csv", na="", row.names=FALSE)

proc_concept_vocab_table <- proc_tabulation %>%
                            group_by(vocabulary_id_procedure, hospital_site) %>%
                            dplyr::summarise(n=n())

proc_concept_vocab_table <- dcast(proc_concept_vocab_table,  vocabulary_id_procedure~hospital_site)

write.csv(proc_concept_vocab_table, "proc_concept_vocab_table_byTrust.csv", na="", row.names=FALSE)

procedure_type_concept_table <- proc_tabulation %>%
                                group_by(procedure_type_concept_id,procedure_type_concept_name, hospital_site) %>%
                                dplyr::summarise(n=n())

procedure_type_concept_table <- dcast(procedure_type_concept_table, procedure_type_concept_name + procedure_type_concept_id~hospital_site)

procedure_type_concept_table = procedure_type_concept_table %>%
                                  mutate(UHB=if_else(is.na(UHB)|UHB<=10,"<=10", as.character(UHB)),
                                         UCLH = if_else(is.na(UCLH)|UCLH<=10,"<=10", as.character(UCLH)))

write.csv(procedure_type_concept_table, "procedure_type_concept_table_byTrust.csv", na="", row.names=FALSE)

rm(proc_tabulation)
#Specimen
spec_concepts_query <- paste0("SELECT a.specimen_id,
                                        a.specimen_concept_id, 
                                        b.specimen_concept_name, 
                                        b.concept_class_id_specimen,
                                        b.vocabulary_id_specimen,
                                        a.anatomic_site_concept_id, 
                                        c.anatomic_concept_name,
                                        c.concept_class_id_anatomic,
                                        c.vocabulary_id_anatomic,
                                        a.person_id,
                                        a.person_id %10 as hospital_site
                        FROM omop_03082021.specimen a
                        LEFT JOIN 
                        (SELECT concept_id as b_specimen_concept_id,
                                concept_name as specimen_concept_name,
                                concept_class_id as concept_class_id_specimen,
                                vocabulary_id as vocabulary_id_specimen
                          FROM omop_03082021.concept) b
                          ON a.specimen_concept_id=b.b_specimen_concept_id
                        LEFT JOIN 
                        (SELECT concept_id as c_anatomic_site_id, 
                                concept_name as anatomic_concept_name,
                                concept_class_id as concept_class_id_anatomic,
                                vocabulary_id as vocabulary_id_anatomic
                          FROM omop_03082021.concept) c
                        ON a.anatomic_site_concept_id=c.c_anatomic_site_id")


copd_spec_tabulation <- dbGetQuery(copd, spec_concepts_query)
coag_spec_tabulation<- dbGetQuery(coag, spec_concepts_query)
vent_spec_tabulation <- dbGetQuery(vent, spec_concepts_query)
news_spec_tabulation <- dbGetQuery(news2, spec_concepts_query)

spec_tabulation = rbind(copd_spec_tabulation, coag_spec_tabulation, vent_spec_tabulation, news_spec_tabulation) %>% 
                  distinct() %>%
                  mutate(hospital_site = case_when(hospital_site=hospital_site==4 ~ "UHB",
                                                   hospital_site==6 ~ "UCLH"))

rm(copd_spec_tabulation, coag_spec_tabulation, vent_spec_tabulation, news_spec_tabulation)

specimen_concept_table <- spec_tabulation %>%
                            group_by(specimen_concept_id,specimen_concept_name, hospital_site) %>%
                            dplyr::summarise(n=n())

specimen_concept_table <- dcast(specimen_concept_table, specimen_concept_name + specimen_concept_id~hospital_site)

specimen_concept_table = specimen_concept_table %>%
                          mutate(UHB=if_else(is.na(UHB)|UHB<=10,"<=10", as.character(UHB)),
                                 UCLH = if_else(is.na(UCLH)|UCLH<=10,"<=10", as.character(UCLH)))

write.csv(specimen_concept_table, "specimen_concept_table_byTrust.csv", na="", row.names=FALSE)

specimen_concept_vocab_table <- spec_tabulation %>%
                                group_by(vocabulary_id_specimen, hospital_site) %>%
                                dplyr::summarise(n=n())

specimen_concept_vocab_table <- dcast(specimen_concept_vocab_table,  vocabulary_id_specimen~hospital_site)

specimen_concept_vocab_table = specimen_concept_vocab_table %>%
  mutate(UHB=if_else(is.na(UHB)|UHB<=10,"<=10", as.character(UHB)),
         UCLH = if_else(is.na(UCLH)|UCLH<=10,"<=10", as.character(UCLH)))

write.csv(specimen_concept_vocab_table, "specimen_concept_vocab_table_byTrust.csv", na="", row.names=FALSE)

anatomic_site_concept_table <- spec_tabulation %>%
                                group_by(anatomic_site_concept_id,anatomic_concept_name, hospital_site) %>%
                                dplyr::summarise(n=n())

anatomic_site_concept_table <- dcast(anatomic_site_concept_table, anatomic_concept_name + anatomic_site_concept_id~hospital_site)

anatomic_site_concept_table = anatomic_site_concept_table %>%
  mutate(anatomic_concept_name=ifelse(is.na(anatomic_concept_name), "NULL", as.character(anatomic_concept_name)),
         anatomic_site_concept_id=ifelse(is.na(as.character(anatomic_site_concept_id)), "NULL", as.character(anatomic_site_concept_id)),
    UHB=if_else(is.na(UHB)|UHB<=10,"<=10", as.character(UHB)),
         UCLH = if_else(is.na(UCLH)|UCLH<=10,"<=10", as.character(UCLH)))

write.csv(anatomic_site_concept_table, "anatomic_site_concept_table_byTrust.csv", na="", row.names=FALSE)


anatomic_site_concept_vocab_table <- spec_tabulation %>%
                                      group_by(vocabulary_id_anatomic, hospital_site) %>%
                                      dplyr::summarise(n=n())

anatomic_site_concept_vocab_table <- dcast(anatomic_site_concept_vocab_table, vocabulary_id_anatomic~hospital_site)

anatomic_site_concept_vocab_table = anatomic_site_concept_vocab_table %>%
  mutate(vocabulary_id_anatomic=ifelse(is.na(vocabulary_id_anatomic), "NULL", as.character(vocabulary_id_anatomic)),
         UHB=if_else(is.na(UHB)|UHB<=10,"<=10", as.character(UHB)),
         UCLH = if_else(is.na(UCLH)|UCLH<=10,"<=10", as.character(UCLH)))


write.csv(anatomic_site_concept_vocab_table, "anatomic_site_concept_vocab_table_byTrust.csv", na="", row.names=FALSE)

anatomic_site_concept_class_table <- spec_tabulation %>%
                                      group_by(concept_class_id_anatomic, hospital_site) %>%
                                      dplyr::summarise(n=n())

anatomic_site_concept_class_table <- dcast(anatomic_site_concept_class_table, concept_class_id_anatomic~hospital_site)

anatomic_site_concept_class_table = anatomic_site_concept_class_table %>%
                                      mutate(concept_class_id_anatomic=ifelse(is.na(concept_class_id_anatomic), "NULL", as.character(concept_class_id_anatomic)),
                                             UHB=if_else(is.na(UHB)|UHB<=10,"<=10", as.character(UHB)),
                                             UCLH = if_else(is.na(UCLH)|UCLH<=10,"<=10", as.character(UCLH)))

write.csv(anatomic_site_concept_class_table, "anatomic_site_concept_class_table_byTrust.csv", na="", row.names=FALSE)

#Visit occurrence
visit_occur_concepts_query <- paste0("SELECT a.visit_occurrence_id,
                                        a.visit_concept_id, 
                                        b.visit_concept_name, 
                                        b.concept_class_id_visit_concept,
                                        b.vocabulary_id_visit_concept,
                                        a.admitting_source_concept_id, 
                                        c.admitting_source_concept_name,
                                        c.concept_class_id_admitting_source,
                                        c.vocabulary_id_visit_admitting_source,
                                        a.discharge_to_concept_id,
                                        d.discharge_to_concept_name,
                                        d.concept_class_id_discharge_to,
                                        d.vocabulary_id_visit_discharge_to,
                                        a.visit_occurrence_id,
                                        a.person_id,
                                        (CASE WHEN a.visit_occurrence_id IS NOT NULL THEN a.visit_occurrence_id %10
                                          WHEN a.person_id IS NOT NULL THEN a.person_id %10
                                          ELSE NULL END) as hospital_site
                        FROM omop_03082021.visit_occurrence a
                        LEFT JOIN 
                        (SELECT concept_id as b_visit_concept, 
                                concept_name as visit_concept_name,
                                concept_class_id as concept_class_id_visit_concept,
                                vocabulary_id as vocabulary_id_visit_concept
                          FROM omop_03082021.concept) b
                          ON a.visit_concept_id=b.b_visit_concept
                        LEFT JOIN 
                        (SELECT concept_id as c_admitting_source_concept_id, 
                                concept_name as admitting_source_concept_name,
                                concept_class_id as concept_class_id_admitting_source,
                                vocabulary_id as vocabulary_id_visit_admitting_source
                          FROM omop_03082021.concept) c
                        ON a.admitting_source_concept_id=c.c_admitting_source_concept_id
                        LEFT JOIN
                        (SELECT concept_id as d_discharge_to_concept_id, 
                                concept_name as discharge_to_concept_name,
                                concept_class_id as concept_class_id_discharge_to,
                                vocabulary_id as vocabulary_id_visit_discharge_to
                          FROM omop_03082021.concept) d
                        ON a.discharge_to_concept_id=d.d_discharge_to_concept_id")


copd_visitocc_tabulation <- dbGetQuery(copd, visit_occur_concepts_query)
coag_visitocc_tabulation<- dbGetQuery(coag, visit_occur_concepts_query)
vent_visitocc_tabulation <- dbGetQuery(vent, visit_occur_concepts_query)
news_visitocc_tabulation <- dbGetQuery(news2, visit_occur_concepts_query)

visitocc_tabulation = rbind(copd_visitocc_tabulation, coag_visitocc_tabulation, vent_visitocc_tabulation, news_visitocc_tabulation) %>% 
  distinct() %>%
  mutate(hospital_site = case_when(hospital_site=hospital_site==4 ~ "UHB",
                                   hospital_site==6 ~ "UCLH"))

rm(copd_visitocc_tabulation, coag_visitocc_tabulation, vent_visitocc_tabulation, news_visitocc_tabulation)


visit_concept_table <- visitocc_tabulation %>%
                        group_by(visit_concept_id,visit_concept_name,hospital_site) %>%
                        dplyr::summarise(n=n())

visit_concept_table <- dcast(visit_concept_table, visit_concept_name + visit_concept_id~hospital_site)

visit_concept_table = visit_concept_table %>%
                      mutate(UHB=if_else(is.na(UHB)|UHB<=10,"<=10", as.character(UHB)),
                             UCLH = if_else(is.na(UCLH)|UCLH<=10,"<=10", as.character(UCLH)))

write.csv(visit_concept_table, "visit_concept_table_byTrust.csv", na="", row.names=FALSE)

admitting_concept_table <- visitocc_tabulation %>%
                          group_by(admitting_source_concept_id,admitting_source_concept_name, hospital_site) %>%
                          dplyr::summarise(n=n())

admitting_concept_table <- dcast(admitting_concept_table, admitting_source_concept_name + admitting_source_concept_id~hospital_site)

admitting_concept_table = admitting_concept_table %>%
  mutate(admitting_source_concept_id=ifelse(is.na(as.character(admitting_source_concept_id)),"0",as.character(admitting_source_concept_id)),
         admitting_source_concept_name=ifelse(is.na(admitting_source_concept_name), "NULL", as.character(admitting_source_concept_name)),
    UHB=if_else(is.na(UHB)|UHB<=10,"<=10", as.character(UHB)),
         UCLH = if_else(is.na(UCLH)|UCLH<=10,"<=10", as.character(UCLH)))

write.csv(admitting_concept_table, "admitting_concept_table_byTrust.csv", na="", row.names=FALSE)

discharge_concept_table <- visitocc_tabulation %>%
  group_by(discharge_to_concept_id,discharge_to_concept_name, hospital_site) %>%
  dplyr::summarise(n=n())


discharge_concept_table <- dcast(discharge_concept_table, discharge_to_concept_name + discharge_to_concept_id~hospital_site)

discharge_concept_table = discharge_concept_table %>%
  mutate(discharge_to_concept_name=ifelse(is.na(discharge_to_concept_name), "NULL", as.character(discharge_to_concept_name)),
         discharge_to_concept_id=ifelse(is.na(as.character(discharge_to_concept_id)), "0",as.character(discharge_to_concept_id)),
       UHB=if_else(is.na(UHB)|UHB<=10,"<=10", as.character(UHB)),
         UCLH = if_else(is.na(UCLH)|UCLH<=10,"<=10", as.character(UCLH)))

write.csv(discharge_concept_table, "discharge_concept_table_byTrust.csv", na="", row.names=FALSE)

#Visit_detail
visit_detail_concepts_query <- paste0("SELECT a.visit_detail_id,
                                        a.visit_detail_concept_id, 
                                        a.care_site_id,
                                        b.visit_detail_concept_name,
                                        b.concept_class_id_visit_detail,
                                        b.vocabulary_id__visit_detail,
                                        a.visit_occurrence_id,
                                        c.*,
                                        a.person_id,
                                        (CASE WHEN a.visit_occurrence_id IS NOT NULL THEN a.visit_occurrence_id %10
                                          WHEN a.person_id IS NOT NULL THEN a.person_id %10
                                          ELSE NULL END) as hospital_site
                        FROM omop_03082021.visit_detail a
                        LEFT JOIN 
                        (SELECT concept_id as b_visit_detail_concept,
                                concept_name as visit_detail_concept_name,
                                concept_class_id as concept_class_id_visit_detail,
                                vocabulary_id as vocabulary_id__visit_detail
                          FROM omop_03082021.concept) b
                          ON a.visit_detail_concept_id=b.b_visit_detail_concept
                        LEFT JOIN 
                        (SELECT care_site_id as care_site_id_c, *
                           FROM omop_03082021.care_site) c 
                        ON a.care_site_id=c.care_site_id_c")


copd_visitdet_tabulation <- dbGetQuery(copd, visit_detail_concepts_query)
coag_visitdet_tabulation<- dbGetQuery(coag, visit_detail_concepts_query)
vent_visitdet_tabulation <- dbGetQuery(vent, visit_detail_concepts_query)
news_visitdet_tabulation <- dbGetQuery(news2, visit_detail_concepts_query)

visitdet_tabulation = rbind(copd_visitdet_tabulation, coag_visitdet_tabulation, vent_visitdet_tabulation, news_visitdet_tabulation) %>% 
  distinct() %>%
  mutate(hospital_site = case_when(hospital_site=hospital_site==4 ~ "UHB",
                                   hospital_site==6 ~ "UCLH"))

rm(copd_visitdet_tabulation, coag_visitdet_tabulation, vent_visitdet_tabulation, news_visitdet_tabulation)


visit_det_concept_table <- visitdet_tabulation %>%
                          group_by(visit_detail_concept_id,visit_detail_concept_name, hospital_site) %>%
                          dplyr::summarise(n=n())



visit_det_concept_table <- dcast(visit_det_concept_table, visit_detail_concept_name + visit_detail_concept_id~hospital_site)

visit_det_concept_table = visit_det_concept_table %>%
  mutate(UHB=if_else(is.na(UHB)|UHB<=10,"<=10", as.character(UHB)),
         UCLH = if_else(is.na(UCLH)|UCLH<=10,"<=10", as.character(UCLH)))

write.csv(visit_det_concept_table, "visit_det_concept_table_byTrust.csv", na="", row.names=FALSE)


visit_det_caresite_table <- visitdet_tabulation %>%
  group_by(care_site_id,care_site_name, hospital_site) %>%
  dplyr::summarise(n=n())



visit_det_caresite_table <- dcast(visit_det_caresite_table, care_site_id + care_site_name~hospital_site)

visit_det_caresite_table = visit_det_caresite_table %>%
  mutate(UHB=if_else(is.na(UHB)|UHB<=10,"<=10", as.character(UHB)),
         UCLH = if_else(is.na(UCLH)|UCLH<=10,"<=10", as.character(UCLH)))

write.csv(visit_det_caresite_table, "visit_det_caresite_table_byTrust.csv", na="", row.names=FALSE)

#Person
person_query_concepts <- paste0("SELECT a.person_id, 
                                  a.gender_concept_id,
                                  b.gender_concept_name,
                                  b.concept_class_id_gender,
                                  b.vocabulary_id_gender,
                                  a.race_concept_id,
                                  c.race_concept_name,
                                  c.concept_class_id_race,
                                  c.vocabulary_id_race,
                                  a.person_id,
                                  a.person_id %10 as hospital_site
                        FROM omop_03082021.person a
                        LEFT JOIN
                        (SELECT concept_id as b_gender_concept_id, 
                                concept_name as gender_concept_name,
                                concept_class_id as concept_class_id_gender,
                                vocabulary_id as vocabulary_id_gender
                          FROM omop_03082021.concept) b
                          ON a.gender_concept_id=b.b_gender_concept_id
                          LEFT JOIN
                          (SELECT concept_id as c_race_concept_id, 
                                  concept_name as race_concept_name,
                                   concept_class_id as concept_class_id_race,
                                   vocabulary_id as vocabulary_id_race
                          FROM omop_03082021.concept) c
                          ON a.race_concept_id=c.c_race_concept_id")

copd_person_tabulation <- dbGetQuery(copd, person_query_concepts)
coag_person_tabulation<- dbGetQuery(coag, person_query_concepts)
vent_person_tabulation <- dbGetQuery(vent, person_query_concepts)
news_person_tabulation <- dbGetQuery(news2, person_query_concepts)


person_tabulation = rbind(copd_person_tabulation, coag_person_tabulation, vent_person_tabulation, news_person_tabulation) %>% 
  distinct() %>%
  mutate(hospital_site = case_when(hospital_site=hospital_site==4 ~ "UHB",
                                   hospital_site==6 ~ "UCLH"))

rm(copd_person_tabulation, coag_person_tabulation, vent_person_tabulation, news_person_tabulation)

gender_concept_table <- person_tabulation %>%
  group_by(gender_concept_id,gender_concept_name, hospital_site) %>%
  dplyr::summarise(n=n())


gender_concept_table <- dcast(gender_concept_table, gender_concept_name + gender_concept_id~hospital_site)

gender_concept_table = gender_concept_table %>%
  mutate(UHB=if_else(is.na(UHB)|UHB<=10,"<=10", as.character(UHB)),
         UCLH = if_else(is.na(UCLH)|UCLH<=10,"<=10", as.character(UCLH)))

write.csv(gender_concept_table, "gender_concept_table_byTrust.csv", na="", row.names=FALSE)

race_concept_table <- person_tabulation %>%
  group_by(race_concept_id,race_concept_name, hospital_site) %>%
  dplyr::summarise(n=n())

race_concept_table <- dcast(race_concept_table, race_concept_name + race_concept_id~hospital_site)

race_concept_table = race_concept_table %>%
  mutate(UHB=if_else(is.na(UHB)|UHB<=10,"<=10", as.character(UHB)),
         UCLH = if_else(is.na(UCLH)|UCLH<=10,"<=10", as.character(UCLH)))

write.csv(race_concept_table, "race_concept_table_byTrust.csv", na="", row.names=FALSE)

#Death
death_query_concepts <- paste0("SELECT a.person_id,
                                       a.death_type_concept_id,
                                       b.death_type_concept_name,
                                       a.cause_concept_id,
                                       c.cause_concept_name,
                                       a.cause_source_concept_id,
                                       d.cause_source_concept_name
                        FROM omop_03082021.death a
                        LEFT JOIN
                        (SELECT concept_id as b_death_type_concept_id, 
                                concept_name as death_type_concept_name
                          FROM omop_03082021.concept) b
                          ON a.death_type_concept_id=b.b_death_type_concept_id
                          LEFT JOIN
                          (SELECT concept_id as c_cause_concept_id, 
                                  concept_name as cause_concept_name
                          FROM omop_03082021.concept) c
                          ON a.cause_concept_id=c.c_cause_concept_id
                               LEFT JOIN
                          (SELECT concept_id as d_cause_source_concept_id, 
                                  concept_name as cause_source_concept_name
                          FROM omop_03082021.concept) d
                               ON a.cause_source_concept_id=d.d_cause_source_concept_id")

copd_death_tabulation <- dbGetQuery(copd, death_query_concepts)
coag_death_tabulation<- dbGetQuery(coag, death_query_concepts)
vent_death_tabulation <- dbGetQuery(vent, death_query_concepts)
news_death_tabulation <- dbGetQuery(news2, death_query_concepts)


death_tabulation = rbind(copd_death_tabulation, coag_death_tabulation, vent_death_tabulation, news_death_tabulation) %>% 
  distinct()
rm(copd_death_tabulation, coag_death_tabulation, vent_death_tabulation, news_death_tabulation)

colnames(death_tabulation)

death_type_concept_table <- death_tabulation %>%
  group_by(death_type_concept_id,death_type_concept_name) %>%
  dplyr::summarise(n=n())

#Nothing - all nulls

death_cause_concept_table <- death_tabulation %>%
  group_by(cause_concept_id,cause_concept_name) %>%
  dplyr::summarise(n=n())


death_cause__source_concept_table <- death_tabulation %>%
  group_by(cause_source_concept_id,cause_source_concept_name) %>%
  dplyr::summarise(n=n())

#no concept_ids in the death table are populated with anything other than no matching concept id or 0...

meas_q <- paste0("SELECT a.measurement_id,
                         a.measurement_concept_id,
                         b.measurement_concept_name,
                         b.concept_class_id_measurement,
                         b.vocabulary_id_measurement,
                         a.visit_occurrence_id,
                         a.person_id,
                          (CASE WHEN a.visit_occurrence_id IS NOT NULL THEN a.visit_occurrence_id %10
                                WHEN a.person_id IS NOT NULL THEN a.person_id %10
                                ELSE NULL END) as hospital_site
                      FROM omop_03082021.measurement a
                      LEFT JOIN 
                      (SELECT concept_id as measurement_id,
                              concept_name as measurement_concept_name,
                              concept_class_id as concept_class_id_measurement,
                                vocabulary_id as vocabulary_id_measurement
                       FROM omop_03082021.concept) b
                      ON a.measurement_concept_id=b.measurement_id")


copd_meas_q <- dbGetQuery(copd, meas_q)
coag_meas_q <- dbGetQuery(coag, meas_q)
vent_meas_q <- dbGetQuery(vent, meas_q)
news2_meas_q <- dbGetQuery(news2, meas_q)

omop_meas_all <- rbind(copd_meas_q, coag_meas_q) %>% 
                  distinct()

rm(copd_meas_q,coag_meas_q)

omop_meas_all <- rbind(omop_meas_all, vent_meas_q) %>% 
                  distinct()

rm(vent_meas_q)

omop_meas_all <- rbind(omop_meas_all, news2_meas_q) %>% 
  distinct()

rm(news2_meas_q)

omop_meas_all= omop_meas_all %>%  
  mutate(hospital_site = case_when(hospital_site=hospital_site==4 ~ "UHB",
                                   hospital_site==6 ~ "UCLH"))


measurements_concept_table <- omop_meas_all %>%
  group_by(measurement_concept_id,measurement_concept_name, hospital_site) %>%
  dplyr::summarise(n=n())

measurements_concept_table <- dcast(measurements_concept_table, measurement_concept_name + measurement_concept_id~hospital_site)

measurements_concept_table = measurements_concept_table %>%
  mutate(UHB=if_else(is.na(UHB)|UHB<=10,"<=10", as.character(UHB)),
         UCLH = if_else(is.na(UCLH)|UCLH<=10,"<=10", as.character(UCLH)))

write.csv(measurements_concept_table,"measurements_concept_table_byTrust.csv", na="", row.names=F)


measurements_concept_vocab_table <- omop_meas_all %>%
  group_by(vocabulary_id_measurement, hospital_site) %>%
  dplyr::summarise(n=n())

measurements_concept_vocab_table <- dcast(measurements_concept_vocab_table,  vocabulary_id_measurement~hospital_site)

measurements_concept_vocab_table = measurements_concept_vocab_table %>%
  mutate(UHB=if_else(is.na(UHB)|UHB<=10,"<=10", as.character(UHB)),
         UCLH = if_else(is.na(UCLH)|UCLH<=10,"<=10", as.character(UCLH)))

write.csv(measurements_concept_vocab_table,"measurements_concept_vocab_table_byTrust.csv", na="", row.names=F)


measurements_concept_class_table <- omop_meas_all %>%
  group_by(concept_class_id_measurement, hospital_site) %>%
  dplyr::summarise(n=n())

measurements_concept_class_table <- dcast(measurements_concept_class_table,  concept_class_id_measurement~hospital_site)

measurements_concept_class_table = measurements_concept_class_table %>%
  mutate(UHB=if_else(is.na(UHB)|UHB<=10,"<=10", as.character(UHB)),
         UCLH = if_else(is.na(UCLH)|UCLH<=10,"<=10", as.character(UCLH)))

write.csv(measurements_concept_class_table,"measurements_concept_class_table_byTrust.csv", na="", row.names=F)


meas_q <- paste0("SELECT a.*,
                         b.measurement_concept_name,
                         b.concept_class_id_measurement,
                         b.vocabulary_id_measurement,
                          (CASE WHEN a.visit_occurrence_id IS NOT NULL THEN a.visit_occurrence_id %10
                                WHEN a.person_id IS NOT NULL THEN a.person_id %10
                                ELSE NULL END) as hospital_site
                      FROM 
                      (SELECT *
                      FROM omop_03082021.measurement
                      WHERE measurement_concept_id IN ('4313591', '4108138','4154772')) a
                      LEFT JOIN 
                      (SELECT concept_id as measurement_id,
                              concept_name as measurement_concept_name,
                              concept_class_id as concept_class_id_measurement,
                                vocabulary_id as vocabulary_id_measurement
                       FROM omop_03082021.concept) b
                      ON a.measurement_concept_id=b.measurement_id")


copd_meas_q <- dbGetQuery(copd, meas_q)
coag_meas_q <- dbGetQuery(coag, meas_q)
vent_meas_q <- dbGetQuery(vent, meas_q)
news2_meas_q <- dbGetQuery(news2, meas_q)

omop_meas_all <- rbind(copd_meas_q, coag_meas_q, vent_meas_q) %>% 
                 distinct()

omop_meas_all <- omop_meas_all %>%  
                 mutate(hospital_site = case_when(hospital_site=hospital_site==4 ~ "UHB",
                                   hospital_site==6 ~ "UCLH"))

omop_care$visit_detail_id_char <- as.character(omop_care$visit_detail_id)
omop_meas_all$visit_detail_id_char <- as.character(omop_meas_all$visit_detail_id)

sum(is.na(omop_meas_all$visit_detail_id_char))
sum(is.na(as.character(omop_meas_all$visit_occurrence_id)))

omop_meas_all_care <- inner_join(omop_meas_all, omop_care, by=c("visit_occurrence_id"))

omop_meas_all_carefilter <- omop_meas_all_care %>%
  filter(patient_days>=1) %>%
  filter(measurement_datetime>=visit_detail_start_datetime, measurement_datetime<=visit_detail_end_datetime)

omop_meas_all_carefilter$day <- ceiling(as.numeric((omop_meas_all_carefilter$measurement_datetime-omop_meas_all_carefilter$visit_detail_start_datetime))/ 86400)

NewMeasures <- omop_meas_all_carefilter %>%
  group_by(visit_occurrence_id, visit_detail_id.y, care_site_id, hospital_site.x,day, patient_days) %>%
  summarise(count=n())

NewMeasures$hospital_site <- ifelse(as.character(NewMeasures$visit_detail_id %% 10)=="4", "UHB", "UCLH")
table(omop_meas_all$hospital_site, omop_meas_all$measurement_concept_name)
omop_meas_all$person_id_num <- as.numeric(omop_meas_all$person_id)

omop_meas_all_UHB <- omop_meas_all %>%
                    filter(hospital_site=="UHB") %>%
                    arrange(person_id_num, measurement_datetime)

measurement <- read.csv("measurements_filtered.csv") %>% 
  rename(measurement_concept_id=concept_id)

#type for the summary and it is easier to alise these measures in a separate query.
RRQuery<- paste0("SELECT * FROM
                            (SELECT visit_occurrence_id, 
                                    visit_occurrence_id %10 as hospital_site, 
                                    day, 
                                    length_of_stay, 
                                    visit_detail_id, 
                                    care_site_id, 
                                    measurement_concept AS measurement_concept_id,
                                    'RespiratoryRate' AS measurement_concept_name, 
                                    COUNT(measurement_concept) FROM
                            (SELECT visit_occurrence_id,
                                    visit_detail_id,
                                    care_site_id,
                                    measurement_datetime,
                                    visit_detail_end_datetime,
                                    visit_detail_start_datetime,
                                   (CASE 
                                  WHEN CEILING((DATE_PART('day', measurement_datetime::timestamp - visit_detail_start_datetime::timestamp))  +
                                      (DATE_PART('hour', measurement_datetime::timestamp - visit_detail_start_datetime::timestamp) /24 + 
                                      DATE_PART('minute', measurement_datetime::timestamp - visit_detail_start_datetime::timestamp) / 1440))=0 THEN 1
                                  ELSE CEILING((DATE_PART('day', measurement_datetime::timestamp - visit_detail_start_datetime::timestamp))  +
                                      (DATE_PART('hour', measurement_datetime::timestamp - visit_detail_start_datetime::timestamp) /24 + 
                                      DATE_PART('minute', measurement_datetime::timestamp - visit_detail_start_datetime::timestamp) / 1440)) END) AS day,
                                   (DATE_PART('day', visit_detail_end_datetime::timestamp - visit_detail_start_datetime::timestamp) +
                                  DATE_PART('hour', visit_detail_end_datetime::timestamp - visit_detail_start_datetime::timestamp) /24 + 
                                  DATE_PART('minute', visit_detail_end_datetime::timestamp - visit_detail_start_datetime::timestamp) / 1440) AS length_of_stay, 
                                  measurement_concept FROM
                            (SELECT visit_occurrence_id, 
                                    visit_detail_start_datetime, 
                                    visit_detail_end_datetime, 
                                    visit_detail_id,
                                    care_site_id FROM omop_03082021.visit_detail
                            WHERE (DATE_PART('day', visit_detail_end_datetime::timestamp - visit_detail_start_datetime::timestamp) +
                            DATE_PART('hour', visit_detail_end_datetime::timestamp - visit_detail_start_datetime::timestamp) /24 + 
                            DATE_PART('minute', visit_detail_end_datetime::timestamp - visit_detail_start_datetime::timestamp) / 1440) >=1) a
                            LEFT JOIN
                            (SELECT visit_occurrence_id, 
                                    measurement_datetime,
                                   'RespiratoryRate' AS measurement_concept 
                                    FROM omop_03082021.measurement
                            	   WHERE measurement_concept_id IN (", 
                                  paste0(paste0("'", measurement %>% filter(category %in% c("resp")) %>% pull(measurement_concept_id), "'", collapse=",")), ") AND visit_occurrence_id IS NOT NULL
                                GROUP BY visit_occurrence_id,
                                measurement_datetime,
                                measurement_concept) b
                            USING (visit_occurrence_id)) c
                            WHERE ((measurement_datetime::timestamp >= visit_detail_start_datetime::timestamp) AND (measurement_datetime::timestamp <= visit_detail_end_datetime::timestamp))
                            GROUP BY visit_occurrence_id, 
                                     visit_detail_id, 
                                     day,
                                     length_of_stay, 
                                     measurement_concept,  
                                     care_site_id) d")


#This query pulls all measurement records for the measurements read in the csv file above
RRQuery <- paste("SELECT * 
                  FROM
                        (SELECT visit_occurrence_id, 
                                visit_occurrence_id %10 as hospital_site, 
                                day,
                                length_of_stay, 
                                visit_detail_id, 
                                care_site_id, 
                                measurement_concept_id, 
                                COUNT(measurement_concept_id) FROM
                        (SELECT visit_occurrence_id,
                                visit_detail_id,
                                care_site_id,
                                measurement_datetime,
                                visit_detail_end_datetime,
                                visit_detail_start_datetime,
                                (CASE 
                                  WHEN CEILING((DATE_PART('day', measurement_datetime::timestamp - visit_detail_start_datetime::timestamp))  +
                                      (DATE_PART('hour', measurement_datetime::timestamp - visit_detail_start_datetime::timestamp) /24 + 
                                      DATE_PART('minute', measurement_datetime::timestamp - visit_detail_start_datetime::timestamp) / 1440))=0 THEN 1
                                  ELSE CEILING((DATE_PART('day', measurement_datetime::timestamp - visit_detail_start_datetime::timestamp))  +
                                      (DATE_PART('hour', measurement_datetime::timestamp - visit_detail_start_datetime::timestamp) /24 + 
                                      DATE_PART('minute', measurement_datetime::timestamp - visit_detail_start_datetime::timestamp) / 1440)) END) AS day,
                               (DATE_PART('day', visit_detail_end_datetime::timestamp - visit_detail_start_datetime::timestamp) +
                              DATE_PART('hour', visit_detail_end_datetime::timestamp - visit_detail_start_datetime::timestamp) /24 + 
                              DATE_PART('minute', visit_detail_end_datetime::timestamp - visit_detail_start_datetime::timestamp) / 1440) AS length_of_stay, measurement_concept_id FROM
                        (SELECT visit_occurrence_id, 
                                visit_detail_start_datetime, 
                                visit_detail_end_datetime, 
                                visit_detail_id,
                                care_site_id FROM omop_03082021.visit_detail
                        WHERE (DATE_PART('day', visit_detail_end_datetime::timestamp - visit_detail_start_datetime::timestamp) +
                        DATE_PART('hour', visit_detail_end_datetime::timestamp - visit_detail_start_datetime::timestamp) /24 + 
                        DATE_PART('minute', visit_detail_end_datetime::timestamp - visit_detail_start_datetime::timestamp) / 1440) >=1) a
                        LEFT JOIN
                        (SELECT visit_occurrence_id, 
                                measurement_datetime, 
                                measurement_concept_id 
                                FROM omop_03082021.measurement
                        	   WHERE measurement_concept_id IN ('4313591','4108138','4154772')) AND visit_occurrence_id IS NOT NULL) b
                        USING (visit_occurrence_id)) c
                        WHERE ((measurement_datetime::timestamp >= visit_detail_start_datetime::timestamp) AND (measurement_datetime::timestamp <= visit_detail_end_datetime::timestamp))
                        GROUP BY visit_occurrence_id, 
                                 visit_detail_id, 
                                 day, 
                                 length_of_stay, 
                                 measurement_concept_id,  
                                 care_site_id) d
                        LEFT JOIN
                        (SELECT concept_id as measurement_concept_id, concept_name as measurement_concept_name FROM omop_03082021.concept) e
                        USING (measurement_concept_id)")
# RRQuery <-  paste("SELECT * 
#                    FROM
#                             (SELECT visit_occurrence_id, 
#                                     visit_occurrence_id %10 as hospital_site, 
#                                     day, 
#                                     length_of_stay, 
#                                     visit_detail_id, 
#                                     care_site_id, 
#                                     measurement_concept AS measurement_concept_id,
#                                     'RR' AS measurement_concept_name, 
#                                     COUNT(measurement_concept) FROM
#                             (SELECT visit_occurrence_id,
#                                     visit_detail_id,
#                                     care_site_id,
#                                     measurement_datetime,
#                                     visit_detail_end_datetime,
#                                     visit_detail_start_datetime,
#                                    (CASE 
#                                   WHEN CEILING((DATE_PART('day', measurement_datetime::timestamp - visit_detail_start_datetime::timestamp))  +
#                                       (DATE_PART('hour', measurement_datetime::timestamp - visit_detail_start_datetime::timestamp) /24 + 
#                                       DATE_PART('minute', measurement_datetime::timestamp - visit_detail_start_datetime::timestamp) / 1440))=0 THEN 1
#                                   ELSE CEILING((DATE_PART('day', measurement_datetime::timestamp - visit_detail_start_datetime::timestamp))  +
#                                       (DATE_PART('hour', measurement_datetime::timestamp - visit_detail_start_datetime::timestamp) /24 + 
#                                       DATE_PART('minute', measurement_datetime::timestamp - visit_detail_start_datetime::timestamp) / 1440)) END) AS day,
#                                    (DATE_PART('day', visit_detail_end_datetime::timestamp - visit_detail_start_datetime::timestamp) +
#                                   DATE_PART('hour', visit_detail_end_datetime::timestamp - visit_detail_start_datetime::timestamp) /24 + 
#                                   DATE_PART('minute', visit_detail_end_datetime::timestamp - visit_detail_start_datetime::timestamp) / 1440) AS length_of_stay, 
#                                   measurement_concept FROM
#                             (SELECT visit_occurrence_id, 
#                                     visit_detail_start_datetime, 
#                                     visit_detail_end_datetime, 
#                                     visit_detail_id,
#                                     care_site_id FROM omop_03082021.visit_detail
#                             WHERE (DATE_PART('day', visit_detail_end_datetime::timestamp - visit_detail_start_datetime::timestamp) +
#                             DATE_PART('hour', visit_detail_end_datetime::timestamp - visit_detail_start_datetime::timestamp) /24 + 
#                             DATE_PART('minute', visit_detail_end_datetime::timestamp - visit_detail_start_datetime::timestamp) / 1440) >=1) a
#                             LEFT JOIN
#                             (SELECT 
#                                     measurement_datetime,
#                                     visit_detail_id,
#                                     'RR' AS measurement_concept 
#                                     FROM omop_03082021.measurement
#                             	  WHERE (measurement_concept_id IN ('4313591','4108138','4154772')) AND (visit_detail_id IS NOT NULL)
#                                 GROUP BY 
#                                           visit_detail_id,
#                                           measurement_datetime,
#                                           measurement_concept) b
#                             USING (visit_detail_id)) c
#                             WHERE ((measurement_datetime::timestamp >= visit_detail_start_datetime::timestamp) AND (measurement_datetime::timestamp <= visit_detail_end_datetime::timestamp))
#                             GROUP BY visit_occurrence_id, 
#                                      visit_detail_id, 
#                                      day,
#                                      length_of_stay, 
#                                      measurement_concept,  
#                                      care_site_id) d")

copd_meas_q_rr <- dbGetQuery(copd, RRQuery)
coag_meas_q_rr <- dbGetQuery(coag, RRQuery)
vent_meas_q_rr <- dbGetQuery(vent, RRQuery)
news2_meas_q_rr <- dbGetQuery(news2, RRQuery)

omop_meas_all_rr <- rbind(copd_meas_q_rr, coag_meas_q_rr, vent_meas_q_rr, news2_meas_q_rr) %>% 
                    distinct()

omop_meas_all_rr$visit_detail_id_char <- as.character(omop_meas_all_rr$visit_detail_id)
omop_meas_all_rr$care_site_id_num <- as.numeric(omop_meas_all_rr$care_site_id)

CheckRR <- omop_meas_all_rr %>%
  group_by(visit_detail_id_char) %>%
  dplyr::summarise(n=n(), los=max(floor(length_of_stay)), care_site=max(care_site_id_num)) %>%
  distinct()
CheckRR$Diff <- CheckRR$n - CheckRR$los
#This is the query used to extract COVID cases
covid_pcr_query <- paste("SELECT visit_occurrence_id 
                        FROM
                        (SELECT visit_occurrence_id,
                                measurement_id AS fact_id_1 
                                FROM omop_03082021.measurement
                                WHERE (measurement_concept_id=37310255)
                                AND (value_as_concept_id=37310282)) a
                        INNER JOIN
                        (SELECT fact_id_1, 
                              fact_id_2 as specimen_id 
                              FROM omop_03082021.fact_relationship
                        WHERE domain_concept_id_1=21 AND domain_concept_id_2=36) b
                        USING (fact_id_1)
                        INNER JOIN
                        (SELECT specimen_id, 
                                specimen_datetime 
                                FROM omop_03082021.specimen) c
                        USING (specimen_id)
                        INNER JOIN
                        (SELECT visit_occurrence_id, 
                                visit_start_datetime, 
                                visit_end_datetime
                                FROM omop_03082021.visit_occurrence) d
                        USING (visit_occurrence_id)
                        WHERE (specimen_datetime >=visit_start_datetime - INTERVAL'14 day' AND specimen_datetime <=visit_end_datetime) 
                        OR (specimen_datetime >=visit_start_datetime - INTERVAL'14 day' AND visit_end_datetime IS NULL)")

#Confirmed/suspected COVID-19 Query
covid_obs_all_query <- paste("SELECT visit_occurrence_id 
                              FROM
                              (SELECT * FROM omop_03082021.condition_occurrence
                              WHERE condition_concept_id IN (45590872, 703441, 
                              37310287, 45604597, 37311060, 703440, 37310282,
                              439676, 45585955, 37311061, 45756093, 45756094, 
                              320651, 37310268)) a
                              INNER JOIN (SELECT visit_start_datetime,
                                                 visit_end_datetime, 
                                                 visit_occurrence_id 
                                                 FROM omop_03082021.visit_occurrence) b
                              USING (visit_occurrence_id)
                              WHERE (condition_start_datetime >=visit_start_datetime - INTERVAL'14 day' AND condition_start_datetime <=visit_end_datetime) 
                              OR (condition_start_datetime >=visit_start_datetime - INTERVAL'14 day' AND visit_end_datetime IS NULL)")


#Run PCR Only Queries - distinct() is used to remove duplicates
copd_covid_pcr <- dbGetQuery(copd, covid_pcr_query) %>% 
  distinct()

coag_covid_pcr <- dbGetQuery(coag, covid_pcr_query) %>% 
  distinct()

vent_covid_pcr <- dbGetQuery(vent, covid_pcr_query) %>% 
  distinct()

news2_covid_pcr <- dbGetQuery(news2, covid_pcr_query) %>% 
  distinct()

#Append all of the cases from all of DECOVID's research question databases, and remove duplicates using distinct()
omop_covid_pcr <-   rbind(copd_covid_pcr, coag_covid_pcr, vent_covid_pcr, news2_covid_pcr) %>% 
  distinct()


#Here, the COVID-19 cases based on clinical diagnoses (suspected and confirmed) are appended to the PCR only cases - again, distinct()
#is used to remove duplicates
copd_covid_all <- copd_covid_pcr %>% 
  rbind(dbGetQuery(copd, covid_obs_all_query)) %>% 
  distinct()

coag_covid_all <- coag_covid_pcr %>% 
  rbind(dbGetQuery(coag, covid_obs_all_query)) %>% 
  distinct()

vent_covid_all <- vent_covid_pcr %>% 
  rbind(dbGetQuery(vent, covid_obs_all_query)) %>% 
  distinct()

news2_covid_all <- news2_covid_pcr %>% 
  rbind(dbGetQuery(news2, covid_obs_all_query)) %>% 
  distinct()

#Append all of the cases from all of DECOVID's research question databases, and remove duplicates using distinct()
omop_covid_all <-   rbind(copd_covid_all, coag_covid_all, vent_covid_all, news2_covid_all) %>% 
  distinct()

#remove individual research question dataframes
rm(copd_covid_all, coag_covid_all, vent_covid_all, news2_covid_all,
   copd_covid_pcr, coag_covid_pcr, vent_covid_pcr, news2_covid_pcr)

#Before proceeding, specify the COVID-19 case type the data summaries should be based on.
#In the DECOVID Data Descriptor paper, the omop_covid_all cases are used.
#However, the omop_covid_pcr can be assigned to covid_case_type instead.
covid_case_type <- omop_covid_all

################################################
####### Table 1 - Measurements/Obs data ########
################################################

#Revised Becki's original queries to query visit detail instead. What this query
#does is retrieves all records in the visit_detail table that have a measurement
#take place between the visit_detail interval (inclusive)

#Very important to note that a measurement datetime can be equal to visit_detail start_datetime 
#(same for visit_occurrence start_datetime), and ceiling 0 would result in 0, although we want this to be 1
#whether we are looking at hours or days. A case when statement has been included in the code
#to account for this.

#Care site query with the # of days in each visit detail record from visit_detail table
#This is the only we can calculate the number patient days in each level of care
care_site_query_measurments <- paste0("SELECT * ,
                                      visit_occurrence_id %10 as hospital_site,
                                      (DATE_PART('day', visit_detail_end_datetime::timestamp - visit_detail_start_datetime::timestamp) +
                                       DATE_PART('hour', visit_detail_end_datetime::timestamp - visit_detail_start_datetime::timestamp) /24 + 
                                       DATE_PART('minute', visit_detail_end_datetime::timestamp - visit_detail_start_datetime::timestamp) / 1440) AS patient_days
                                       FROM omop_03082021.visit_detail")

copd_care_measurments <- dbGetQuery(copd, care_site_query_measurments)

coag_care_measurments <- dbGetQuery(coag, care_site_query_measurments)

vent_care_measurments <- dbGetQuery(vent, care_site_query_measurments)

news2_care_measurments <- dbGetQuery(news2, care_site_query_measurments)

#Append all four research questions, remove duplicates
omop_care <-  rbind(copd_care_measurments, coag_care_measurments, vent_care_measurments, news2_care_measurments) %>% 
  distinct() %>%
  mutate(care_site_id=as.character(care_site_id),
         covid=ifelse(visit_occurrence_id %in% covid_case_type$visit_occurrence_id, "Yes", "No"),
         hospital_site=case_when(
           hospital_site==4 ~ "UHB",
           hospital_site==6 ~ "UCLH"))

#Let's make the category a character, and code the blood pressure
#category, as it is NA. In the final step, we make category a factor.


#Converting other relevant bigint fields to character.
omop_meas_all_rr$care_site_id_char = as.character(omop_meas_all_rr$care_site_id)

#Level 2/3 Care
omop_meas_all_rr_l2l3 <- omop_meas_all_rr %>% 
  filter(care_site_id_char %in% c("1","2","3","15","20","21","22"))
NewMeasures$care_site_id
CheckRRl2l3 <- NewMeasures %>%
  filter(care_site_id %in% c("1","2","3","15","20","21","22")) %>%
  filter(day <= floor(patient_days)) 
CheckRRl2l3$visit_occurrence_id <- as.character(CheckRRl2l3$visit_occurrence_id)

CheckRRl2l3$covid <- if_else(CheckRRl2l3$visit_occurrence_id %in% as.character(omop_covid_all$visit_occurrence_id), "Yes", "No")

CheckRRl2l3 %>%
  filter(day <= floor(patient_days)) %>%
  group_by(hospital_site.x, covid) %>%
  summarise(days=n(), count=sum(count))
#make level 2 and 3 copy of omop_care to get total 24-hour periods.
omop_care_l2_l3_any_visit_type <- omop_care %>% 
  filter(care_site_id %in% c("1","2","3","15","20","21","22"))  %>%
  filter(patient_days>=1)

#Calculate number of completed 24 hour windows in visit detail for only level 2 and 3 care types.
#for mean and % missing.
n_complete_windows_all_l2_l3_any_visit <- omop_care_l2_l3_any_visit_type %>% 
                                            group_by(hospital_site, covid) %>%
                                            summarise(total_days=sum(floor(patient_days)))

omop_meas_all_rr_l2l3$hospital_site <- ifelse(as.character(omop_meas_all_rr_l2l3$hospital_site)=="4", "UHB", "UCLH")
table()
#Calculate mean and % missing for all measurements


omop_count_all_l2_l3_any <- omop_meas_all_rr_l2l3 %>%
  filter(day <= floor(length_of_stay)) %>%
  mutate(count=as.numeric(count)) %>%
  group_by( measurement_concept_name, hospital_site) %>% summarise(days_for_calc=n(),sum=sum(count)) %>%
  left_join(n_complete_windows_all_l2_l3_any_visit, by=c("hospital_site")) %>%
  mutate(mean = signif(sum/total_days, 2), missing=round(((total_days-days_for_calc)/total_days)*100, 1)) 

RR_check <- omop_meas_all_rr_l2l3 %>%
            filter(measurement_concept_name %in% c("Rate of spontaneous respiration", "Respiratory rate", "Ventilator rate" ))
#Write mean and missingness file
rbind(omop_count_all_l2_l3_any %>%
        dcast(measurement_concept_name ~ hospital_site, value.var=c("mean")) %>%
        mutate("Summary"=rep("mean")), omop_count_all_l2_l3_any %>% dcast(measurement_concept_name ~ hospital_site, value.var=c("missing")) %>%
        mutate("Summary"=rep("% missing"))) %>% arrange(measurement_concept_name) %>%
  write.csv("omop_measure_mean_missing_l2_l3_anyvisittype_RR.csv", row.names=FALSE, na="")




  