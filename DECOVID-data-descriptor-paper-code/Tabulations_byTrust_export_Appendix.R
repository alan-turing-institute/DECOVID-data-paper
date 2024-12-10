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

source("common-functions.R")
source("common-database.R")
source("common-queries.R")

# Queries to create tables of ids by trust

# Tabulations
drug_concept_id_query <- paste0(
  "SELECT
     a.drug_exposure_id,
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
   FROM drug_exposure a
    LEFT JOIN
    (SELECT concept_id as b_drug_concept_id,
           concept_name as drug_concept_name,
           concept_class_id as concept_class_id_drug,
           vocabulary_id as vocabulary_id_drug
      FROM concept) b
      ON a.drug_concept_id=b.b_drug_concept_id
    LEFT JOIN
    (SELECT concept_id as c_drug_type_concept_id,
             concept_name as drug_type_concept_name,
             concept_class_id as concept_class_id_drug_type,
            vocabulary_id as vocabulary_id_drug_type
      FROM concept) c
    ON a.drug_type_concept_id=c.c_drug_type_concept_id
    LEFT JOIN
    (SELECT concept_id as d_route_concept_id,
               concept_name as route_concept_name,
                concept_class_id as concept_class_id_drug_route,
            vocabulary_id as vocabulary_id_drug_route
    FROM concept) d
    ON a.route_concept_id=d.d_route_concept_id"
)


drugs_tabulation <-
  dbGetQueryBothTrusts(db, drug_concept_id_query) %>%
  distinct() %>%
  mutate(hospital_site = case_when(schema == "uhb" ~ "UHB",
                                   schema == "uclh" ~ "UCLH"))

drugs_concept_table <- drugs_tabulation %>%
  group_by(drug_concept_id, drug_concept_name, hospital_site) %>%
  dplyr::summarise(n = n())

drugs_concept_table <-
  dcast(drugs_concept_table,
        drug_concept_name + drug_concept_id ~ hospital_site)

drugs_concept_table = drugs_concept_table %>%
  mutate(
    UHB = if_else(is.na(UHB) | UHB <= 10, "<=10", as.character(UHB)),
    UCLH = if_else(is.na(UCLH) |
                     UCLH <= 10, "<=10", as.character(UCLH))
  )

write.csv(
  drugs_concept_table,
  csv_file("drugs_concept_table_Trust"),
  na = "",
  row.names = FALSE
)

drugs_concept_class_table <- drugs_tabulation %>%
  group_by(concept_class_id_drug, hospital_site) %>%
  dplyr::summarise(n = n())

drugs_concept_class_table <-
  dcast(drugs_concept_class_table,
        concept_class_id_drug ~ hospital_site)


drugs_concept_class_table = drugs_concept_class_table %>%
  mutate(
    UHB = if_else(is.na(UHB) | UHB <= 10, "<=10", as.character(UHB)),
    UCLH = if_else(is.na(UCLH) |
                     UCLH <= 10, "<=10", as.character(UCLH))
  )

write.csv(
  drugs_concept_class_table,
  csv_file("drugs_concept_class_table_Trust"),
  na = "",
  row.names = FALSE
)


drugs_concept_vocab_table <- drugs_tabulation %>%
  group_by(vocabulary_id_drug, hospital_site) %>%
  dplyr::summarise(n = n())

drugs_concept_vocab_table <-
  dcast(drugs_concept_vocab_table,  vocabulary_id_drug ~ hospital_site)


drugs_concept_vocab_table = drugs_concept_vocab_table %>%
  mutate(
    UHB = if_else(is.na(UHB) | UHB <= 10, "<=10", as.character(UHB)),
    UCLH = if_else(is.na(UCLH) |
                     UCLH <= 10, "<=10", as.character(UCLH))
  )

write.csv(
  drugs_concept_vocab_table,
  csv_file("drugs_concept_vocab_table_Trust"),
  na = "",
  row.names = FALSE
)

drugs_route_concept_table <- drugs_tabulation %>%
  group_by(route_concept_id, route_concept_name, hospital_site) %>%
  dplyr::summarise(n = n())

drugs_route_concept_table <-
  dcast(drugs_route_concept_table,
        route_concept_name + route_concept_id ~ hospital_site)

drugs_route_concept_table = drugs_route_concept_table %>%
  mutate(
    route_concept_name = ifelse(
      is.na(route_concept_name),
      "NULL",
      as.character(route_concept_name)
    ),
    route_concept_id =
      ifelse(is.na((route_concept_id)), "0", as.character(route_concept_id)),
    UHB = if_else(is.na(UHB) |
                    UHB <= 10, "<=10", as.character(UHB)),
    UCLH = if_else(is.na(UCLH) |
                     UCLH <= 10, "<=10", as.character(UCLH))
  )

write.csv(
  drugs_route_concept_table,
  csv_file("drugs_route_concep_table_Trust"),
  na = "",
  row.names = FALSE
)

rm(drugs_tabulation)



# Condition
condition_concepts_query <-
  paste0(
    "SELECT
       a.condition_occurrence_id,
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
       (CASE WHEN a.visit_occurrence_id IS NOT NULL
         THEN a.visit_occurrence_id %10
         WHEN a.person_id IS NOT NULL THEN a.person_id %10
         ELSE NULL END) as hospital_site
     FROM condition_occurrence a
     LEFT JOIN
     (SELECT concept_id as b_condition_concept_id,
              concept_name as condition_concept_name,
              concept_class_id as concept_class_id_condition,
              vocabulary_id as vocabulary_id_condition
       FROM concept) b
       ON a.condition_concept_id=b.b_condition_concept_id
     LEFT JOIN
     (SELECT concept_id as c_condition_type_concept_id,
             concept_name as condition_type_concept_name,
             concept_class_id as concept_class_id_condition_type,
              vocabulary_id as vocabulary_id_condition_type
       FROM concept) c
     ON a.condition_type_concept_id=c.c_condition_type_concept_id
     LEFT JOIN
     (SELECT concept_id as d_condition_status_concept_id,
             concept_name as condition_status_concept_name,
              concept_class_id as concept_class_id_condition_status,
              vocabulary_id as vocabulary_id_condition_status
     FROM concept) d
     ON a.condition_status_concept_id=d.d_condition_status_concept_id"
  )


conds_tabulation <-
  dbGetQueryBothTrusts(db, condition_concepts_query) %>%
  distinct() %>%
  mutate(hospital_site = case_when(schema == "uhb" ~ "UHB",
                                   schema == "uclh" ~ "UCLH"))

conds_concept_table <- conds_tabulation %>%
  group_by(condition_concept_id, condition_concept_name, hospital_site) %>%
  dplyr::summarise(n = n())

conds_concept_table <-
  dcast(conds_concept_table,
        condition_concept_name + condition_concept_id ~ hospital_site)

conds_concept_table = conds_concept_table %>%
  mutate(
    UHB = if_else(is.na(UHB) | UHB <= 10, "<=10", as.character(UHB)),
    UCLH = if_else(is.na(UCLH) |
                     UCLH <= 10, "<=10", as.character(UCLH))
  )

write.csv(
  conds_concept_table,
  csv_file("conds_concept_table_by_Trust"),
  na = "",
  row.names = FALSE
)

conds_concept_vocab_table <- conds_tabulation %>%
  group_by(vocabulary_id_condition, hospital_site) %>%
  dplyr::summarise(n = n())

conds_concept_vocab_table <-
  dcast(conds_concept_vocab_table,
        vocabulary_id_condition ~ hospital_site)

conds_concept_vocab_table = conds_concept_vocab_table %>%
  mutate(
    UHB = if_else(is.na(UHB) | UHB <= 10, "<=10", as.character(UHB)),
    UCLH = if_else(is.na(UCLH) |
                     UCLH <= 10, "<=10", as.character(UCLH))
  )

write.csv(
  conds_concept_vocab_table,
  csv_file("conds_concept_vocab_table_by_Trust"),
  na = "",
  row.names = FALSE
)

conds_concept_class_table <- conds_tabulation %>%
  group_by(concept_class_id_condition, hospital_site) %>%
  dplyr::summarise(n = n())

conds_concept_class_table <-
  dcast(conds_concept_class_table,
        concept_class_id_condition ~ hospital_site)

conds_concept_class_table = conds_concept_class_table %>%
  mutate(
    UHB = if_else(is.na(UHB) | UHB <= 10, "<=10", as.character(UHB)),
    UCLH = if_else(is.na(UCLH) |
                     UCLH <= 10, "<=10", as.character(UCLH))
  )

write.csv(
  conds_concept_class_table,
  csv_file("conds_concept_class_table_by_Trust"),
  na = "",
  row.names = FALSE
)

condition_type_concept_table <- conds_tabulation %>%
  group_by(condition_type_concept_id,
           condition_type_concept_name,
           hospital_site) %>%
  dplyr::summarise(n = n())

condition_type_concept_table <-
  dcast(
    condition_type_concept_table,
    condition_type_concept_name + condition_type_concept_id ~ hospital_site
  )

condition_type_concept_table = condition_type_concept_table %>%
  mutate(
    UHB = if_else(is.na(UHB) | UHB <= 10, "<=10", as.character(UHB)),
    UCLH = if_else(is.na(UCLH) |
                     UCLH <= 10, "<=10", as.character(UCLH))
  )

write.csv(
  condition_type_concept_table,
  csv_file("condition_type_concept_table_byTrust"),
  na = "",
  row.names = FALSE
)

condition_status_concept_table <- conds_tabulation %>%
  group_by(condition_status_concept_id,
           condition_status_concept_name,
           hospital_site) %>%
  dplyr::summarise(n = n())

condition_status_concept_table <-
  dcast(
    condition_status_concept_table,
    condition_status_concept_name + condition_status_concept_id ~ hospital_site
  )

condition_status_concept_table = condition_status_concept_table %>%
  mutate(
    condition_status_concept_name = ifelse(
      is.na(condition_status_concept_name),
      "NULL",
      as.character(condition_status_concept_name)
    ),
    condition_status_concept_id =
      ifelse(
        is.na(as.character(condition_status_concept_id)),
        "0",
        as.character(condition_status_concept_id)
      ),
    UHB = if_else(is.na(UHB) |
                    UHB <= 10, "<=10", as.character(UHB)),
    UCLH = if_else(is.na(UCLH) |
                     UCLH <= 10, "<=10", as.character(UCLH))
  )

write.csv(
  condition_status_concept_table,
  csv_file("condition_status_concept_table_byTrust"),
  na = "",
  row.names = FALSE
)
rm(conds_tabulation)

# Procedure
procedure_concepts_query <-
  paste0(
    "SELECT
       a.procedure_occurrence_id,
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
       (CASE WHEN 
          a.visit_occurrence_id IS NOT NULL
            THEN a.visit_occurrence_id %10
        WHEN a.person_id IS NOT NULL THEN a.person_id %10
         ELSE NULL END) as hospital_site
    FROM procedure_occurrence a
    LEFT JOIN
    (SELECT concept_id as b_procedure_concept_id,
            concept_name as procedure_concept_name,
            concept_class_id as concept_class_id_procedure,
            vocabulary_id as vocabulary_id_procedure
      FROM concept) b
      ON a.procedure_concept_id=b.b_procedure_concept_id
    LEFT JOIN
    (SELECT concept_id as c_procedure_type_concept_id,
            concept_name as procedure_type_concept_name,
            concept_class_id as concept_class_id_procedure_type,
            vocabulary_id as vocabulary_id_procedure_type
      FROM concept) c
    ON a.procedure_type_concept_id=c.c_procedure_type_concept_id
    LEFT JOIN
    (SELECT concept_id as d_procedure_source_concept_id,
            concept_name as procedure_source_concept_name,
            concept_class_id as concept_class_id_procedure_source,
            vocabulary_id as vocabulary_id_procedure_source
    FROM concept) d
    ON a.procedure_source_concept_id=d.d_procedure_source_concept_id"
  )


proc_tabulation <-
  dbGetQueryBothTrusts(db, procedure_concepts_query) %>%
  distinct()  %>%
  mutate(hospital_site = case_when(schema == "uhb" ~ "UHB",
                                   schema == "uclh" ~ "UCLH"))

proc_concept_table <- proc_tabulation %>%
  group_by(procedure_concept_id, procedure_concept_name, hospital_site) %>%
  dplyr::summarise(n = n())

proc_concept_table <-
  dcast(proc_concept_table,
        procedure_concept_name + procedure_concept_id ~ hospital_site)

proc_concept_table = proc_concept_table %>%
  mutate(
    UHB = if_else(is.na(UHB) | UHB <= 10, "<=10", as.character(UHB)),
    UCLH = if_else(is.na(UCLH) |
                     UCLH <= 10, "<=10", as.character(UCLH))
  )

write.csv(
  proc_concept_table,
  csv_file("proc_concept_table_byTrust"),
  na = "",
  row.names = FALSE
)

proc_concept_vocab_table <- proc_tabulation %>%
  group_by(vocabulary_id_procedure, hospital_site) %>%
  dplyr::summarise(n = n())

proc_concept_vocab_table <-
  dcast(proc_concept_vocab_table,
        vocabulary_id_procedure ~ hospital_site)

write.csv(
  proc_concept_vocab_table,
  csv_file("proc_concept_vocab_table_byTrust"),
  na = "",
  row.names = FALSE
)

procedure_type_concept_table <- proc_tabulation %>%
  group_by(procedure_type_concept_id,
           procedure_type_concept_name,
           hospital_site) %>%
  dplyr::summarise(n = n())

procedure_type_concept_table <-
  dcast(
    procedure_type_concept_table,
    procedure_type_concept_name + procedure_type_concept_id ~ hospital_site
  )

procedure_type_concept_table = procedure_type_concept_table %>%
  mutate(
    UHB = if_else(is.na(UHB) | UHB <= 10, "<=10", as.character(UHB)),
    UCLH = if_else(is.na(UCLH) |
                     UCLH <= 10, "<=10", as.character(UCLH))
  )

write.csv(
  procedure_type_concept_table,
  csv_file("procedure_type_concept_table_byTrust"),
  na = "",
  row.names = FALSE
)

rm(proc_tabulation)

# Specimen
spec_concepts_query <- paste0(
  "SELECT
     a.specimen_id,
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
   FROM specimen a
   LEFT JOIN
   (SELECT concept_id as b_specimen_concept_id,
           concept_name as specimen_concept_name,
           concept_class_id as concept_class_id_specimen,
           vocabulary_id as vocabulary_id_specimen
     FROM concept) b
     ON a.specimen_concept_id=b.b_specimen_concept_id
   LEFT JOIN
   (SELECT concept_id as c_anatomic_site_id,
           concept_name as anatomic_concept_name,
           concept_class_id as concept_class_id_anatomic,
           vocabulary_id as vocabulary_id_anatomic
     FROM concept) c
   ON a.anatomic_site_concept_id=c.c_anatomic_site_id"
)


spec_tabulation <- dbGetQueryBothTrusts(db, spec_concepts_query) %>%
  distinct() %>%
  mutate(hospital_site = case_when(schema == "uhb" ~ "UHB",
                                   schema == "uclh" ~ "UCLH"))

specimen_concept_table <- spec_tabulation %>%
  group_by(specimen_concept_id, specimen_concept_name, hospital_site) %>%
  dplyr::summarise(n = n())

specimen_concept_table <-
  dcast(specimen_concept_table,
        specimen_concept_name + specimen_concept_id ~ hospital_site)

specimen_concept_table = specimen_concept_table %>%
  mutate(
    UHB = if_else(is.na(UHB) | UHB <= 10, "<=10", as.character(UHB)),
    UCLH = if_else(is.na(UCLH) |
                     UCLH <= 10, "<=10", as.character(UCLH))
  )

write.csv(
  specimen_concept_table,
  csv_file("specimen_concept_table_byTrust"),
  na = "",
  row.names = FALSE
)

specimen_concept_vocab_table <- spec_tabulation %>%
  group_by(vocabulary_id_specimen, hospital_site) %>%
  dplyr::summarise(n = n())

specimen_concept_vocab_table <-
  dcast(specimen_concept_vocab_table,
        vocabulary_id_specimen ~ hospital_site)

specimen_concept_vocab_table = specimen_concept_vocab_table %>%
  mutate(
    UHB = if_else(is.na(UHB) | UHB <= 10, "<=10", as.character(UHB)),
    UCLH = if_else(is.na(UCLH) |
                     UCLH <= 10, "<=10", as.character(UCLH))
  )

write.csv(
  specimen_concept_vocab_table,
  csv_file("specimen_concept_vocab_table_byTrust"),
  na = "",
  row.names = FALSE
)

anatomic_site_concept_table <- spec_tabulation %>%
  group_by(anatomic_site_concept_id,
           anatomic_concept_name,
           hospital_site) %>%
  dplyr::summarise(n = n())

anatomic_site_concept_table <-
  dcast(
    anatomic_site_concept_table,
    anatomic_concept_name + anatomic_site_concept_id ~ hospital_site
  )

anatomic_site_concept_table = anatomic_site_concept_table %>%
  mutate(
    anatomic_concept_name = ifelse(
      is.na(anatomic_concept_name),
      "NULL",
      as.character(anatomic_concept_name)
    ),
    anatomic_site_concept_id = ifelse(
      is.na(as.character(anatomic_site_concept_id)),
      "NULL",
      as.character(anatomic_site_concept_id)
    ),
    UHB = if_else(is.na(UHB) | UHB <= 10, "<=10", as.character(UHB)),
    UCLH = if_else(is.na(UCLH) |
                     UCLH <= 10, "<=10", as.character(UCLH))
  )

write.csv(
  anatomic_site_concept_table,
  csv_file("anatomic_site_concept_table_byTrust"),
  na = "",
  row.names = FALSE
)


anatomic_site_concept_vocab_table <- spec_tabulation %>%
  group_by(vocabulary_id_anatomic, hospital_site) %>%
  dplyr::summarise(n = n())

anatomic_site_concept_vocab_table <-
  dcast(anatomic_site_concept_vocab_table,
        vocabulary_id_anatomic ~ hospital_site)

anatomic_site_concept_vocab_table = anatomic_site_concept_vocab_table %>%
  mutate(
    vocabulary_id_anatomic = ifelse(
      is.na(vocabulary_id_anatomic),
      "NULL",
      as.character(vocabulary_id_anatomic)
    ),
    UHB = if_else(is.na(UHB) |
                    UHB <= 10, "<=10", as.character(UHB)),
    UCLH = if_else(is.na(UCLH) |
                     UCLH <= 10, "<=10", as.character(UCLH))
  )


write.csv(
  anatomic_site_concept_vocab_table,
  csv_file("anatomic_site_concept_vocab_table_byTrust"),
  na = "",
  row.names = FALSE
)

anatomic_site_concept_class_table <- spec_tabulation %>%
  group_by(concept_class_id_anatomic, hospital_site) %>%
  dplyr::summarise(n = n())

anatomic_site_concept_class_table <-
  dcast(anatomic_site_concept_class_table,
        concept_class_id_anatomic ~ hospital_site)

anatomic_site_concept_class_table = anatomic_site_concept_class_table %>%
  mutate(
    concept_class_id_anatomic = ifelse(
      is.na(concept_class_id_anatomic),
      "NULL",
      as.character(concept_class_id_anatomic)
    ),
    UHB = if_else(is.na(UHB) |
                    UHB <= 10, "<=10", as.character(UHB)),
    UCLH = if_else(is.na(UCLH) |
                     UCLH <= 10, "<=10", as.character(UCLH))
  )

write.csv(
  anatomic_site_concept_class_table,
  csv_file("anatomic_site_concept_class_table_byTrust"),
  na = "",
  row.names = FALSE
)

# clear large unneeded table
rm(spec_tabulation)

# Visit occurrence
visit_occur_concepts_query <- paste0(
  "SELECT
     a.visit_occurrence_id,
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
     a.person_id,
     (CASE WHEN a.visit_occurrence_id IS NOT NULL THEN a.visit_occurrence_id %10
       WHEN a.person_id IS NOT NULL THEN a.person_id %10
       ELSE NULL END) as hospital_site
  FROM visit_occurrence a
  LEFT JOIN
  (SELECT concept_id as b_visit_concept,
          concept_name as visit_concept_name,
          concept_class_id as concept_class_id_visit_concept,
          vocabulary_id as vocabulary_id_visit_concept
    FROM concept) b
    ON a.visit_concept_id=b.b_visit_concept
  LEFT JOIN
  (SELECT concept_id as c_admitting_source_concept_id,
          concept_name as admitting_source_concept_name,
          concept_class_id as concept_class_id_admitting_source,
          vocabulary_id as vocabulary_id_visit_admitting_source
    FROM concept) c
  ON a.admitting_source_concept_id=c.c_admitting_source_concept_id
  LEFT JOIN
  (SELECT concept_id as d_discharge_to_concept_id,
          concept_name as discharge_to_concept_name,
          concept_class_id as concept_class_id_discharge_to,
          vocabulary_id as vocabulary_id_visit_discharge_to
    FROM concept) d
  ON a.discharge_to_concept_id=d.d_discharge_to_concept_id"
)


visitocc_tabulation <-
  dbGetQueryBothTrusts(db, visit_occur_concepts_query) %>%
  distinct() %>%
  mutate(hospital_site = case_when(schema == "uhb" ~ "UHB",
                                   schema == "uclh" ~ "UCLH"))


visit_concept_table <- visitocc_tabulation %>%
  group_by(visit_concept_id, visit_concept_name, hospital_site) %>%
  dplyr::summarise(n = n())

visit_concept_table <-
  dcast(visit_concept_table,
        visit_concept_name + visit_concept_id ~ hospital_site)

visit_concept_table = visit_concept_table %>%
  mutate(
    UHB = if_else(is.na(UHB) | UHB <= 10, "<=10", as.character(UHB)),
    UCLH = if_else(is.na(UCLH) |
                     UCLH <= 10, "<=10", as.character(UCLH))
  )

write.csv(
  visit_concept_table,
  csv_file("visit_concept_table_byTrust"),
  na = "",
  row.names = FALSE
)

admitting_concept_table <- visitocc_tabulation %>%
  group_by(admitting_source_concept_id,
           admitting_source_concept_name,
           hospital_site) %>%
  dplyr::summarise(n = n())

admitting_concept_table <-
  dcast(
    admitting_concept_table,
    admitting_source_concept_name + admitting_source_concept_id ~ hospital_site
  )

admitting_concept_table = admitting_concept_table %>%
  mutate(
    admitting_source_concept_id = ifelse(
      is.na(as.character(admitting_source_concept_id)),
      "0",
      as.character(admitting_source_concept_id)
    ),
    admitting_source_concept_name = ifelse(
      is.na(admitting_source_concept_name),
      "NULL",
      as.character(admitting_source_concept_name)
    ),
    UHB = if_else(is.na(UHB) | UHB <= 10, "<=10", as.character(UHB)),
    UCLH = if_else(is.na(UCLH) |
                     UCLH <= 10, "<=10", as.character(UCLH))
  )

write.csv(
  admitting_concept_table,
  csv_file("admitting_concept_table_byTrust"),
  na = "",
  row.names = FALSE
)

discharge_concept_table <- visitocc_tabulation %>%
  group_by(discharge_to_concept_id,
           discharge_to_concept_name,
           hospital_site) %>%
  dplyr::summarise(n = n())


discharge_concept_table <-
  dcast(
    discharge_concept_table,
    discharge_to_concept_name + discharge_to_concept_id ~ hospital_site
  )

discharge_concept_table = discharge_concept_table %>%
  mutate(
    discharge_to_concept_name = ifelse(
      is.na(discharge_to_concept_name),
      "NULL",
      as.character(discharge_to_concept_name)
    ),
    discharge_to_concept_id = ifelse(
      is.na(as.character(discharge_to_concept_id)),
      "0",
      as.character(discharge_to_concept_id)
    ),
    UHB = if_else(is.na(UHB) | UHB <= 10, "<=10", as.character(UHB)),
    UCLH = if_else(is.na(UCLH) |
                     UCLH <= 10, "<=10", as.character(UCLH))
  )

write.csv(
  discharge_concept_table,
  csv_file("discharge_concept_table_byTrust"),
  na = "",
  row.names = FALSE
)


# clear large unneeded table
rm(visitocc_tabulation)

# Visit_detail
visit_detail_concepts_query <- paste0(
  "SELECT
     a.visit_detail_id,
     a.visit_detail_concept_id,
     b.visit_detail_concept_name,
     b.concept_class_id_visit_detail,
     b.vocabulary_id__visit_detail,
     a.visit_occurrence_id,
     c.*,
     a.person_id,
     (CASE WHEN a.visit_occurrence_id IS NOT NULL THEN a.visit_occurrence_id %10
       WHEN a.person_id IS NOT NULL THEN a.person_id %10
       ELSE NULL END) as hospital_site
   FROM visit_detail a
   LEFT JOIN
   (SELECT concept_id as b_visit_detail_concept,
           concept_name as visit_detail_concept_name,
           concept_class_id as concept_class_id_visit_detail,
           vocabulary_id as vocabulary_id__visit_detail
     FROM concept) b
     ON a.visit_detail_concept_id=b.b_visit_detail_concept
   LEFT JOIN
   (SELECT care_site_id as care_site_id_c, *
      FROM care_site) c
   ON a.care_site_id=c.care_site_id_c"
)


visitdet_tabulation <-
  dbGetQueryBothTrusts(db, visit_detail_concepts_query) %>%
  distinct() %>%
  mutate(hospital_site = case_when(schema == "uhb" ~ "UHB",
                                   schema == "uclh" ~ "UCLH"))

visit_det_concept_table <- visitdet_tabulation %>%
  group_by(visit_detail_concept_id,
           visit_detail_concept_name,
           hospital_site) %>%
  dplyr::summarise(n = n())



visit_det_concept_table <-
  dcast(
    visit_det_concept_table,
    visit_detail_concept_name + visit_detail_concept_id ~ hospital_site
  )

visit_det_concept_table = visit_det_concept_table %>%
  mutate(
    UHB = if_else(is.na(UHB) | UHB <= 10, "<=10", as.character(UHB)),
    UCLH = if_else(is.na(UCLH) |
                     UCLH <= 10, "<=10", as.character(UCLH))
  )

write.csv(
  visit_det_concept_table,
  csv_file("visit_det_concept_table_byTrust"),
  na = "",
  row.names = FALSE
)


visit_det_caresite_table <- visitdet_tabulation %>%
  group_by(care_site_id, care_site_name, hospital_site) %>%
  dplyr::summarise(n = n())



visit_det_caresite_table <-
  dcast(visit_det_caresite_table,
        care_site_id + care_site_name ~ hospital_site)

visit_det_caresite_table = visit_det_caresite_table %>%
  mutate(
    UHB = if_else(is.na(UHB) | UHB <= 10, "<=10", as.character(UHB)),
    UCLH = if_else(is.na(UCLH) |
                     UCLH <= 10, "<=10", as.character(UCLH))
  )

write.csv(
  visit_det_caresite_table,
  csv_file("visit_det_caresite_table_byTrust"),
  na = "",
  row.names = FALSE
)

# Clear large table to free up memory
rm(visitdet_tabulation)


# Person
person_query_concepts <- paste0(
  "SELECT
     a.person_id,
     a.gender_concept_id,
     b.gender_concept_name,
     b.concept_class_id_gender,
     b.vocabulary_id_gender,
     a.race_concept_id,
     c.race_concept_name,
     c.concept_class_id_race,
     c.vocabulary_id_race
   FROM person a
   LEFT JOIN
   (SELECT concept_id as b_gender_concept_id,
           concept_name as gender_concept_name,
           concept_class_id as concept_class_id_gender,
           vocabulary_id as vocabulary_id_gender
     FROM concept) b
     ON a.gender_concept_id=b.b_gender_concept_id
     LEFT JOIN
     (SELECT concept_id as c_race_concept_id,
             concept_name as race_concept_name,
              concept_class_id as concept_class_id_race,
              vocabulary_id as vocabulary_id_race
     FROM concept) c
     ON a.race_concept_id=c.c_race_concept_id"
)

person_tabulation <-
  dbGetQueryBothTrusts(db, person_query_concepts) %>%
  distinct() %>%
  mutate(hospital_site = case_when(schema == "uhb" ~ "UHB",
                                   schema == "uclh" ~ "UCLH"))


gender_concept_table <- person_tabulation %>%
  group_by(gender_concept_id, gender_concept_name, hospital_site) %>%
  dplyr::summarise(n = n())


gender_concept_table <-
  dcast(gender_concept_table,
        gender_concept_name + gender_concept_id ~ hospital_site)

gender_concept_table = gender_concept_table %>%
  mutate(
    UHB = if_else(is.na(UHB) | UHB <= 10, "<=10", as.character(UHB)),
    UCLH = if_else(is.na(UCLH) |
                     UCLH <= 10, "<=10", as.character(UCLH))
  )

write.csv(
  gender_concept_table,
  csv_file("gender_concept_table_byTrust"),
  na = "",
  row.names = FALSE
)

race_concept_table <- person_tabulation %>%
  group_by(race_concept_id, race_concept_name, hospital_site) %>%
  dplyr::summarise(n = n())

race_concept_table <-
  dcast(race_concept_table,
        race_concept_name + race_concept_id ~ hospital_site)

race_concept_table = race_concept_table %>%
  mutate(
    UHB = if_else(is.na(UHB) | UHB <= 10, "<=10", as.character(UHB)),
    UCLH = if_else(is.na(UCLH) |
                     UCLH <= 10, "<=10", as.character(UCLH))
  )

write.csv(
  race_concept_table,
  csv_file("race_concept_table_byTrust"),
  na = "",
  row.names = FALSE
)

# clear unneeded table
rm(person_tabulation)

# Death
death_query_concepts <- paste0(
  "SELECT
     a.person_id,
     a.death_type_concept_id,
     b.death_type_concept_name,
     a.cause_concept_id,
     c.cause_concept_name,
     a.cause_source_concept_id,
     d.cause_source_concept_name
   FROM death a
   LEFT JOIN
   (SELECT concept_id as b_death_type_concept_id,
           concept_name as death_type_concept_name
     FROM concept) b
     ON a.death_type_concept_id=b.b_death_type_concept_id
     LEFT JOIN
     (SELECT concept_id as c_cause_concept_id,
             concept_name as cause_concept_name
     FROM concept) c
     ON a.cause_concept_id=c.c_cause_concept_id
          LEFT JOIN
     (SELECT concept_id as d_cause_source_concept_id,
             concept_name as cause_source_concept_name
     FROM concept) d
          ON a.cause_source_concept_id=d.d_cause_source_concept_id"
)

death_tabulation <-
  dbGetQueryBothTrusts(db, death_query_concepts) %>%
  distinct()

colnames(death_tabulation)

death_type_concept_table <- death_tabulation %>%
  group_by(death_type_concept_id, death_type_concept_name) %>%
  dplyr::summarise(n = n())


# Nothing - all nulls

death_cause_concept_table <- death_tabulation %>%
  group_by(cause_concept_id, cause_concept_name) %>%
  dplyr::summarise(n = n())


death_cause__source_concept_table <- death_tabulation %>%
  group_by(cause_source_concept_id, cause_source_concept_name) %>%
  dplyr::summarise(n = n())

# no concept_ids in the death table are populated with anything other than no
# matching concept id or 0...

meas_q <- paste0(
  "SELECT
     a.measurement_id,
     a.measurement_concept_id,
     b.measurement_concept_name,
     b.concept_class_id_measurement,
     b.vocabulary_id_measurement,
     a.visit_occurrence_id,
     a.person_id,
      (CASE WHEN a.visit_occurrence_id IS NOT NULL THEN a.visit_occurrence_id %10
            WHEN a.person_id IS NOT NULL THEN a.person_id %10
            ELSE NULL END) as hospital_site
   FROM measurement a
   LEFT JOIN
   (SELECT concept_id as measurement_id,
           concept_name as measurement_concept_name,
           concept_class_id as concept_class_id_measurement,
             vocabulary_id as vocabulary_id_measurement
    FROM concept) b
   ON a.measurement_concept_id=b.measurement_id"
)


omop_meas_all <- dbGetQueryBothTrusts(db, meas_q) %>%
  mutate(hospital_site = case_when(schema == "uhb" ~ "UHB",
                                   schema == "uclh" ~ "UCLH"))


measurements_concept_table <- omop_meas_all %>%
  group_by(measurement_concept_id,
           measurement_concept_name,
           hospital_site) %>%
  dplyr::summarise(n = n())

measurements_concept_table <-
  dcast(
    measurements_concept_table,
    measurement_concept_name + measurement_concept_id ~ hospital_site
  )

measurements_concept_table = measurements_concept_table %>%
  mutate(
    UHB = if_else(is.na(UHB) | UHB <= 10, "<=10", as.character(UHB)),
    UCLH = if_else(is.na(UCLH) |
                     UCLH <= 10, "<=10", as.character(UCLH))
  )

write.csv(
  measurements_concept_table,
  csv_file("measurements_concept_table_byTrust"),
  na = "",
  row.names = F
)


measurements_concept_vocab_table <- omop_meas_all %>%
  group_by(vocabulary_id_measurement, hospital_site) %>%
  dplyr::summarise(n = n())

measurements_concept_vocab_table <-
  dcast(measurements_concept_vocab_table,
        vocabulary_id_measurement ~ hospital_site)

measurements_concept_vocab_table = measurements_concept_vocab_table %>%
  mutate(
    UHB = if_else(is.na(UHB) | UHB <= 10, "<=10", as.character(UHB)),
    UCLH = if_else(is.na(UCLH) |
                     UCLH <= 10, "<=10", as.character(UCLH))
  )

write.csv(
  measurements_concept_vocab_table,
  csv_file("measurements_concept_vocab_table_byTrust"),
  na = "",
  row.names = F
)


measurements_concept_class_table <- omop_meas_all %>%
  group_by(concept_class_id_measurement, hospital_site) %>%
  dplyr::summarise(n = n())

measurements_concept_class_table <-
  dcast(measurements_concept_class_table,
        concept_class_id_measurement ~ hospital_site)

measurements_concept_class_table = measurements_concept_class_table %>%
  mutate(
    UHB = if_else(is.na(UHB) | UHB <= 10, "<=10", as.character(UHB)),
    UCLH = if_else(is.na(UCLH) |
                     UCLH <= 10, "<=10", as.character(UCLH))
  )

write.csv(
  measurements_concept_class_table,
  csv_file("measurements_concept_class_table_byTrust"),
  na = "",
  row.names = F
)
