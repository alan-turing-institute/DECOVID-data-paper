library(DBI) #for performing SQL queries
library(dplyr) #for data manipulation
library(Hmisc) #for describing variables
library(purrr) #for data manipulation
library(tableone) #for demographics table
library(kableExtra) #for markdown tables
library(reshape2) #for measurement table reshaping
library(lubridate)
library(aweek)

#This includes code to create measusurement plots - but would require clean up
#with new data.

#PCR
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


#Run PCR Only Queries
copd_covid_pcr <- dbGetQuery(copd, covid_pcr_query) %>%
  distinct()

coag_covid_pcr <- dbGetQuery(coag, covid_pcr_query) %>%
  distinct()

vent_covid_pcr <- dbGetQuery(vent, covid_pcr_query) %>%
  distinct()

news2_covid_pcr <- dbGetQuery(news2, covid_pcr_query) %>%
  distinct()

#Combine with all - this is used for the final Table 1.
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

#Combine all research questions.
omop_covid_all <-   rbind(copd_covid_all, coag_covid_all, vent_covid_all, news2_covid_all) %>%
  distinct()

visit_query_org <- paste0("SELECT visit_occurrence_id, person_id, gender_concept_name, race_concept_name, year_of_birth, hospital_site, visit_start_date, visit_end_date, admitting_source_concept_name, discharge_to_concept_name, patient_days FROM
                        (SELECT visit_occurrence_id, person_id, visit_start_date, visit_end_date, admitting_source_concept_id, discharge_to_concept_id,
                        (DATE_PART('day', visit_end_datetime::timestamp - visit_start_datetime::timestamp) +
                        DATE_PART('hour', visit_end_datetime::timestamp - visit_start_datetime::timestamp) /24 +
                        DATE_PART('minute', visit_end_datetime::timestamp - visit_start_datetime::timestamp) / 1440) AS patient_days,
                        visit_occurrence_id %10 AS hospital_site
                        FROM omop_03082021.visit_occurrence) a
                        LEFT JOIN
                        (SELECT gender_concept_id, race_concept_id, person_id, year_of_birth
                          FROM omop_03082021.person) b
                          USING (person_id)
                        LEFT JOIN
                        (SELECT concept_id as gender_concept_id, concept_name as gender_concept_name
                          FROM omop_03082021.concept) c
                          USING (gender_concept_id)
                        LEFT JOIN
                        (SELECT concept_id as race_concept_id, concept_name as race_concept_name
                          FROM omop_03082021.concept) d
                          USING (race_concept_id)
                        LEFT JOIN
                        (SELECT concept_id as discharge_to_concept_id, concept_name as
                          discharge_to_concept_name FROM omop_03082021.concept) e
                        USING (discharge_to_concept_id)
                        LEFT JOIN
                        (SELECT concept_id as admitting_source_concept_id, concept_name as
                          admitting_source_concept_name FROM omop_03082021.concept) f
                        USING (admitting_source_concept_id)")


copd_visit_org <- dbGetQuery(copd, visit_query_org)
paste("STATUS: information for", nrow(copd_visit_org), "visits extracted from copd")

coag_visit_org <- dbGetQuery(coag, visit_query_org)
paste("STATUS: information for", nrow(coag_visit_org), "visits extracted from coagthrombo")

vent_visit_org <- dbGetQuery(vent, visit_query_org)
paste("STATUS: information for", nrow(vent_visit_org), "visits extracted from ventilation")

news2_visit_org <- dbGetQuery(news2, visit_query_org)
paste("STATUS: information for", nrow(news2_visit_org), "visits extracted from news2")

#Create pipe message function so code easier to debug
pipe_message=function(.data, status){message(status); .data}


omop_visit_all_org <-
  list(copd_visit_org, coag_visit_org, vent_visit_org, news2_visit_org) %>%
  plyr::join_all(by="visit_occurrence_id", type="full", match="all") %>%
  mutate(covid=ifelse(visit_occurrence_id %in% omop_covid_all$visit_occurrence_id, "Yes", "No"))


#Create pipe message function so code easier to debug
pipe_message=function(.data, status){message(status); .data}

#Clean visit data
omop_visit_clean_all_org <- omop_visit_all_org %>%
  mutate(year_of_birth=replace(year_of_birth, year_of_birth < 1912, NA)) %>%
  mutate(ethnicity_group=case_when(
    grepl(race_concept_name, pattern="Asian or Asian British:") ~ "Asian",
    grepl(race_concept_name, pattern="Black or African or Caribbean or Black British:") ~ "Black",
    grepl(race_concept_name, pattern="Mixed multiple ethnic groups:") ~ "Mixed",
    grepl(race_concept_name, pattern="White:") ~ "White",
    grepl(race_concept_name, pattern="Other ethnic group:") ~ "Other",
    race_concept_name=="Ethnicity not stated" | race_concept_name=="Unknown racial group" ~ "Unknown"),
    race_concept_name=NULL) %>%
  mutate(hospital_site=case_when(
    hospital_site==4 ~ "UHB",
    hospital_site==6 ~ "UCLH")) %>%
  mutate(patient_days=ifelse(patient_days < 0, NA, patient_days)) %>%
  mutate(gender_concept_name=tolower(gender_concept_name) %>% Hmisc::capitalize(),
         gender_concept_name=ifelse(gender_concept_name=="Unknown" | gender_concept_name=="No matching concept", "Unknown", gender_concept_name)) %>%
  mutate(l2_l3=ifelse(visit_occurrence_id %in% (omop_care %>% filter(care_site_id %in% c(1,2,3,15,20,21,22)) %>% pull(visit_occurrence_id)), "Yes", "No"),
         Inpatient=ifelse(visit_occurrence_id %in% (omop_care %>% filter(care_site_id %in% c(7,8,17)) %>% pull(visit_occurrence_id)), "Yes", "No"),
         ED=ifelse(visit_occurrence_id %in% (omop_care %>% filter(care_site_id %in% c(9,10,11,12,13,18,19)) %>% pull(visit_occurrence_id)), "Yes", "No"),
         Other=ifelse(visit_occurrence_id %in% (omop_care %>% filter(care_site_id %in% c(4,5,6,14,16)) %>% pull(visit_occurrence_id)), "Yes", "No")
  ) %>%

  mutate(discharge_to_concept_name=case_when(
    discharge_to_concept_name %in% c("Inpatient Hospital", "Inpatient Psychiatric Facility") ~ "Transferred as inpatient",
    discharge_to_concept_name %in% c(
      "Hospice",
      "Assisted Living Facility",
      "Adult Living Care Facility",
      "Intermediate Mental Care Facility",
      "Prison/Correctional Facility"
    ) ~ "Discharged to other setting",
    discharge_to_concept_name=="Patient died" ~ "Died",
    discharge_to_concept_name=="Home" ~ "Discharged home",
    (is.na(visit_end_date) &
       is.na(discharge_to_concept_name)) ~ "Remain in hospital",
    (
      is.na(visit_end_date) &
        discharge_to_concept_name=="No matching concept"
    ) ~ "Remain in hospital",
    discharge_to_concept_name=="No matching concept" ~ NA_character_
  )) %>%
  mutate(admitting_source_concept_name=case_when(
    admitting_source_concept_name %in% c("Inpatient Hospital", "Inpatient Psychiatric Facility") ~ "Admitted from other inpatient facility",
    admitting_source_concept_name %in% c(
      "Hospice",
      "Assisted Living Facility",
      "Adult Living Care Facility",
      "Prison/Correctional Facility"
    ) ~ "Admitted from other setting",
    admitting_source_concept_name=="Home" ~ "Admitted from home",
    admitting_source_concept_name=="No matching concept" ~ NA_character_
  )) %>%
  mutate_at(
    c(
      "gender_concept_name",
      "ethnicity_group",
      "covid",
      "hospital_site",
      "admitting_source_concept_name",
      "discharge_to_concept_name",
      "l2_l3",
      "Inpatient",
      "ED",
      "Other"    ),
    as.factor
  ) %>%
  mutate_at(c("patient_days"),
            as.numeric)

omop_care <-
  rbind(copd_care, coag_care, vent_care, news2_care) %>%
  distinct() %>%
  mutate(care_site_id=as.numeric(care_site_id))


#Load in visit data
visit_query <- paste0("SELECT visit_occurrence_id,
                              person_id,
                              gender_concept_name,
                              race_concept_name,
                              year_of_birth,
                              hospital_site,
                              visit_start_date,
                              visit_end_date,
                              admitting_source_concept_name,
                              discharge_to_concept_name,
                              patient_hours
                              FROM
                              (SELECT visit_occurrence_id,
                                      person_id,
                                      visit_start_date,
                                      visit_end_date,
                                      admitting_source_concept_id,
                                      discharge_to_concept_id,
                              (DATE_PART('day',
                              visit_end_datetime::timestamp - visit_start_datetime::timestamp)*24 +
                              DATE_PART('hour', visit_end_datetime::timestamp - visit_start_datetime::timestamp) +
                              DATE_PART('minute', visit_end_datetime::timestamp - visit_start_datetime::timestamp) / 60) AS patient_hours,
                              visit_occurrence_id %10 AS hospital_site
                              FROM omop_03082021.visit_occurrence) a
                              LEFT JOIN
                              (SELECT gender_concept_id,
                                      race_concept_id,
                                      person_id,
                                      year_of_birth
                              FROM omop_03082021.person) b
                              USING (person_id)
                              LEFT JOIN
                              (SELECT concept_id as gender_concept_id,
                                      concept_name as gender_concept_name
                                FROM omop_03082021.concept) c
                                USING (gender_concept_id)
                               LEFT JOIN
                              (SELECT concept_id as race_concept_id,
                                      concept_name as race_concept_name
                              FROM omop_03082021.concept) d
                                USING (race_concept_id)
                              LEFT JOIN
                              (SELECT concept_id as discharge_to_concept_id,
                                     concept_name as discharge_to_concept_name
                              FROM omop_03082021.concept) e
                              USING (discharge_to_concept_id)
                              LEFT JOIN
                              (SELECT concept_id as admitting_source_concept_id,
                                      concept_name as admitting_source_concept_name
                              FROM omop_03082021.concept) f
                              USING (admitting_source_concept_id)")

copd_visit <- dbGetQuery(copd, visit_query)
paste("STATUS: information for", nrow(copd_visit), "visits extracted from copd")

coag_visit <- dbGetQuery(coag, visit_query)
paste("STATUS: information for", nrow(coag_visit), "visits extracted from coagthrombo")

vent_visit <- dbGetQuery(vent, visit_query)
paste("STATUS: information for", nrow(vent_visit), "visits extracted from ventilation")

news2_visit <- dbGetQuery(news2, visit_query)
paste("STATUS: information for", nrow(news2_visit), "visits extracted from news2")

#Care Site query
care_site_query <- paste0("SELECT care_site_id, visit_occurrence_id FROM
                          omop_03082021.visit_detail")

copd_care <- dbGetQuery(copd, care_site_query)
coag_care <- dbGetQuery(coag, care_site_query)
vent_care <- dbGetQuery(vent, care_site_query)
news2_care <- dbGetQuery(news2, care_site_query)

omop_care <-
  rbind(copd_care, coag_care, vent_care, news2_care) %>%
  distinct() %>%
  mutate(care_site_id=as.numeric(care_site_id))


omop_visit_all <-
  list(copd_visit, coag_visit, vent_visit, news2_visit) %>%
  plyr::join_all(by="visit_occurrence_id", type="full", match="all") %>%
  mutate(covid=ifelse(visit_occurrence_id %in% omop_covid_all$visit_occurrence_id, "Yes", "No"))

paste("Number of visits:", nrow(omop_visit_all), "-", sum(duplicated(omop_visit_all$visit_occurrence_id)), "duplicates")

paste("Number of people:", n_distinct(omop_visit_all$person_id))

paste("Date range:", min(omop_visit_all$visit_start_date), "to", max(c(omop_visit_all$visit_start_date, omop_visit_all$visit_end_date), na.rm=T))

#Create pipe message function so code easier to debug
pipe_message=function(.data, status){message(status); .data}

#Clean visit data
omop_visit_clean_all <- omop_visit_all %>%

  mutate(year_of_birth=replace(year_of_birth, year_of_birth < 1912, NA)) %>%


  mutate(ethnicity_group=case_when(
    grepl(race_concept_name, pattern="Asian or Asian British:") ~ "Asian",
    grepl(race_concept_name, pattern="Black or African or Caribbean or Black British:") ~ "Black",
    grepl(race_concept_name, pattern="Mixed multiple ethnic groups:") ~ "Mixed",
    grepl(race_concept_name, pattern="White:") ~ "White",
    grepl(race_concept_name, pattern="Other ethnic group:") ~ "Other",
    race_concept_name=="Ethnicity not stated" | race_concept_name=="Unknown racial group" ~ "Unknown"),
    race_concept_name=NULL) %>%


  mutate(hospital_site=case_when(
    hospital_site==4 ~ "UHB",
    hospital_site==6 ~ "UCLH")) %>%


  mutate(patient_hours=ifelse(patient_hours < 0, NA, patient_hours)) %>%


  mutate(gender_concept_name=tolower(gender_concept_name) %>% Hmisc::capitalize(),
         gender_concept_name=ifelse(gender_concept_name=="Unknown" | gender_concept_name=="No matching concept", "Unknown", gender_concept_name)) %>%

  mutate(l2_l3=ifelse(visit_occurrence_id %in% (omop_care %>% filter(care_site_id %in% c(1,2,3,15,20,21,22)) %>% pull(visit_occurrence_id)), "Yes", "No"),
         Inpatient=ifelse(visit_occurrence_id %in% (omop_care %>% filter(care_site_id %in% c(7,8,17)) %>% pull(visit_occurrence_id)), "Yes", "No"),
         ED=ifelse(visit_occurrence_id %in% (omop_care %>% filter(care_site_id %in% c(9,10,11,12,13,18,19)) %>% pull(visit_occurrence_id)), "Yes", "No"),
         Other=ifelse(visit_occurrence_id %in% (omop_care %>% filter(care_site_id %in% c(4,5,6,14,16)) %>% pull(visit_occurrence_id)), "Yes", "No")
  )  %>%
  mutate(discharge_to_concept_name=case_when(
    discharge_to_concept_name %in% c("Inpatient Hospital", "Inpatient Psychiatric Facility") ~ "Transferred as inpatient",
    discharge_to_concept_name %in% c(
      "Hospice",
      "Assisted Living Facility",
      "Adult Living Care Facility",
      "Intermediate Mental Care Facility",
      "Prison/Correctional Facility"
    ) ~ "Discharged to other setting",
    discharge_to_concept_name=="Patient died" ~ "Died",
    discharge_to_concept_name=="Home" ~ "Discharged home",
    (is.na(visit_end_date) &
       is.na(discharge_to_concept_name)) ~ "Remain in hospital",
    (
      is.na(visit_end_date) &
        discharge_to_concept_name=="No matching concept"
    ) ~ "Remain in hospital",
    discharge_to_concept_name=="No matching concept" ~ NA_character_
  ))  %>%
  mutate(admitting_source_concept_name=case_when(
    admitting_source_concept_name %in% c("Inpatient Hospital", "Inpatient Psychiatric Facility") ~ "Admitted from other inpatient facility",
    admitting_source_concept_name %in% c(
      "Hospice",
      "Assisted Living Facility",
      "Adult Living Care Facility",
      "Prison/Correctional Facility"
    ) ~ "Admitted from other setting",
    admitting_source_concept_name=="Home" ~ "Admitted from home",
    admitting_source_concept_name=="No matching concept" ~ NA_character_
  )) %>%
  mutate_at(
    c(
      "gender_concept_name",
      "ethnicity_group",
      "covid",
      "hospital_site",
      "admitting_source_concept_name",
      "discharge_to_concept_name",
      "l2_l3",
      "Inpatient",
      "ED",
      "Other"    ),
    as.factor
  ) %>%
  mutate_at(c("patient_hours"),
            as.numeric)


sapply(omop_visit_clean_all, class) %>%
  data.frame()
describe(omop_visit_clean_all) %>%
  html()
omop_visit_format_all <- omop_visit_clean_all %>%
  dplyr::rename(
    "Patients"="person_id",
    "Hospital encounters"="visit_occurrence_id",
    "Sex"="gender_concept_name",
    "Year of birth"="year_of_birth",
    "Ethnicity"="ethnicity_group",
    "Length of stay (hours)"="patient_hours",
    "Level 2/3 care"="l2_l3",
    "COVID diagnosis"="covid",
    "Hospital"="hospital_site",
    "Admitted from"="admitting_source_concept_name",
    "Outcomes"="discharge_to_concept_name")

CreateTableOne(data=omop_visit_format_all,
               vars=colnames(omop_visit_format_all)[which(!(colnames(omop_visit_format_all) %in% c("Hospital", "COVID diagnosis", "visit_start_date", "visit_end_date")))],
               strata=c("COVID diagnosis", "Hospital"), addOverall=T, test=F, includeNA=T) %>% print(showAllLevels=T, nonnormal=c("Year of birth", "Length of stay (days)"), noSpaces=T)


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
         covid=ifelse(visit_occurrence_id %in% omop_covid_all$visit_occurrence_id, "Yes", "No"),
         hospital_site=case_when(
           hospital_site==4 ~ "UHB",
           hospital_site==6 ~ "UCLH"))


#Read in measurements of interest
library(lubridate)
library(hms)
library(dplyr)

measurement <- read.csv("//idhs.ucl.ac.uk/user/User_Data/idhsnba/My Documents/R/win-library/3.6/measurements_filtered.csv") %>%
               dplyr::rename(measurement_concept_id=concept_id)

measurement_nobp_no_resp <- measurement %>%
                            filter(!(category %in% c("blood_pressure", "resp", "RR Man", "RR Spont", "troponin", "vent_mode", "news2Resp", "peep")))

#nrow(measurement_nobp_no_resp)
for (i in 1:length(measurement_nobp_no_resp$measurement_concept_id)) {
  print(i)
meas_q_figs <- paste0("SELECT a.*,
                         b.measurement_concept_name,
                         b.concept_class_id_measurement,
                         b.vocabulary_id_measurement,
                          (CASE WHEN a.visit_occurrence_id IS NOT NULL THEN a.visit_occurrence_id %10
                                WHEN a.person_id IS NOT NULL THEN a.person_id %10
                                ELSE NULL END) as hospital_site
                      FROM
                      (SELECT *
                      FROM omop_03082021.measurement
                      WHERE measurement_concept_id = ",
                 paste0(paste0("'", measurement_nobp_no_resp[i,] %>% pull(measurement_concept_id), "'", collapse="")), " AND visit_occurrence_id IS NOT NULL) a
                      LEFT JOIN
                      (SELECT concept_id as measurement_id,
                              concept_name as measurement_concept_name,
                              concept_class_id as concept_class_id_measurement,
                                vocabulary_id as vocabulary_id_measurement
                       FROM omop_03082021.concept) b
                      ON a.measurement_concept_id=b.measurement_id")


copd_meas_q <- dbGetQuery(copd, meas_q_figs)
coag_meas_q <- dbGetQuery(coag, meas_q_figs)
vent_meas_q <- dbGetQuery(vent, meas_q_figs)
news2_meas_q <- dbGetQuery(news2, meas_q_figs)

omop_meas_all <- rbind(news2_meas_q, coag_meas_q,vent_meas_q,news2_meas_q) %>%
                  distinct()

omop_meas_all <- omop_meas_all %>%
                  mutate(hospital_site = case_when(hospital_site=hospital_site==4 ~ "UHB",
                                   hospital_site==6 ~ "UCLH"),
                 covid=if_else(as.character(visit_occurrence_id) %in% as.character(omop_covid_all$visit_occurrence_id), "COVID-19", "Non-COVID-19"))

omop_care$visit_detail_id_char <- as.character(omop_care$visit_detail_id)
omop_meas_all$visit_detail_id_char <- as.character(omop_meas_all$visit_detail_id)
omop_meas_all_care <- inner_join(omop_meas_all, omop_care, by=c("visit_occurrence_id"))

omop_meas_all_carefilter <- omop_meas_all_care %>%
                             filter(measurement_datetime>=visit_detail_start_datetime, measurement_datetime<=visit_detail_end_datetime)

#time
omop_meas_all_carefilter$measurement_time_re <- as.POSIXct(omop_meas_all_carefilter$measurement_datetime, format="%Y-%m-%d %H:%M:%S")
omop_meas_all_carefilter$measurement_time_re <- format(omop_meas_all_carefilter$measurement_time_re, format="%H:%M:%S")
omop_meas_all_carefilter$measurement_time_re <- as_hms(omop_meas_all_carefilter$measurement_time_re )
omop_meas_all_carefilter$Level <- ifelse(omop_meas_all_carefilter$care_site_id %in% c("1","2","3","15","20","21","22"), "Level 2/3",
                                         ifelse(omop_meas_all_carefilter$care_site_id %in% c("7","8","17"), "Level 1", "Other"))

omop_meas_all_carefilter$Level <- as.factor(omop_meas_all_carefilter$Level)

RR_d_UCLH <- ggplot(omop_meas_all_carefilter %>% filter(Level !="Other" & hospital_site.x=="UCLH"), aes(measurement_time_re)) +
                geom_density() +
                facet_wrap(~Level +covid.x, scales="free") +
                theme_classic() +
                 ggtitle(paste("UCLH: ", omop_meas_all_carefilter$measurement_concept_name, "\n(Concept ID: ",omop_meas_all_carefilter$measurement_concept_id, ")", sep="" )) +
                xlab("Time of Day") +
                ylab("Density")

ggsave(paste("MeasurementTimeSamplePlot_UCLH",unique(omop_meas_all_carefilter$measurement_concept_id),".png", sep=""), plot=RR_d_UCLH)

RR_d_UHB <- ggplot(omop_meas_all_carefilter %>% filter(Level !="Other" & hospital_site.x=="UHB"), aes(measurement_time_re)) +
  geom_density() +
  facet_wrap(~Level +covid.x, scales="free") +
  theme_classic() +
  ggtitle(paste("UHB: ", omop_meas_all_carefilter$measurement_concept_name, "\n(Concept ID: ",omop_meas_all_carefilter$measurement_concept_id, ")", sep="" )) +
  xlab("Time of Day") +
  ylab("Density")

ggsave(paste("MeasurementTimeSamplePlot_UHB",unique(omop_meas_all_carefilter$measurement_concept_id),".png", sep=""), plot=RR_d_UHB)

}

measurement_bp <- measurement %>%
  filter((category %in% c("blood_pressure")))


meas_q_figs <- paste0("SELECT a.*,
                         b.measurement_concept_name,
                         b.concept_class_id_measurement,
                         b.vocabulary_id_measurement,
                          (CASE WHEN a.visit_occurrence_id IS NOT NULL THEN a.visit_occurrence_id %10
                                WHEN a.person_id IS NOT NULL THEN a.person_id %10
                                ELSE NULL END) as hospital_site
                      FROM
                      (SELECT *
                      FROM omop_03082021.measurement
                      WHERE measurement_concept_id IN (",
                        paste0(paste0("'", measurement_bp %>% pull(measurement_concept_id), "'", collapse=",")), ") AND visit_occurrence_id IS NOT NULL) a
                      LEFT JOIN
                      (SELECT concept_id as measurement_id,
                              concept_name as measurement_concept_name,
                              concept_class_id as concept_class_id_measurement,
                                vocabulary_id as vocabulary_id_measurement
                       FROM omop_03082021.concept) b
                      ON a.measurement_concept_id=b.measurement_id")


copd_meas_q <- dbGetQuery(copd, meas_q_figs)
coag_meas_q <- dbGetQuery(coag, meas_q_figs)
vent_meas_q <- dbGetQuery(vent, meas_q_figs)
news2_meas_q <- dbGetQuery(news2, meas_q_figs)

omop_meas_all <- rbind(news2_meas_q, coag_meas_q,vent_meas_q,news2_meas_q) %>%
    distinct()

omop_meas_all <- omop_meas_all %>%
    mutate(hospital_site = case_when(hospital_site=hospital_site==4 ~ "UHB",
                                     hospital_site==6 ~ "UCLH"),
           covid=if_else(as.character(visit_occurrence_id) %in% as.character(omop_covid_all$visit_occurrence_id), "COVID-19", "Non-COVID-19"))

omop_care$visit_detail_id_char <- as.character(omop_care$visit_detail_id)
omop_meas_all$visit_detail_id_char <- as.character(omop_meas_all$visit_detail_id)
omop_meas_all$visit_occurrence_id <- as.character(omop_meas_all$visit_occurrence_id)
omop_care$visit_occurrence_id <- as.character(omop_care$visit_occurrence_id)
omop_meas_all_care <- inner_join(omop_meas_all, omop_care, by=c("visit_occurrence_id"))

omop_meas_all_carefilter <- omop_meas_all_care %>%
    filter(measurement_datetime>=visit_detail_start_datetime, measurement_datetime<=visit_detail_end_datetime)
rm(omop_meas_all_care)

#time
library(lubridate)
library(hms)
omop_meas_all_carefilter$measurement_time_re <- as.POSIXct(strptime(omop_meas_all_carefilter$measurement_datetime, format="%Y-%m-%d %H:%M:%S"))
omop_meas_all_carefilter$measurement_time_re <- format(omop_meas_all_carefilter$measurement_time_re, format="%H:%M:%S")
omop_meas_all_carefilter$measurement_time <- as_hms(omop_meas_all_carefilter$measurement_time_re )
omop_meas_all_carefilter$Level <- ifelse(omop_meas_all_carefilter$care_site_id %in% c("1","2","3","15","20","21","22"), "Level 2/3 Care",
                                           ifelse(omop_meas_all_carefilter$care_site_id %in% c("7","8","17"), "Level 1 Care", "Other"))
as.POSIXct(omop_meas_all_carefilter$measurement_time)
omop_meas_all_carefilter$Level <- as.factor(omop_meas_all_carefilter$Level)
BLOODPRESSUREDATA <- omop_meas_all_carefilter %>% filter(Level !="Other" & hospital_site.x=="UCLH")
BLOODPRESSUREDATA$measurement_time_re <- as_hms(BLOODPRESSUREDATA$measurement_time_re )
BLOODPRESSUREDATA$measurement_time <- as.POSIXct(BLOODPRESSUREDATA$measurement_time_re)

RR_d_UCLH <- ggplot(BLOODPRESSUREDATA, aes(measurement_time)) +
    geom_density() +
    facet_wrap(~Level +covid.x, scales="free") +
    theme_classic() +
    ggtitle(paste("UCLH: ", "Blood Pressure", "\n(Concept IDs: ",paste0(unique(BLOODPRESSUREDATA$measurement_concept_id),collapse = ","), ")", sep="" )) +
    xlab("Time of Day") +
    ylab("Density")


ggsave(filename=paste("MeasurementTimeSamplePlot_UCLH","BloodPressure",".png", sep=""), height=7, width=7, plot=RR_d_UCLH)
grDevices::png(filename=paste("MeasurementTimeSamplePlot_UCLH","BloodPressure",".png", sep=""), plot=RR_d_UCLH, height=7, width=7)
dev.off()

RR_d_UHB <- ggplot(omop_meas_all_carefilter %>% filter(Level !="Other" & hospital_site.x=="UHB"), aes(measurement_time)) +
    geom_density() +
    facet_wrap(~Level +covid.x, scales="free") +
    theme_classic() +
    ggtitle(paste("UHB: ", "Blood Pressure", "\n(Concept IDs: ",paste0(unique(omop_meas_all_carefilter$measurement_concept_id),collapse = ","), ")", sep="" )) +
    xlab("Time of Day") +
    ylab("Density")

ggsave(filename=paste("MeasurementTimeSamplePlot_UHB","BloodPressure",".png", sep=""), height=7, width=7, plot=RR_d_UHB)

measurement_rr <- measurement %>%
  filter((category %in% c("resp", "RR Man", "RR Spont")))

meas_q_figs <- paste0("SELECT a.*,
                         b.measurement_concept_name,
                         b.concept_class_id_measurement,
                         b.vocabulary_id_measurement,
                          (CASE WHEN a.visit_occurrence_id IS NOT NULL THEN a.visit_occurrence_id %10
                                WHEN a.person_id IS NOT NULL THEN a.person_id %10
                                ELSE NULL END) as hospital_site
                      FROM
                      (SELECT *
                      FROM omop_03082021.measurement
                      WHERE measurement_concept_id IN ('4313591', '4108138', '4154772') AND visit_occurrence_id IS NOT NULL) a
                      LEFT JOIN
                      (SELECT concept_id as measurement_id,
                              concept_name as measurement_concept_name,
                              concept_class_id as concept_class_id_measurement,
                                vocabulary_id as vocabulary_id_measurement
                       FROM omop_03082021.concept) b
                      ON a.measurement_concept_id=b.measurement_id")


copd_meas_q <- dbGetQuery(copd, meas_q_figs)
coag_meas_q <- dbGetQuery(coag, meas_q_figs)
vent_meas_q <- dbGetQuery(vent, meas_q_figs)
news2_meas_q <- dbGetQuery(news2, meas_q_figs)

omop_meas_all <- rbind(news2_meas_q, coag_meas_q,vent_meas_q,news2_meas_q) %>%
  distinct()

omop_meas_all <- omop_meas_all %>%
  mutate(hospital_site = case_when(hospital_site=hospital_site==4 ~ "UHB",
                                   hospital_site==6 ~ "UCLH"),
         covid=if_else(as.character(visit_occurrence_id) %in% as.character(omop_covid_all$visit_occurrence_id), "COVID-19", "Non-COVID-19"))

omop_care$visit_detail_id_char <- as.character(omop_care$visit_detail_id)
omop_meas_all$visit_detail_id_char <- as.character(omop_meas_all$visit_detail_id)
omop_meas_all$visit_occurrence_id <- as.character(omop_meas_all$visit_occurrence_id)
omop_meas_all_care <- inner_join(omop_meas_all, omop_care, by=c("visit_occurrence_id"))

omop_meas_all_carefilter <- omop_meas_all_care %>%
  filter(measurement_datetime>=visit_detail_start_datetime, measurement_datetime<=visit_detail_end_datetime)

#time
omop_meas_all_carefilter$measurement_time_re <- as.POSIXct(omop_meas_all_carefilter$measurement_datetime, format="%Y-%m-%d %H:%M:%S")
omop_meas_all_carefilter$measurement_time_re <- format(omop_meas_all_carefilter$measurement_time_re, format="%H:%M:%S")
omop_meas_all_carefilter$measurement_time_re <- as_hms(omop_meas_all_carefilter$measurement_time_re )

omop_meas_all_carefilter$Level <- ifelse(omop_meas_all_carefilter$care_site_id %in% c("1","2","3","15","20","21","22"), "Level 2/3",
                                         ifelse(omop_meas_all_carefilter$care_site_id %in% c("7","8","17"), "Level 1", "Other"))

omop_meas_all_carefilter$Level <- as.factor(omop_meas_all_carefilter$Level)
omop_meas_all_carefilter$time <- as.POSIXct(omop_meas_all_carefilter$measurement_time_re)
RR_UCLH <- omop_meas_all_carefilter %>%
filter(Level !="Other" & hospital_site.x=="UCLH")

RR_d_UCLH <- ggplot(RR_UCLH , aes(measurement_time_re)) +
              geom_density() +
              facet_wrap(~Level +covid.x, scales="free") +
              theme_classic() +
              ggtitle(paste("UCLH: ", "Respiratory Rate", "\n(Concept ID: ",paste0(unique(RR_UCLH$measurement_concept_id),collapse = ","), ")", sep="" )) +
              xlab("Time of Day") +
              ylab("Density")

ggsave(filename=paste("MeasurementTimeSamplePlot_UCLH","RespRate",".png", sep=""), height=7, width=7, plot=RR_d_UCLH)

RR_UHB <- omop_meas_all_carefilter %>%
filter(Level !="Other" & hospital_site.x=="UHB")
RR_d_UHB <- ggplot(RR_UHB, aes(measurement_time_re)) +
            geom_density() +
            facet_wrap(~Level +covid.x, scales="free") +
            theme_classic() +
            ggtitle(paste("UHB: ", "Respiratory Rate", "\n(Concept IDs: ",paste0(unique(RR_UHB$measurement_concept_id),collapse = ","), ")", sep="" )) +
            xlab("Time of Day") +
            ylab("Density")

#add path
ggsave(filename=paste("MeasurementTimeSamplePlot_UHB","RespRate",".png", sep=""), height=7, width=7, plot=RR_d_UHB)


