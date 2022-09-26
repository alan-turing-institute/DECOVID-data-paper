##################################################################################
#################### TABLE 1a - DECOVID Data Descriptor Paper ####################
##################################################################################

#This code creates most of the summaries.

#uncomment the following lines if the packages have not been installed previously.
#install,packages("DBI", "dplyr", "Hmisc", "purrr", "tableone")
library(DBI) #for performing SQL queries
library(dplyr) #for data manipulation
library(Hmisc) #for describing variables
library(purrr) #for data manipulation
library(tableone) #for demographics table


#Enter log-in information for the database
port <- rstudioapi::askForPassword(prompt="Please enter port")
user <- rstudioapi::askForPassword(prompt="Please enter username")
pw <- rstudioapi::askForPassword(prompt="Please enter password")

vent <- DBI::dbConnect(
  RPostgres::Postgres(),
  host=rstudioapi::askForPassword(prompt="Please enter host"),
  port=port,
  user=user,  
  password=pw,
  dbname='ventilation'
)
print("STATUS: connected to ventilation")

copd <- DBI::dbConnect(
  RPostgres::Postgres(),
  host=rstudioapi::askForPassword(prompt="Please enter host"),
  port=port,
  user=user,
  password=pw,
  dbname='copd'
)
print("STATUS: connected to copd")

coag <- DBI::dbConnect(
  RPostgres::Postgres(),
  host=rstudioapi::askForPassword(prompt="Please enter host"),
  port=port,
  user=user, 
  password=pw,
  dbname='coagthrombo'
)
print("STATUS: connected to coagthrombo")

news2 <- DBI::dbConnect(
  RPostgres::Postgres(),
  host=rstudioapi::askForPassword(prompt="Please enter host"),
  port=port,
  user=user,
  password=pw,
  dbname='news2'
)
print("STATUS: connected to news2")


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


#First, start with querying variables available in DECOVID's visit_occurrence table that are summarized
#in Table 1. This is primarily for the visit-level summaries
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
                              patient_days 
                              FROM
                              (SELECT visit_occurrence_id, 
                                      person_id, 
                                      visit_start_date, 
                                      visit_end_date, 
                                      admitting_source_concept_id, 
                                      discharge_to_concept_id, 
                             (DATE_PART('day', visit_end_datetime::timestamp - visit_start_datetime::timestamp) +
                              DATE_PART('hour', visit_end_datetime::timestamp - visit_start_datetime::timestamp) /24 + 
                              DATE_PART('minute', visit_end_datetime::timestamp - visit_start_datetime::timestamp) / 1440) AS patient_days,
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

#Again, we have to query each of DECOVID's research question databases separately. Here,
#we do not need to remove duplicates at this stage.
copd_visit <- dbGetQuery(copd, visit_query)

coag_visit <- dbGetQuery(coag, visit_query)

vent_visit <- dbGetQuery(vent, visit_query)

news2_visit <- dbGetQuery(news2, visit_query)

#Append all research questions together. Here, we avoid duplicates by joining on all
#variables rather than use distinct().
omop_visit_all <-  list(copd_visit, coag_visit, vent_visit, news2_visit) %>% 
                    plyr::join_all(by="visit_occurrence_id", type="full", match="all") %>%
                    mutate(covid=ifelse(visit_occurrence_id %in% covid_case_type$visit_occurrence_id, "Yes", "No"))

#remove individual research question dataframes
rm(copd_visit, coag_visit, vent_visit, news2_visit)

#visit_detail query - this is to identity the level of care for each visit, as the 
visit_detail_query <- paste0("SELECT care_site_id, visit_occurrence_id FROM
                             omop_03082021.visit_detail")

copd_care <- dbGetQuery(copd, visit_detail_query)
coag_care <- dbGetQuery(coag, visit_detail_query)
vent_care <- dbGetQuery(vent, visit_detail_query)
news2_care <- dbGetQuery(news2, visit_detail_query)

#Append the visit_detail data extracts from each research question database query,
#removing duplicates.
omop_care <-  rbind(copd_care, coag_care, vent_care, news2_care) %>% 
                    distinct() %>%
                    #the care_site_id values are integer64, which sometimes has issues, so convert to numeric, but
                    #first convert the integer64 to character.
                    mutate(care_site_id=as.numeric(as.character(care_site_id))) 


#remove individual research question dataframes
rm(copd_care, coag_care, vent_care, news2_care)

#The condition_occurrence table is now queried, as this is used for the visit-level summaries.
condition_q <- paste0("SELECT a.*, 
                              a.visit_occurrence_id %10 AS hospital_site,   
                              b.concept_id, 
                              b.concept_name, 
                              c.concept_id AS concept_id_specific, 
                              c.concept_name AS concept_name_specific 
                      FROM omop_03082021.condition_occurrence a
                      LEFT JOIN omop_03082021.concept  b
                      ON a.condition_type_concept_id=b.concept_id
                      LEFT JOIN omop_03082021.concept  c 
                      ON a.condition_concept_id=c.concept_id")


copd_condition_q <- dbGetQuery(copd, condition_q)

coag_condition_q <- dbGetQuery(coag, condition_q)

vent_condition_q <- dbGetQuery(vent, condition_q)

news2_condition_q <- dbGetQuery(news2, condition_q)

#Append the condition_occurrence data extracts from each research question database query,
#removing duplicates.
omop_cond_all <- rbind(copd_condition_q, coag_condition_q, vent_condition_q, news2_condition_q) %>% 
                 distinct()

#remove individual research question dataframes
rm(copd_condition_q,coag_condition_q,vent_condition_q,news2_condition_q)

#Let's create a copy of the omop_visit_all as a primary table
#of all visit_occurrence_ids for the condition summary
omop_for_visit_condition <- data.frame(visit_occurrence_id=as.numeric(as.character(omop_visit_all[,c("visit_occurrence_id")]))) 

#Let's take the columns of interest from the condition table
omop_cond_all_final <- omop_cond_all[,c("condition_occurrence_id","visit_occurrence_id","concept_name")]

omop_cond_all_final$visit_occurrence_id <- as.numeric(as.character(omop_cond_all_final$visit_occurrence_id))

omop_cond_all_final$condition_occurrence_id <- as.numeric(as.character(omop_cond_all_final$condition_occurrence_id))

#Since we are compiling a visit-level summary here, let's exclude any records
#linked to a visit, as we cannot be certain here what visit they may be linked to.
omop_cond_all_final <- omop_cond_all_final %>% 
                       filter(!is.na(visit_occurrence_id))

#Let's create new variable based on point of care when a diagnosis may be made
omop_cond_all_final$DiagnosisLevel = case_when(grepl("Chief Complaint|encounter diagnosis", omop_cond_all_final$concept_name) ~ "Vist level",
                                               grepl("billing diagnosis", omop_cond_all_final$concept_name) ~ "Consultant episode level",
                                               TRUE ~ omop_cond_all_final$concept_name)

#Let's now aggregate by visit_occurrence_id and Diagnosis Level
condition_summary_1 <- omop_cond_all_final  %>%
                        group_by(visit_occurrence_id, DiagnosisLevel) %>%
                        dplyr::summarise(n_per_vist=n()) 

#We need to reshape this data, which will ultimately be joined to the visit data
#table omop_visit_all
condition_summary_2 <- dcast(condition_summary_1, visit_occurrence_id ~ DiagnosisLevel, value.var = "n_per_vist")

#We need to convert the omop_visit_all table visit_occurrence_id
omop_visit_all$visit_occurrence_id <- as.numeric(as.character(omop_visit_all$visit_occurrence_id))

omop_visit_all <- left_join(omop_visit_all, condition_summary_2, by=c("visit_occurrence_id"="visit_occurrence_id"))

#The drug_exposure table is now queried, as this is used for the visit-level summaries.
drug_q <- paste0("SELECT a.*,
                         b.drug_concept_name,
                         b.concept_class_id_drug,
                         b.vocabulary_id_drug,
                         c.drug_type_concept_name,
                         c.concept_class_id_drug_type,
                         c.vocabulary_id_drug_type
                      FROM omop_03082021.drug_exposure a
                      LEFT JOIN 
                      (SELECT concept_id as drug_concept_id_ctable,
                              concept_name as drug_concept_name,
                              concept_class_id as concept_class_id_drug,
                              vocabulary_id as vocabulary_id_drug
                       FROM omop_03082021.concept) b
                      ON a.drug_concept_id=b.drug_concept_id_ctable
                      LEFT JOIN
                      (SELECT concept_id as drug_type_concept_id_ctable,
                              concept_name as drug_type_concept_name,
                              concept_class_id as concept_class_id_drug_type,
                              vocabulary_id as vocabulary_id_drug_type
                       FROM omop_03082021.concept) c
                      ON a.drug_type_concept_id=c.drug_type_concept_id_ctable")


copd_drug_q <- dbGetQuery(copd, drug_q)

coag_drug_q <- dbGetQuery(coag, drug_q)

vent_drug_q <- dbGetQuery(vent, drug_q)

news2_drug_q <- dbGetQuery(news2, drug_q)

#Append the drug_exposure data extracts from each research question database query,
#removing duplicates.
omop_drug_all <- rbind(copd_drug_q, coag_drug_q, vent_drug_q, news2_drug_q) %>% 
                  distinct()

#remove individual research question dataframes
rm(copd_drug_q, coag_drug_q, vent_drug_q, news2_drug_q)

###Now let's load the concept relationship table.
#The purpose for this code is to find locate
#standard vocabularies so that we can find drugs of interest to summarise.
#particularly, anti-coagulants

#Load concept relationship table
concept_relationship_q <- paste0("SELECT *
                                 FROM omop_03082021.concept_relationship")

concept_relationship_table <- dbGetQuery(copd, concept_relationship_q)

#Load ancestor table
concept_ancestor_q <- paste0("SELECT *
                                FROM omop_03082021.concept_ancestor")

concept_ancestor_table <- dbGetQuery(copd, concept_ancestor_q)

#Load concept table
concept_q <- paste0("SELECT *
                     FROM omop_03082021.concept")

concept <- dbGetQuery(copd, concept_q)

#Translate non-standard to standard
concept_relationship_table_standard <- concept_relationship_table %>% 
                                        filter(relationship_id=="Maps to")

#Find non-standard concepts
drugs_exposure_standard <- omop_drug_all %>%
                            left_join(select(concept_relationship_table_standard, concept_id_2,concept_id_1),
                                      by=c("drug_concept_id"="concept_id_1")) %>%
                            rename(drug_concept_id_standard=concept_id_2)

#Find non-standard concepts
concept_relationship_replaces <- concept_relationship_table %>%
                                  filter(relationship_id=="Concept replaces")

drug_exposure_standard_replaced <- drugs_exposure_standard %>%
                                    filter(!is.na(drug_concept_id_standard)) %>%
                                    inner_join(select(concept_relationship_replaces, concept_id_2, concept_id_1),
                                               by=c("drug_concept_id"="concept_id_1")) %>%
                                    rename(concept_id_replaced=concept_id_2) %>%
                                    left_join(select(concept_relationship_table_standard, concept_id_2, concept_id_1),
                                              by=c("concept_id_replaced"="concept_id_1")) %>%
                                    select(-c(drug_concept_id_standard, concept_id_replaced)) %>%
                                    rename(drug_concept_id_standard=concept_id_2) %>%
                                    filter(!(is.na(drug_concept_id_standard)))


drugs_exposure_standard <- drugs_exposure_standard %>% 
                            filter(!is.na(drug_concept_id_standard))     

drugs_exposure_standard = rbind(drugs_exposure_standard, drug_exposure_standard_replaced)

missing_standard = setdiff(omop_drug_all$drug_concept_id, drugs_exposure_standard$drug_concept_id)


concept %>% filter(concept_id %in% missing_standard) %>% select(concept_id, concept_name)

#Now, let's identify drugs of interest. 

#Anti-coagulants
anticoagulants <- concept_ancestor_table %>%
                  filter(ancestor_concept_id==35807264) %>%
                  select(descendant_concept_id) %>%
                  left_join(drugs_exposure_standard, by=c("descendant_concept_id"="drug_concept_id_standard")) %>%
                  filter(visit_occurrence_id != 0)

#IL6 inhib's - we do not really need to do this, since Tocilizumab is the only 
#drug within this drug class.
antiILantibody <- concept_ancestor_table %>%
                  filter(ancestor_concept_id==35807440|ancestor_concept_id==35807449) %>%
                  select(descendant_concept_id) %>%
                  left_join(drugs_exposure_standard, by=c("descendant_concept_id"="drug_concept_id_standard")) %>%
                  filter(visit_occurrence_id != 0)

#Let's just make sure this is no different than just querying Tocilizumab
Tocilizumab = omop_drug_all %>%
              filter(grepl("Tocilizumab", drug_concept_name)) %>%
              filter(visit_occurrence_id != 0)

#Start drug summary - let's filter out drug records that are not linked to a visit_occurrence_id
#as we are presenting a visit-level summary
SpecificDrugSum <- omop_drug_all %>%
                    filter(visit_occurrence_id!="0") %>%
                    group_by(visit_occurrence_id, drug_concept_name, drug_concept_id) %>%
                    dplyr::summarise(n=n())

#We want Dexamethasone, IL6 inhibs and anticoags. We do not care how many records
#of a drug a visit has, just if the drug is present. So, an indicator variable is sufficient. 
SpecificDrugSum$Dexamethasone <- ifelse(grepl("Dexamethasone", SpecificDrugSum$drug_concept_name), 1, 0) 

SpecificDrugSum$IL6I_Tocilizumab <- ifelse(SpecificDrugSum$drug_concept_id %in% antiILantibody$drug_concept_id, 1, 0) 

SpecificDrugSum$Anticoags <- ifelse(SpecificDrugSum$drug_concept_id %in% anticoagulants$drug_concept_id, 1, 0) 

SpecificDrugSum_tbl <- SpecificDrugSum %>%
                       group_by(visit_occurrence_id) %>%
                       dplyr::summarise(Dexamethasone=sum(Dexamethasone),IL6I_Tocilizumab=sum(IL6I_Tocilizumab), Anticoags=sum(Anticoags))

SpecificDrugSum_tbl$visit_occurrence_id <- as.numeric(as.character(SpecificDrugSum_tbl$visit_occurrence_id))

omop_visit_all <- merge(omop_visit_all,SpecificDrugSum_tbl, by=c("visit_occurrence_id"), all.x=TRUE)

omop_visit_all$Dexamethasone <- ifelse(is.na(omop_visit_all$Dexamethasone) | omop_visit_all$Dexamethasone==0, "No", "Yes")

omop_visit_all$IL6I_Tocilizumab <- ifelse(is.na(omop_visit_all$IL6I_Tocilizumab)|omop_visit_all$IL6I_Tocilizumab==0, "No", "Yes")

omop_visit_all$Anticoags <- ifelse(is.na(omop_visit_all$Anticoags) |omop_visit_all$Anticoags ==0, "No", "Yes")

#Clean visit data
omop_visit_clean_all <- omop_visit_all %>%
                        #The oldest person currently living in the UK at the time of data extraction
                        #was born in 1909, so year of birth is modified accordingly.
                        mutate(year_of_birth=replace(year_of_birth, year_of_birth < 1909, NA)) %>% 
  
                        #This variable is actually ethnicity, not race, and the groups are formed
                        #based on the ONS ethnicity groupings.
                        mutate(ethnicity_group=case_when(
                          grepl(race_concept_name, pattern="Asian or Asian British:") ~ "Asian",
                          grepl(race_concept_name, pattern="Black or African or Caribbean or Black British:") ~ "Black",
                          grepl(race_concept_name, pattern="Mixed multiple ethnic groups:") ~ "Mixed",
                          grepl(race_concept_name, pattern="White:") ~ "White",
                          grepl(race_concept_name, pattern="Other ethnic group:") ~ "Other",
                          race_concept_name=="Ethnicity not stated" | race_concept_name=="Unknown racial group" ~ "Unknown"),
                          race_concept_name=NULL) %>%
                        
                        #In the queries, the hospital site variable was created, where the last digit of visit_occurrence,
                        #or person_id can be used to identify the site/Trust as below.
                        mutate(hospital_site=case_when(
                          hospital_site==4 ~ "UHB",
                          hospital_site==6 ~ "UCLH")) %>%
  
                        #There are instances where the length of stay, or patient days, of a  
                        mutate(patient_days=ifelse(patient_days < 0, NA, patient_days)) %>%
  
                        #In the DECOVID database, sex at birth is incorrectly labelled as gender.
                        mutate(gender_concept_name=tolower(gender_concept_name) %>% Hmisc::capitalize(),
                               gender_concept_name=ifelse(gender_concept_name=="Unknown" | gender_concept_name=="No matching concept", "Unknown", gender_concept_name)) %>%
                        
                        #identify patients that have ever received specific levels of care.
                        mutate(l2_l3=ifelse(visit_occurrence_id %in% as.numeric(as.character((omop_care %>% filter(care_site_id %in% c(1,2,3,20,21,22)) %>% pull(visit_occurrence_id)))), "Yes", "No"),
                               Inpatient=ifelse(visit_occurrence_id %in% as.numeric(as.character((omop_care %>% filter(care_site_id %in% c(7,8,17)) %>% pull(visit_occurrence_id)))), "Yes", "No"),
                               ED=ifelse(visit_occurrence_id %in% as.numeric(as.character((omop_care %>% filter(care_site_id %in% c(9,10,11,12,18)) %>% pull(visit_occurrence_id)))), "Yes", "No"),
                               Other=ifelse(visit_occurrence_id %in% as.numeric(as.character((omop_care %>% filter(care_site_id %in% c(4,5,6,14,16)) %>% pull(visit_occurrence_id)))), "Yes", "No")
                        )  %>%
                        
                        #Re-categorize the discharge destination or discharge_to_concept_name.
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
  
                        #Re-categorize the admitting source or admitting_source_concept_name
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
              
                      #Format variables for creating table 1.
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
                          "Other", 
                          "IL6I_Tocilizumab",
                          "Dexamethasone",
                          "Anticoags"),
                        as.factor
                      ) %>%
                      mutate_at(c("patient_days" ),
                                as.numeric) 


#Rename some of the variables
omop_visit_format_all <- omop_visit_clean_all %>%
                          dplyr::rename(
                            "Patients"="person_id",
                            "Hospital_encounters"="visit_occurrence_id",
                            "Sex"="gender_concept_name",
                            "Year of birth"="year_of_birth",
                            "Ethnicity"="ethnicity_group",
                            "Length of stay (days)"="patient_days",
                            "Level 2/3 care"="l2_l3",
                            "covid"="covid",
                            "Hospital"="hospital_site",
                            "Admitted from"="admitting_source_concept_name",
                            "Outcomes"="discharge_to_concept_name")

#Summarise only the select number of visit-level variables that are available in the omop_visit_format_all table
#and the remaining visit-level queries require bespoke queries of which some  are done in separate documents.
CreateTableOne(data=omop_visit_format_all, 
               vars=colnames(omop_visit_format_all)[which(!(colnames(omop_visit_format_all) %in% c("Patients", "Sex", "Ethnicity", "Hospital", "covid", "visit_start_date", "visit_end_date")))], 
               strata=c("covid", "Hospital"), addOverall=T, test=F, includeNA=T) %>% print(showAllLevels=T, nonnormal=c("Year of birth", "Length of stay (days)", "EHR problem list entry", "Past medical history", "Consultant episode level", "Vist level"), noSpaces=T) %>%
                write.csv("visit_data_summary_covid_all_test_2.csv", row.names=T, na="")

