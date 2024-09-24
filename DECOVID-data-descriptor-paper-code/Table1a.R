##################################################################################
#################### TABLE 1a - DECOVID Data Descriptor Paper ####################
##################################################################################

#Authors: Nicholas Bakewell, Rebecca Green, Hannah Nicholls
#Last Updated: 12 June 2022

#This code creates most of the summaries in the DECOVID Data Descriptor paper for Table 1 and
#nearly all of the in-text summaries.

#uncomment the following lines if the packages have not been installed previously.
#install,packages("DBI", "dplyr", "Hmisc", "purrr", "tableone", "zoo","plyr","reshape2")
library(DBI) #for performing SQL queries
library(dplyr) #for data manipulation
library(Hmisc) #for describing variables
library(purrr) #for data manipulation
library(tableone) #for demographics table
library(plyr) #for carrying observations forward for BMI
library(zoo) #for carrying observations forward for BMI
library(reshape2) #For reshaping data

source("common-functions.R")
source("common-database.R")
source("common-queries.R")

#Run PCR Only Queries - distinct() is used to remove duplicates
omop_covid_pcr <- dbGetQueryBothTrusts(db, covid_pcr_query) %>%
  distinct()

#Here, the COVID-19 cases based on clinical diagnoses (suspected and confirmed) are appended to the PCR only cases - again, distinct()
#is used to remove duplicates
omop_covid_all <- omop_covid_pcr %>%
  rbind(dbGetQueryBothTrusts(db, covid_obs_all_query)) %>%
  distinct()

#Before proceeding, specify the COVID-19 case type the data summaries should be based on.
#In the DECOVID Data Descriptor paper, the omop_covid_all cases are used.
#However, the omop_covid_pcr can be assigned to covid_case_type instead.
covid_case_type <- omop_covid_all


#First, start with querying variables available in DECOVID's visit_occurrence table that are summarized
#in Table 1. This is primarily for the visit-level summaries

#Again, we have to query each of DECOVID's research question databases separately. Here,
#we do not need to remove duplicates at this stage.
omop_visit_all <- dbGetQueryBothTrusts(db, visit_query) %>%
               mutate(covid=ifelse(visit_occurrence_id %in% covid_case_type$visit_occurrence_id, "Yes", "No"))

#visit_detail query - this is to identity the level of care for each visit, as the
visit_detail_query <- paste0("SELECT care_site_id, visit_occurrence_id FROM
                             visit_detail")

omop_care <- dbGetQueryBothTrusts(db, visit_detail_query) %>%
                    distinct() %>%
                    #the care_site_id values are integer64, which sometimes has issues, so convert to numeric, but
                    #first convert the integer64 to character.
                    mutate(care_site_id=as.numeric(as.character(care_site_id)))

#The condition_occurrence table is now queried, as this is used for the visit-level summaries.
omop_cond_all <- dbGetQueryBothTrusts(db, condition_q) %>%
                 distinct()

#Let's create a copy of the omop_visit_all as a primary table
#of all visit_occurrence_ids for the condition summary
omop_for_visit_condition <- data.frame(visit_occurrence_id=omop_visit_all[,c("visit_occurrence_id")])

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
                      FROM drug_exposure a
                      LEFT JOIN
                      (SELECT concept_id as drug_concept_id_ctable,
                              concept_name as drug_concept_name,
                              concept_class_id as concept_class_id_drug,
                              vocabulary_id as vocabulary_id_drug
                       FROM concept) b
                      ON a.drug_concept_id=b.drug_concept_id_ctable
                      LEFT JOIN
                      (SELECT concept_id as drug_type_concept_id_ctable,
                              concept_name as drug_type_concept_name,
                              concept_class_id as concept_class_id_drug_type,
                              vocabulary_id as vocabulary_id_drug_type
                       FROM concept) c
                      ON a.drug_type_concept_id=c.drug_type_concept_id_ctable")


omop_drug_all <- dbGetQueryBothTrusts(db, drug_q) %>%
                  distinct()

###Now let's load the concept relationship table.
#The purpose for this code is to find locate
#standard vocabularies so that we can find drugs of interest to summarise.
#particularly, anti-coagulants

#Load concept relationship table
concept_relationship_q <- paste0("SELECT *
                                 FROM concept_relationship")

concept_relationship_table <- dbGetQueryBothTrusts(db,
                                                   concept_relationship_q,
                                                   schemas = "dbo")

#Load ancestor table
concept_ancestor_q <- paste0("SELECT *
                                FROM concept_ancestor")

concept_ancestor_table <- dbGetQueryBothTrusts(db,
                                               concept_ancestor_q,
                                               schemas = "dbo")

#Load concept table
concept_q <- paste0("SELECT *
                     FROM concept")

concept <- dbGetQueryBothTrusts(db, concept_q, schemas = "dbo")

#Translate non-standard to standard
concept_relationship_table_standard <- concept_relationship_table %>%
                                        filter(relationship_id=="Maps to")

#Find non-standard concepts
drugs_exposure_standard <- omop_drug_all %>%
                            left_join(select(concept_relationship_table_standard, concept_id_2,concept_id_1),
                                      by=c("drug_concept_id"="concept_id_1")) %>%
                            dplyr::rename(drug_concept_id_standard=concept_id_2)

#Find non-standard concepts
concept_relationship_replaces <- concept_relationship_table %>%
                                  filter(relationship_id=="Concept replaces")

drug_exposure_standard_replaced <- drugs_exposure_standard %>%
                                    filter(!is.na(drug_concept_id_standard)) %>%
                                    inner_join(select(concept_relationship_replaces, concept_id_2, concept_id_1),
                                               by=c("drug_concept_id"="concept_id_1")) %>%
                                    dplyr::rename(concept_id_replaced=concept_id_2) %>%
                                    left_join(select(concept_relationship_table_standard, concept_id_2, concept_id_1),
                                              by=c("concept_id_replaced"="concept_id_1")) %>%
                                    dplyr::select(-c(drug_concept_id_standard, concept_id_replaced)) %>%
                                    dplyr::rename(drug_concept_id_standard=concept_id_2) %>%
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

#BMI
BMI_query <- paste("SELECT b.visit_occurrence_id,
                           visit_start_datetime,
                           visit_end_datetime,
                           measurement_datetime,
                              value_as_number,
                              a.unit_concept_id,
                              unit
                              FROM
                      (SELECT *
                      FROM measurement
                      WHERE measurement_concept_id IN ('3036277', '3025315')) a
                      INNER JOIN
                      (SELECT visit_start_datetime,
                              visit_end_datetime,
                              visit_occurrence_id
                      FROM visit_occurrence) b
                      ON (a.visit_occurrence_id = b.visit_occurrence_id)
                      LEFT JOIN (SELECT concept_id as unit_concept_id,
                                        concept_name as unit
                      FROM concept) c
                      ON (a.unit_concept_id = c.unit_concept_id)
                      WHERE ((measurement_datetime>=visit_start_datetime) AND (measurement_datetime<=visit_end_datetime))")


omop_BMI_table <- dbGetQueryBothTrusts(db, BMI_query) %>%
            distinct()

#Select only necessary columns and reshape the data to wide format
#such that weight and height taken at the same time
#are in the same row
#Also, data is arranged in order for the next step
BMI <- omop_BMI_table %>%
       dplyr::select(visit_occurrence_id, measurement_datetime, value_as_number, unit) %>%
       dcast(visit_occurrence_id + measurement_datetime ~ unit, value.var = "value_as_number") %>%
       arrange(visit_occurrence_id,measurement_datetime)

#Let's carry-forward the last observation for the same visit for both
#height and weight. Also, carry the a second observation
#backwards if missingness is present for the first observation at presentation.
#library(plyr)
#Carry observations forward with measurement_datetime sorted in ascending order
BMI <- ddply(BMI,.(visit_occurrence_id), na.locf)

#Now, let's carry second observation backwards by resorting
#measurement_datetime
BMI <- BMI %>%
       arrange(visit_occurrence_id, desc(measurement_datetime))


BMI <- ddply(BMI,.(visit_occurrence_id), na.locf)

#Note, any high values have been filtered.
#These thresholds are based on an online search, but
#more sophisticated methods may be used to detect and remove anomalies.
BMI <- BMI %>%
         arrange(visit_occurrence_id, measurement_datetime) %>%
         mutate(BMI=kilogram/(meter^2)) %>%
         filter(!(BMI>=120) & !(BMI<=10))

#Keep the first observation at presentation only
#We sorted the data by visit_occurrence_id and measurement_datetime
#above, so we can just removed duplicated visit_occurrence_id records now
#This is equivalent to taking the first row by visit_occurrence_id in SQL.
BMI <- BMI[!duplicated(BMI$visit_occurrence_id),]

omop_visit_all <- left_join(omop_visit_all, BMI %>% select(visit_occurrence_id, BMI) %>% mutate(visit_occurrence_id=as.numeric(as.character(visit_occurrence_id))), by=c("visit_occurrence_id"))

#Clean visit data for summaries.
omop_visit_clean_all <- omop_visit_all %>%
                        #The oldest person currently living in the UK at the time of data extraction
                        #was born in 1909, so year of birth is modified accordingly.
                        mutate(year_of_birth=replace(year_of_birth, year_of_birth < 1909, NA)) %>%

                        #Age (rounded down/floored) - assumed birth date of 2nd July of the year of birth available in DECOVID
                        mutate(age_years=floor(as.numeric(difftime(visit_start_date,as.Date(paste(year_of_birth, "07","02", sep="-")), units="days")/365.25))) %>%

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
                          schema=="uhb" ~ "UHB",
                          schema=="uclh" ~ "UCLH")) %>%

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

                        #For condition type, let's change the NAs to 0s
                        mutate(EHRProblemList_0s=ifelse(is.na(`EHR problem list entry`), 0, `EHR problem list entry`),
                               PastMedicalHistory_0s=ifelse(is.na(`Past medical history`), 0, `Past medical history`),
                               ConsultantEpisodeLevel_0s=ifelse(is.na(`Consultant episode level`), 0, `Consultant episode level`),
                               HospitalVisitLevel_0s=ifelse(is.na(`Vist level`),0,`Vist level`)

                        ) %>%
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
                      mutate_at(c("patient_days","BMI","age_years" ),
                                as.numeric)



#Rename some of the variables
omop_visit_format_all <- omop_visit_clean_all %>%
                          dplyr::rename(
                            "Patients"="person_id",
                            "Hospital_encounters"="visit_occurrence_id",
                            "Sex"="gender_concept_name",
                            "Year of birth"="year_of_birth",
                            "Age (years)"="age_years",
                            "Ethnicity"="ethnicity_group",
                            "Length of stay (days)"="patient_days",
                            "Level 2/3 care"="l2_l3",
                            "covid"="covid",
                            "Hospital"="hospital_site",
                            "Admitted from"="admitting_source_concept_name",
                            "Outcomes"="discharge_to_concept_name",
                            "Body mass index (kg/m2)"="BMI")

#Summarise only the select number of visit-level variables that are available in the omop_visit_format_all table
#and the remaining visit-level queries require bespoke queries of which some  are done in separate documents.
CreateTableOne(data=omop_visit_format_all,
               vars=colnames(omop_visit_format_all)[which(!(colnames(omop_visit_format_all) %in% c("Patients", "Sex", "Ethnicity", "Hospital", "covid", "visit_start_date", "visit_end_date", "Hospital_encounters")))],
               strata=c("covid", "Hospital"), addOverall=T, test=F, includeNA=T) %>% print(showAllLevels=T, nonnormal=c("Body mass index (kg/m2)","Year of birth", "Age (years)","Length of stay (days)", "EHR problem list entry", "Past medical history", "Consultant episode level", "Vist level", "EHRProblemList_0s", "PastMedicalHistory_0s", "ConsultantEpisodeLevel_0s", "HospitalVisitLevel_0s"), noSpaces=T) %>%
                write.csv("visit_data_summary_covid_all_Paper.csv", row.names=T, na="")


#Text in Data Records
#Note, the length of stay was converted from decimal days to days and hours manually
text_all <- paste0(n_distinct(omop_covid_pcr), " (", round(n_distinct(omop_covid_pcr) / n_distinct(omop_visit_all$visit_occurrence_id)*100,2), "%) of visits tested positive for SARS-CoV-2, with a further ", omop_covid_all$visit_occurrence_id[!(omop_covid_all$visit_occurrence_id %in% omop_covid_pcr$visit_occurrence_id)] %>% length(), " having a clinical diagnosis but no positive test. Of those who tested positive for SARS-CoV-2, the in-hospital mortality was ",
                   round(((omop_visit_clean_all %>% filter(covid=="Yes") %>% filter(discharge_to_concept_name=="Died") %>% pull(person_id) %>% n_distinct()) / (omop_visit_clean_all %>% filter(covid=="Yes") %>% pull(person_id) %>% n_distinct()))*100, 2), "%, with a median (Q1-Q3) length of stay of ", round(omop_visit_clean_all %>% filter(covid=="Yes") %>%  pull(patient_days) %>% median(na.rm=T), 2), " days (", paste(round((omop_visit_clean_all %>% filter(covid=="Yes") %>%  pull(patient_days) %>% quantile(na.rm=T))[2],2)), "-", paste(round((omop_visit_clean_all %>% filter(covid=="Yes") %>%  pull(patient_days) %>% quantile(na.rm=T))[4], 2)), "). There were ", omop_visit_clean_all %>% filter(covid=="Yes") %>% filter(l2_l3=="Yes") %>% pull(visit_occurrence_id) %>% n_distinct(), " total admissions to level 2 and/or 3 units. The data set runs from ", min(omop_visit_all$visit_start_date), " to ", max(c(omop_visit_all$visit_start_date, omop_visit_all$visit_end_date), na.rm=T), ".")


#Patient-level Summary Data

#Could technically use the visit data above, but instead let's query
#the person table.
person_level_table <- paste0("SELECT a.person_id,
                                     b.visit_count,
                                    c.gender_concept_name,
                                    d.race_concept_name,
                                    e.LSOA_code,
                                    f.covid
                             FROM
                             (SELECT person_id,
                                     race_concept_id,
                                     gender_concept_id,
                                     location_id
                             FROM person) a
                             LEFT JOIN
                             (SELECT person_id,
                                     COUNT(visit_occurrence_id) as visit_count
                             FROM  visit_occurrence
                             GROUP BY person_id) b
                             ON a.person_id=b.person_id
                             LEFT JOIN
                             (SELECT concept_id as gender_concept_id,
                                      concept_name as gender_concept_name
                              FROM concept) c
                              ON a.gender_concept_id=c.gender_concept_id
                               LEFT JOIN
                             (SELECT concept_id as race_concept_id,
                                      concept_name as race_concept_name
                              FROM concept) d
                              ON a.race_concept_id=d.race_concept_id
                              LEFT JOIN
                             (SELECT location_id,
                                      zip as LSOA_code
                              FROM location) e
                             ON a.location_id=e.location_id
                             LEFT JOIN
                             (SELECT  DISTINCT person_id,
                                      COUNT(DISTINCT person_id) as covid
                             FROM visit_occurrence
                             WHERE visit_occurrence_id IN (",paste0(paste0("'", covid_case_type %>%pull(visit_occurrence_id), "'", collapse=",")), ")
                             GROUP BY person_id) f
                             ON a.person_id=f.person_id")


omop_person_table <- dbGetQueryBothTrusts(db, person_level_table) %>%
                      distinct() %>%
                      mutate(gender_concept_name=tolower(gender_concept_name) %>% Hmisc::capitalize(),
                              gender_concept_name=ifelse(gender_concept_name=="Unknown" | gender_concept_name=="No matching concept", "Unknown", gender_concept_name),
                              ethnicity_group=case_when(
                                grepl(race_concept_name, pattern="Asian or Asian British:") ~ "Asian",
                                grepl(race_concept_name, pattern="Black or African or Caribbean or Black British:") ~ "Black",
                                grepl(race_concept_name, pattern="Mixed multiple ethnic groups:") ~ "Mixed",
                                grepl(race_concept_name, pattern="White:") ~ "White",
                                grepl(race_concept_name, pattern="Other ethnic group:") ~ "Other",
                                race_concept_name=="Ethnicity not stated" | race_concept_name=="Unknown racial group" ~ "Unknown"),
                                race_concept_name=NULL,
                             covid=ifelse(is.na(covid), "No", "Yes"),
                             hospital_site = ifelse(schema=="uhb", "UHB", "UCLH"),
                             visit_count=as.numeric(visit_count))


#Read in Index of Multiple Deprivation (IMD) data. England only
IMD <- read.csv("IMD.csv")

#Convert the LSOA (2011) codes to character for the join
IMD$lsoa11cd = as.character(IMD$lsoa11cd)

#Join to the omop_person_table created above.
omop_person_table <- left_join(omop_person_table, IMD %>% select(lsoa11cd, IMD_Decile), by=c("lsoa_code"="lsoa11cd"))

#Each country in the UK has their own IMD measure.The IMD we use is for England,
#which should be fine, as nearly 98% of the patients are from England.
#Convert the deciles to quintiles (Q)
omop_person_table$IMD_Q = if_else(is.na(omop_person_table$IMD_Decile), "NA",
                                if_else(omop_person_table$IMD_Decile>=9, "5",
                                        if_else(omop_person_table$IMD_Decile<9 & omop_person_table$IMD_Decile >6, "4",
                                                if_else(omop_person_table$IMD_Decile<7 & omop_person_table$IMD_Decile>4, "3",
                                                        if_else(omop_person_table$IMD_Decile<5 & omop_person_table$IMD_Decile>2, "2",
                                                                if_else(omop_person_table$IMD_Decile<3 & omop_person_table$IMD_Decile>=1,"1", "NA"))))))

#Write summary data for person-/patient-level
CreateTableOne(data=omop_person_table,
               vars=colnames(omop_person_table)[which(!(colnames(omop_person_table) %in% c("person_id", "lsoa_code", "covid", "hospital_site", "IMD_Decile")))],
               strata=c("covid", "hospital_site"), addOverall=T, test=F, includeNA=T) %>% print(showAllLevels=T, nonnormal=c("gender_concept_name", "ethnicity_group", "visit_count"), noSpaces=T) %>%
  write.csv("person_data_summary_July2022.csv", row.names=T, na="")

