##############################################################################################
#################### TABLE 1c Ventilation - DECOVID Data Descriptor Paper ####################
##############################################################################################
#Load libraries
#uncomment below if no packages have been installed previously.
#install.packages("DBI", "dplyr", "stringr", "purrr", "zoo")
library(DBI) #for performing SQL queries
library(dplyr) #for data manipulation
library(stringr)
library(zoo)
library(purrr)

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


## 2. Query database
# `vent_value_char` - this will extract all information for the measurement_concept_ids of interest that map to **concepts**. Missing visit_occurrence_ids will be dropped.

# `vent_value_num` - this will extract all information for the measurement_concept_ids of interest that map to **values**. Missing visit_occurrence_ids will be dropped. NA values will
#be kept for now


vent_value_char <- paste("SELECT visit_occurrence_id, 
                                 dt, 
                                 measurement_concept_id, 
                                 concept_name, 
                                 value_as_concept_id  
                                 FROM
                    (SELECT measurement_concept_id, 
                            measurement_datetime as dt, 
                            value_as_concept_id, 
                            visit_occurrence_id 
                     FROM omop_03082021.measurement 
                     WHERE measurement_concept_id IN (4230167, 4183713, 4222965)) a
                     INNER JOIN
                     (SELECT concept_name, 
                            concept_id as value_as_concept_id 
                            FROM omop_03082021.concept) b
                    USING (value_as_concept_id) WHERE visit_occurrence_id IS NOT NULL")

vent_value_num <-  paste("SELECT visit_occurrence_id, 
                                 measurement_concept_id, 
                                 concept_name,dt, 
                                 value_as_number 
                                 FROM 
                        (SELECT visit_occurrence_id,
                                measurement_concept_id, 
                                measurement_datetime as dt, 
                                value_as_number
                         FROM omop_03082021.measurement 
                         WHERE measurement_concept_id IN (4353621, 4220163, 3020716, 4141684, 4215838, 4216746, 37208377)) a
                        INNER JOIN
                        (SELECT concept_name, 
                                concept_id as measurement_concept_id 
                                FROM omop_03082021.concept) b
                        USING (measurement_concept_id) 
                         WHERE visit_occurrence_id IS NOT NULL")


vent_vent_char <- dbGetQuery(vent, vent_value_char) %>% 
  mutate(value_as_concept_id = as.numeric(value_as_concept_id))

copd_vent_char <- dbGetQuery(copd, vent_value_char) %>% 
  mutate(value_as_concept_id = as.numeric(value_as_concept_id))

coag_vent_char <- dbGetQuery(coag, vent_value_char) %>% 
  mutate(value_as_concept_id = as.numeric(value_as_concept_id))

news2_vent_char <- dbGetQuery(news2, vent_value_char) %>% 
  mutate(value_as_concept_id = as.numeric(value_as_concept_id))


vent_vent_num <- dbGetQuery(vent, vent_value_num)

copd_vent_num <- dbGetQuery(copd, vent_value_num)

coag_vent_num <- dbGetQuery(coag, vent_value_num)

news2_vent_num <- dbGetQuery(news2, vent_value_num)

#Check for NAs
sum(is.na(vent_vent_char))
sum(is.na(vent_vent_num))

sum(is.na(copd_vent_char)) 
sum(is.na(copd_vent_num)) 

sum(is.na(coag_vent_char)) 
sum(is.na(coag_vent_num)) 

sum(is.na(news2_vent_char)) 
sum(is.na(news2_vent_num)) 


## 3. Ventilation
#### 3.1 Ventilation mode

This code block will map vent_mode values to either "ventilation" or "no ventilation". 

# Ventilation - Pressure controlled ventilation, Continuous positive airway pressure/Bilevel 
# positive airway pressure mask, High frequency oscillatory ventilation, Home CPAP unit, 
# Emergency continuous positive airway pressure device, Pressure controlled SIMV, 
# Volume controlled ventilation, Inspiration mandatory ventilation therapy, 
# Patient triggered inspiratory assistance, initiation and management (not in results as all missing visit_occurrence_ids), 
# High frequency jet ventilation (not in results as all missing visit_occurrence_ids).

# No ventilation - Normal spontaneous respiration

vent_concept_id <- c(45765273, 37396684, 45768222, 4208272, 4055377, 4074666, 4120570, 4236738, 4113618, 4055374)

no_vent_concept_id <- c(4174578)

vent_vent_mode <- vent_vent_char %>% 
                  filter(measurement_concept_id == 4230167) %>%
                  filter(value_as_concept_id != 4239130) %>%
                  rename(vent_mode_dt = dt) %>%
                  mutate(value_as_concept_id = as.numeric(value_as_concept_id)) %>%
                  mutate(vent_mode = case_when(
                    value_as_concept_id %in% vent_concept_id  ~ "ventilation",
                    value_as_concept_id %in% no_vent_concept_id ~ "no ventilation")) %>%
                  select(visit_occurrence_id, vent_mode_dt, raw_value = concept_name , vent_mode)

copd_vent_mode <- copd_vent_char %>% 
                  filter(measurement_concept_id == 4230167) %>%
                  filter(value_as_concept_id != 4239130) %>%
                  rename(vent_mode_dt = dt) %>%
                  mutate(value_as_concept_id = as.numeric(value_as_concept_id)) %>%
                  mutate(vent_mode = case_when(
                    value_as_concept_id %in% vent_concept_id  ~ "ventilation",
                    value_as_concept_id %in% no_vent_concept_id ~ "no ventilation"))  %>%
                  select(visit_occurrence_id, vent_mode_dt, raw_value = concept_name , vent_mode)

coag_vent_mode <- coag_vent_char %>% 
                  filter(measurement_concept_id == 4230167) %>%
                  filter(value_as_concept_id != 4239130) %>%
                  rename(vent_mode_dt = dt) %>%
                  mutate(value_as_concept_id = as.numeric(value_as_concept_id)) %>%
                  mutate(vent_mode = case_when(
                    value_as_concept_id %in% vent_concept_id  ~ "ventilation",
                    value_as_concept_id %in% no_vent_concept_id ~ "no ventilation"))  %>%
                  select(visit_occurrence_id, vent_mode_dt, raw_value = concept_name , vent_mode)

news2_vent_mode <- news2_vent_char %>% 
                    filter(measurement_concept_id == 4230167) %>%
                    filter(value_as_concept_id != 4239130) %>%
                    rename(vent_mode_dt = dt) %>%
                    mutate(value_as_concept_id = as.numeric(value_as_concept_id)) %>%
                    mutate(vent_mode = case_when(
                      value_as_concept_id %in% vent_concept_id  ~ "ventilation",
                      value_as_concept_id %in% no_vent_concept_id ~ "no ventilation"))  %>%
                    select(visit_occurrence_id, vent_mode_dt, raw_value = concept_name , vent_mode)

#Check NAs
sum(is.na(vent_vent_mode))
sum(is.na(copd_vent_mode)) 
sum(is.na(coag_vent_mode))
sum(is.na(news2_vent_mode)) 


#### 3.2 Ventilation setting
@This code block will:
  
#1. Define gas volume as "positive" if either tidal volume *OR* 
#minute volume are not NA, and "zero" otherwise. 
#(Data quality check revealed no values exist below 0 in the dataset).

#2. Define pressure settings as "psv" if PEEP *OR* ps are >0, and "cpap" 
#if both are 0. 

#3.Combine these into a "vent_set" variable. 
#If gas_vol is positive *OR* pressure setting is "psv" *OR* 
#"cpap" (i.e. a value exists for pressure setting), 
#then "ventilation". No pressure setting *AND* and gas_vol of "zero" will be mapped to "no ventilation".

#### 3.2.1 Gas volume
vent_gas_vol <- vent_vent_num %>% 
  filter(measurement_concept_id == 4220163 | 
           measurement_concept_id == 4353621) %>%
  rename(gas_vol_dt = dt) %>%
  mutate(
    gas_vol = value_as_number,
    raw_value=str_c(concept_name, ": ", if_else(is.na(value_as_number), "NA", as.character(value_as_number)))
  ) %>%
  select(visit_occurrence_id,gas_vol_dt, 
         raw_value, gas_vol) %>% 
  arrange(visit_occurrence_id, gas_vol_dt, raw_value) %>% 
  group_by(visit_occurrence_id, gas_vol_dt) %>% 
  summarise( # Combine measurements taken at the exact same datetime
    raw_value = str_c(unique(raw_value), collapse = "; "),
    gas_vol = case_when(
      any(!is.na(gas_vol)) ~ "positive",
      TRUE ~ "zero"
    ),
    .groups = "drop"
  )

vent = vent_gas_vol %>%
  filter(visit_occurrence_id==1324200006)
copd_gas_vol <- copd_vent_num %>% 
  filter(measurement_concept_id == 4220163 | 
           measurement_concept_id == 4353621) %>%
  rename(gas_vol_dt = dt) %>%
  mutate(
    gas_vol = value_as_number,
    raw_value=str_c(concept_name, ": ", if_else(is.na(value_as_number), "NA", as.character(value_as_number)))
  ) %>%
  select(visit_occurrence_id,gas_vol_dt, 
         raw_value, gas_vol) %>% 
  arrange(visit_occurrence_id, gas_vol_dt, raw_value) %>% 
  group_by(visit_occurrence_id, gas_vol_dt) %>% 
  summarise( # Combine measurements taken at the exact same datetime
    raw_value = str_c(unique(raw_value), collapse = "; "),
    gas_vol = case_when(
      any(!is.na(gas_vol)) ~ "positive",
      TRUE ~ "zero"
    ),
    .groups = "drop"
  )

coag_gas_vol <- coag_vent_num %>% 
  filter(measurement_concept_id == 4220163 | 
           measurement_concept_id == 4353621) %>%
  rename(gas_vol_dt = dt) %>%
  mutate(
    gas_vol = value_as_number,
    raw_value=str_c(concept_name, ": ", if_else(is.na(value_as_number), "NA", as.character(value_as_number)))
  ) %>%
  select(visit_occurrence_id,gas_vol_dt, 
         raw_value, gas_vol) %>% 
  arrange(visit_occurrence_id, gas_vol_dt, raw_value) %>% 
  group_by(visit_occurrence_id, gas_vol_dt) %>% 
  summarise( # Combine measurements taken at the exact same datetime
    raw_value = str_c(unique(raw_value), collapse = "; "),
    gas_vol = case_when(
      any(!is.na(gas_vol)) ~ "positive",
      TRUE ~ "zero"
    ),
    .groups = "drop"
  )

news2_gas_vol <- news2_vent_num %>% 
  filter(measurement_concept_id == 4220163 | 
           measurement_concept_id == 4353621) %>%
  rename(gas_vol_dt = dt) %>%
  mutate(
    gas_vol = value_as_number,
    raw_value=str_c(concept_name, ": ", if_else(is.na(value_as_number), "NA", as.character(value_as_number)))
  ) %>%
  select(visit_occurrence_id,gas_vol_dt, 
         raw_value, gas_vol) %>% 
  arrange(visit_occurrence_id, gas_vol_dt, raw_value) %>% 
  group_by(visit_occurrence_id, gas_vol_dt) %>% 
  summarise( # Combine measurements taken at the exact same datetime
    raw_value = str_c(unique(raw_value), collapse = "; "),
    gas_vol = case_when(
      any(!is.na(gas_vol)) ~ "positive",
      TRUE ~ "zero"
    ),
    .groups = "drop"
  )


#### 3.2.2 Pressure settings
#This will extract the pressure settings. 
#The criteria is PS>0 for PSV, PEEP>=0 AND/OR PS=0|NULL. 
#Any records where PS AND PEEP are both NA are excluded, or if PS=0 AND is.na(PEEP).


vent_press_set <- vent_vent_num %>% 
  filter(measurement_concept_id == 4215838 | 
           measurement_concept_id == 4216746,
         !is.na(value_as_number)) %>% 
  rename(press_set_dt = dt) %>%
  mutate(
    press_set = case_when(
      measurement_concept_id == 4215838 & value_as_number > 0 & !is.na(value_as_number) ~ "psv",
      ((measurement_concept_id == 4216746) & (value_as_number >=0) | ((measurement_concept_id == 4215838) & (value_as_number ==0))) ~ "cpap",
      TRUE ~ "unknown"
    ),
    raw_value = str_c(concept_name, ": ", if_else(is.na(value_as_number), "NA", as.character(value_as_number)))
  ) %>% 
  select(visit_occurrence_id, press_set_dt, 
         raw_value, press_set) %>% 
  arrange(visit_occurrence_id, press_set_dt, raw_value) %>% 
  group_by(visit_occurrence_id, press_set_dt) %>% 
  summarise( # Combine measurements taken at the exact same datetime
    raw_value = str_c(unique(raw_value), collapse = "; "),
    press_set = case_when(
      any(press_set == "psv") ~ "psv",
      any(press_set== "cpap") ~ "cpap",
      TRUE ~ "Unknown"),
    .groups = "drop"
  ) %>%
  filter(!(raw_value=="Inspiratory pressure setting: 0" & press_set=="cpap")) %>%
  filter(press_set!="Unknown")


copd_press_set <- copd_vent_num %>% 
  filter(measurement_concept_id == 4215838 | 
           measurement_concept_id == 4216746,
         !is.na(value_as_number)) %>% 
  rename(press_set_dt = dt) %>%
  mutate(
    press_set = case_when(
      measurement_concept_id == 4215838 & value_as_number > 0 & !is.na(value_as_number) ~ "psv",
      ((measurement_concept_id == 4216746) & (value_as_number >=0) | ((measurement_concept_id == 4215838) & (value_as_number ==0))) ~ "cpap",
      TRUE ~ "unknown"
    ),
    raw_value = str_c(concept_name, ": ", if_else(is.na(value_as_number), "NA", as.character(value_as_number)))
  ) %>% 
  select(visit_occurrence_id, press_set_dt, 
         raw_value, press_set) %>% 
  arrange(visit_occurrence_id, press_set_dt, raw_value) %>% 
  group_by(visit_occurrence_id, press_set_dt) %>% 
  summarise( # Combine measurements taken at the exact same datetime
    raw_value = str_c(unique(raw_value), collapse = "; "),
    press_set = case_when(
      any(press_set == "psv") ~ "psv",
      any(press_set== "cpap") ~ "cpap",
      TRUE ~ "Unknown"),
    .groups = "drop"
  ) %>%
  filter(!(raw_value=="Inspiratory pressure setting: 0" & press_set=="cpap")) %>%
  filter(press_set!="Unknown")


coag_press_set <- coag_vent_num %>% 
  filter(measurement_concept_id == 4215838 | 
           measurement_concept_id == 4216746,
         !is.na(value_as_number)) %>% 
  rename(press_set_dt = dt) %>%
  mutate(
    press_set = case_when(
      measurement_concept_id == 4215838 & value_as_number > 0 & !is.na(value_as_number) ~ "psv",
      ((measurement_concept_id == 4216746) & (value_as_number >=0) | ((measurement_concept_id == 4215838) & (value_as_number ==0))) ~ "cpap",
      TRUE ~ "unknown"
    ),
    raw_value = str_c(concept_name, ": ",if_else(is.na(value_as_number), "NA", as.character(value_as_number)))
  ) %>% 
  select(visit_occurrence_id, press_set_dt, 
         raw_value, press_set) %>% 
  arrange(visit_occurrence_id, press_set_dt, raw_value) %>% 
  group_by(visit_occurrence_id, press_set_dt) %>% 
  summarise( # Combine measurements taken at the exact same datetime
    raw_value = str_c(unique(raw_value), collapse = "; "),
    press_set = case_when(
      any(press_set == "psv") ~ "psv",
      any(press_set== "cpap") ~ "cpap",
      TRUE ~ "Unknown"),
    .groups = "drop"
  ) %>%
  filter(!(raw_value=="Inspiratory pressure setting: 0" & press_set=="cpap")) %>%
  filter(press_set!="Unknown")


news2_press_set <- news2_vent_num %>% 
  filter(measurement_concept_id == 4215838 | 
           measurement_concept_id == 4216746,
         !is.na(value_as_number)) %>% 
  rename(press_set_dt = dt) %>%
  mutate(
    press_set = case_when(
      measurement_concept_id == 4215838 & value_as_number > 0 & !is.na(value_as_number) ~ "psv",
      ((measurement_concept_id == 4216746) & (value_as_number >=0) | ((measurement_concept_id == 4215838) & (value_as_number ==0))) ~ "cpap",
      TRUE ~ "unknown"
    ),
    raw_value = str_c(concept_name, ": ", if_else(is.na(value_as_number), "NA", as.character(value_as_number)))
  ) %>% 
  select(visit_occurrence_id, press_set_dt, 
         raw_value, press_set) %>% 
  arrange(visit_occurrence_id, press_set_dt, raw_value) %>% 
  group_by(visit_occurrence_id, press_set_dt) %>% 
  summarise( # Combine measurements taken at the exact same datetime
    raw_value = str_c(unique(raw_value), collapse = "; "),
    press_set = case_when(
      any(press_set == "psv") ~ "psv",
      any(press_set== "cpap") ~ "cpap",
      TRUE ~ "Unknown"),
    .groups = "drop"
  ) %>%
  filter(!(raw_value=="Inspiratory pressure setting: 0" & press_set=="cpap")) %>%
  filter(press_set!="Unknown")


#### 3.2.3 Combine
vent_vent_set <- bind_rows(
                vent_gas_vol %>% 
                  rename(vent_set_dt = gas_vol_dt) %>% 
                  mutate(vent_set = if_else(
                    gas_vol == "positive", "ventilation", "no ventilation"
                  )),
                vent_press_set %>% 
                  rename(vent_set_dt = press_set_dt) %>% 
                  mutate(vent_set = "ventilation")
              ) %>% 
                select(visit_occurrence_id, vent_set_dt, raw_value, vent_set) %>% 
                arrange(visit_occurrence_id, vent_set_dt, raw_value) %>% 
                group_by(visit_occurrence_id, vent_set_dt) %>% 
                summarise( # Combine measurements taken at the exact same datetime
                  raw_value = str_c(unique(raw_value), collapse = "; "),
                  vent_set = case_when(
                    any(vent_set == "ventilation") ~ "ventilation",
                    TRUE ~ "no ventilation"
                  ),
                  .groups = "drop"
                )

copd_vent_set <- bind_rows(
                  copd_gas_vol %>% 
                    rename(vent_set_dt = gas_vol_dt) %>% 
                    mutate(vent_set = if_else(
                      gas_vol == "positive", "ventilation", "no ventilation"
                    )),
                  copd_press_set %>% 
                    rename(vent_set_dt = press_set_dt) %>% 
                    mutate(vent_set = "ventilation")
                ) %>% 
                  select(visit_occurrence_id, vent_set_dt, raw_value, vent_set) %>% 
                  arrange(visit_occurrence_id, vent_set_dt, raw_value) %>% 
                  group_by(visit_occurrence_id, vent_set_dt) %>% 
                  summarise( # Combine measurements taken at the exact same datetime
                    raw_value = str_c(unique(raw_value), collapse = "; "),
                    vent_set = case_when(
                      any(vent_set == "ventilation") ~ "ventilation",
                      TRUE ~ "no ventilation"
                    ),
                    .groups = "drop"
                  )

coag_vent_set <- bind_rows(
                  coag_gas_vol %>% 
                    rename(vent_set_dt = gas_vol_dt) %>% 
                    mutate(vent_set = if_else(
                      gas_vol == "positive", "ventilation", "no ventilation"
                    )),
                  coag_press_set %>% 
                    rename(vent_set_dt = press_set_dt) %>% 
                    mutate(vent_set = "ventilation")
                ) %>% 
                  select(visit_occurrence_id, vent_set_dt, raw_value, vent_set) %>% 
                  arrange(visit_occurrence_id, vent_set_dt, raw_value) %>% 
                  group_by(visit_occurrence_id, vent_set_dt) %>% 
                  summarise( # Combine measurements taken at the exact same datetime
                    raw_value = str_c(unique(raw_value), collapse = "; "),
                    vent_set = case_when(
                      any(vent_set == "ventilation") ~ "ventilation",
                      TRUE ~ "no ventilation"
                    ),
                    .groups = "drop"
                  )

news2_vent_set <- bind_rows(
                    news2_gas_vol %>% 
                      rename(vent_set_dt = gas_vol_dt) %>% 
                      mutate(vent_set = if_else(
                        gas_vol == "positive", "ventilation", "no ventilation"
                      )),
                    news2_press_set %>% 
                      rename(vent_set_dt = press_set_dt) %>% 
                      mutate(vent_set = "ventilation")
                  ) %>% 
                    select(visit_occurrence_id, vent_set_dt, raw_value, vent_set) %>% 
                    arrange(visit_occurrence_id, vent_set_dt, raw_value) %>% 
                    group_by(visit_occurrence_id, vent_set_dt) %>% 
                    summarise( # Combine measurements taken at the exact same datetime
                      raw_value = str_c(unique(raw_value), collapse = "; "),
                      vent_set = case_when(
                        any(vent_set == "ventilation") ~ "ventilation",
                        TRUE ~ "no ventilation"
                      ),
                      .groups = "drop"
                    )

#Check NAs
sum(is.na(vent_vent_set))
sum(is.na(copd_vent_set))
sum(is.na(coag_vent_set))
sum(is.na(news2_vent_set))


#### 3.3. Airway
#This code block will map airway values to either "invasive" or "no device".

#Invasive - Laryngeal mask, Tracheostomy tube, Esophageal tracheal double 
#lumen supraglottic airway device, Supraglottic airway, Endotracheal tube

#No device -Nasopharyngeal airway device, None, Oropharyngeal airway device

invasive_concept_id <- c(4044008, 4097216, 37017286, 4106029, 4329254)
no_device_concept_id <- c(4139134, 4266238, 4124462)


vent_airway <- vent_vent_char %>% 
  filter(measurement_concept_id == 4183713) %>%
  filter(value_as_concept_id != 4089217) %>%
  rename(airway_dt = dt) %>%
   mutate(airway = case_when(
    value_as_concept_id %in% invasive_concept_id  ~ "invasive",
    value_as_concept_id %in% no_device_concept_id ~ "no device")) %>%
  select(visit_occurrence_id, airway_dt, raw_value=concept_name, airway)

copd_airway <- copd_vent_char %>% 
  filter(measurement_concept_id == 4183713) %>%
  filter(value_as_concept_id != 4089217) %>%
  rename(airway_dt = dt) %>%
   mutate(airway = case_when(
    value_as_concept_id %in% invasive_concept_id  ~ "invasive",
    value_as_concept_id %in% no_device_concept_id ~ "no device")) %>%
  select(visit_occurrence_id, airway_dt, raw_value=concept_name, airway)

coag_airway <- coag_vent_char %>% 
  filter(measurement_concept_id == 4183713) %>%
  filter(value_as_concept_id != 4089217) %>%
  rename(airway_dt = dt) %>%
   mutate(airway = case_when(
    value_as_concept_id %in% invasive_concept_id  ~ "invasive",
    value_as_concept_id %in% no_device_concept_id ~ "no device")) %>%
  select(visit_occurrence_id, airway_dt, raw_value=concept_name, airway)

news2_airway <- news2_vent_char %>% 
  filter(measurement_concept_id == 4183713) %>%
  filter(value_as_concept_id != 4089217) %>%
  rename(airway_dt = dt) %>%
   mutate(airway = case_when(
    value_as_concept_id %in% invasive_concept_id  ~ "invasive",
    value_as_concept_id %in% no_device_concept_id ~ "no device")) %>%
  select(visit_occurrence_id, airway_dt, raw_value=concept_name, airway)

#Check NAs
sum(is.na(vent_airway))
sum(is.na(copd_airway))
sum(is.na(coag_airway))
sum(is.na(news2_airway))

#### 3.4 Oxygen delivery device
#This code block will map oxygen delivery device values to "invasive", 
#"non-invasive", "simple mask", or "no device".

#Invasive - Tracheostomy tube, Endotracheal tube

#Non-invasive - Oxyhood, Bag valve mask ventilation, Continuous positive airway 
#pressure/Bilevel positive airway pressure mask

#Simple mask - Oxygen nasal cannula, Humidified oxygen therapy, 
#Tracheostomy mask, oxygen, Nebulizer, Oxygen mask, Nonrebreather oxygen mask,
#High flow oxygen nasal cannula, Venturi oxygen face mask 

#No device - None

invasive_concept_id <- c(4097216, 4044008)
non_invasive_concept_id <- c(45768222, 4139542, 4145529)
simple_mask_concept_id <- c(45759930, 4219814, 4119964, 4145528, 45760219, 4222966, 4224038)
HFNO_concept_id  <- c( 4139525)
no_device_concept_id <- c(4124462)

vent_oxy_del_dev <- vent_vent_char %>% 
  filter(measurement_concept_id == 4222965) %>%
  rename(o2_dev_dt = dt) %>%
  mutate(o2_dev = case_when(
    value_as_concept_id %in% invasive_concept_id  ~ "invasive",
    value_as_concept_id %in% non_invasive_concept_id ~ "non-invasive",
    value_as_concept_id %in% simple_mask_concept_id ~ "simple mask",
    value_as_concept_id %in% HFNO_concept_id ~ "HFNO",
    value_as_concept_id %in% no_device_concept_id ~ "no device")) %>%
  select(visit_occurrence_id, o2_dev_dt , raw_value=concept_name, o2_dev)

copd_oxy_del_dev <- copd_vent_char %>% 
  filter(measurement_concept_id == 4222965) %>%
  rename(o2_dev_dt = dt) %>%
  mutate(o2_dev = case_when(
    value_as_concept_id %in% invasive_concept_id  ~ "invasive",
    value_as_concept_id %in% non_invasive_concept_id ~ "non-invasive",
    value_as_concept_id %in% simple_mask_concept_id ~ "simple mask",
    value_as_concept_id %in% HFNO_concept_id ~ "HFNO",
    value_as_concept_id %in% no_device_concept_id ~ "no device")) %>%
  select(visit_occurrence_id, o2_dev_dt , raw_value=concept_name, o2_dev)

coag_oxy_del_dev <- coag_vent_char %>% 
  filter(measurement_concept_id == 4222965) %>%
  rename(o2_dev_dt = dt) %>%
  mutate(o2_dev = case_when(
    value_as_concept_id %in% invasive_concept_id  ~ "invasive",
    value_as_concept_id %in% non_invasive_concept_id ~ "non-invasive",
    value_as_concept_id %in% simple_mask_concept_id ~ "simple mask",
    value_as_concept_id %in% HFNO_concept_id ~ "HFNO",
    value_as_concept_id %in% no_device_concept_id ~ "no device")) %>%
  select(visit_occurrence_id, o2_dev_dt , raw_value=concept_name, o2_dev)

news2_oxy_del_dev <- news2_vent_char %>% 
  filter(measurement_concept_id == 4222965) %>%
  rename(o2_dev_dt = dt) %>%
  mutate(o2_dev = case_when(
    value_as_concept_id %in% invasive_concept_id  ~ "invasive",
    value_as_concept_id %in% non_invasive_concept_id ~ "non-invasive",
    value_as_concept_id %in% simple_mask_concept_id ~ "simple mask",
    value_as_concept_id %in% HFNO_concept_id ~ "HFNO",
    value_as_concept_id %in% no_device_concept_id ~ "no device")) %>%
  select(visit_occurrence_id, o2_dev_dt , raw_value=concept_name, o2_dev)

#Check NAs
sum(is.na(vent_oxy_del_dev))
sum(is.na(copd_oxy_del_dev))
sum(is.na(coag_oxy_del_dev))
sum(is.na(news2_oxy_del_dev))

#### 3.5 Use of oxygen
#This code block will:

#1. Filter out any fio2 measures <0.21

#2. If fio2 is >0.21 *OR* lpm > 0 then map o2 to "o2". 
#Otherwise, map to "no o2" (ie. fio2 = 0.21 & lpm = 0)

vent_oxy_use <- vent_vent_num %>% 
  filter(measurement_concept_id == 3020716 | 
           measurement_concept_id == 4141684) %>%
   filter(!(measurement_concept_id == 3020716 & value_as_number <0.21)) %>%
    filter(  !is.na(value_as_number)) %>%
   rename(o2_dt = dt) %>% 
   mutate(o2 = if_else(
    measurement_concept_id == 3020716 & value_as_number >0.21 |
      measurement_concept_id == 4141684 & value_as_number >0, 1, 0)) %>%
  mutate(raw_value = str_c(concept_name, ": ", value_as_number)) %>% 
  select(visit_occurrence_id, o2_dt, raw_value, o2) %>% 
  arrange(visit_occurrence_id, o2_dt, raw_value) %>% 
  group_by(visit_occurrence_id, o2_dt) %>% 
  summarise( # Combine measurements taken at the exact same datetime
    raw_value = str_c(unique(raw_value), collapse = "; "),
    o2 = case_when(
      any(o2>0) ~ "o2",
      TRUE ~ "no o2"
    ),
    .groups = "drop"
  )
  
copd_oxy_use <- copd_vent_num %>% 
  filter(measurement_concept_id == 3020716 | 
           measurement_concept_id == 4141684) %>%
   filter(!(measurement_concept_id == 3020716 & value_as_number <0.21)) %>%
    filter(  !is.na(value_as_number)) %>%
  rename(o2_dt = dt) %>% 
   mutate(o2 = ifelse(
    measurement_concept_id == 3020716 & value_as_number >0.21 |
      measurement_concept_id == 4141684 & value_as_number >0, 1, 0)) %>%
  mutate(raw_value = str_c(concept_name, ": ", value_as_number)) %>% 
  select(visit_occurrence_id, o2_dt, raw_value, o2) %>% 
  arrange(visit_occurrence_id, o2_dt, raw_value) %>% 
  group_by(visit_occurrence_id, o2_dt) %>% 
  summarise( # Combine measurements taken at the exact same datetime
    raw_value = str_c(unique(raw_value), collapse = "; "),
    o2 = case_when(
      any(o2>0) ~ "o2",
      TRUE ~ "no o2"
    ),
    .groups = "drop"
  )

coag_oxy_use <- coag_vent_num %>% 
  filter(measurement_concept_id == 3020716 | 
           measurement_concept_id == 4141684) %>%
   filter(!(measurement_concept_id == 3020716 & value_as_number <0.21)) %>%
  filter(  !is.na(value_as_number)) %>%
  rename(o2_dt = dt) %>% 
   mutate(o2 = ifelse(
    measurement_concept_id == 3020716 & value_as_number >0.21 |
      measurement_concept_id == 4141684 & value_as_number >0, 1, 0)) %>%
  mutate(raw_value = str_c(concept_name, ": ", value_as_number)) %>% 
  select(visit_occurrence_id, o2_dt, raw_value, o2) %>% 
  arrange(visit_occurrence_id, o2_dt, raw_value) %>% 
  group_by(visit_occurrence_id, o2_dt) %>% 
  summarise( # Combine measurements taken at the exact same datetime
    raw_value = str_c(unique(raw_value), collapse = "; "),
    o2 = case_when(
      any(o2>0) ~ "o2",
      TRUE ~ "no o2"
    ),
    .groups = "drop"
  )

news2_oxy_use <- news2_vent_num %>% 
  filter(measurement_concept_id == 3020716 | 
           measurement_concept_id == 4141684) %>%
   filter(!(measurement_concept_id == 3020716 & value_as_number <0.21)) %>%
    filter(  !is.na(value_as_number)) %>%
  rename(o2_dt = dt) %>% 
   mutate(o2 = ifelse(
    measurement_concept_id == 3020716 & value_as_number >0.21 |
      measurement_concept_id == 4141684 & value_as_number >0, 1, 0)) %>%
  mutate(raw_value = str_c(concept_name, ": ", value_as_number)) %>% 
  select(visit_occurrence_id, o2_dt, raw_value, o2) %>% 
  arrange(visit_occurrence_id, o2_dt, raw_value) %>% 
  group_by(visit_occurrence_id, o2_dt) %>% 
  summarise( # Combine measurements taken at the exact same datetime
    raw_value = str_c(unique(raw_value), collapse = "; "),
    o2 = case_when(
      any(o2>0) ~ "o2",
      TRUE ~ "no o2"
    ),
    .groups = "drop"
  )

#Check NAs
sum(is.na(vent_oxy_use))
sum(is.na(copd_oxy_use))
sum(is.na(coag_oxy_use))
sum(is.na(news2_oxy_use))

vent_oxy_flow <- vent_vent_num %>% 
  filter(measurement_concept_id == 4141684) %>%
    filter(!is.na(value_as_number)) %>%
   rename(o2FL_dt = dt) %>% 
  mutate(raw_value = str_c(concept_name, ": ", value_as_number)) %>% 
  select(visit_occurrence_id, o2FL_dt, raw_value) %>% 
  arrange(visit_occurrence_id, o2FL_dt, raw_value) %>% 
  group_by(visit_occurrence_id, o2FL_dt) %>% 
  dplyr::summarise( # Combine measurements taken at the exact same datetime
    raw_value = str_c(unique(raw_value), collapse = "; "),
    .groups = "drop"
  )
 vent_oxy_flow$o2FL=max(parse_number(vent_oxy_flow$raw_value))
 
copd_oxy_flow <- copd_vent_num %>% 
  filter(measurement_concept_id == 4141684) %>%
    filter(!is.na(value_as_number)) %>%
   rename(o2FL_dt = dt) %>% 
  mutate(raw_value = str_c(concept_name, ": ", value_as_number)) %>% 
  select(visit_occurrence_id, o2FL_dt, raw_value) %>% 
  arrange(visit_occurrence_id, o2FL_dt, raw_value) %>% 
  group_by(visit_occurrence_id, o2FL_dt) %>% 
  dplyr::summarise( # Combine measurements taken at the exact same datetime
    raw_value = str_c(unique(raw_value), collapse = "; "),
    .groups = "drop"
  )
 copd_oxy_flow$o2FL=max(parse_number(copd_oxy_flow$raw_value))

coag_oxy_flow <- coag_vent_num %>% 
  filter(measurement_concept_id == 4141684) %>%
    filter(!is.na(value_as_number)) %>%
   rename(o2FL_dt = dt) %>% 
  mutate(raw_value = str_c(concept_name, ": ", value_as_number)) %>% 
  select(visit_occurrence_id, o2FL_dt, raw_value) %>% 
  arrange(visit_occurrence_id, o2FL_dt, raw_value) %>% 
  group_by(visit_occurrence_id, o2FL_dt) %>% 
  dplyr::summarise( # Combine measurements taken at the exact same datetime
    raw_value = str_c(unique(raw_value), collapse = "; "),
    .groups = "drop"
  )
 coag_oxy_flow$o2FL=max(parse_number(coag_oxy_flow$raw_value))


news2_oxy_flow <- news2_vent_num %>% 
  filter(measurement_concept_id == 4141684) %>%
    filter(!is.na(value_as_number)) %>%
   rename(o2FL_dt = dt) %>% 
  mutate(raw_value = str_c(concept_name, ": ", value_as_number)) %>% 
  select(visit_occurrence_id, o2FL_dt, raw_value) %>% 
  arrange(visit_occurrence_id, o2FL_dt, raw_value) %>% 
  group_by(visit_occurrence_id, o2FL_dt) %>% 
  dplyr::summarise( # Combine measurements taken at the exact same datetime
    raw_value = str_c(unique(raw_value), collapse = "; "),
    .groups = "drop"
  )
 news2_oxy_flow$o2FL=max(parse_number(news2_oxy_flow$raw_value))

news_news_o2 <- news2_vent_num %>% 
  filter(measurement_concept_id == 37208377) %>%
  rename(news2_o2_dt = dt) %>%
  mutate(
    news2_o2 = value_as_number,
    raw_value=str_c(concept_name, ": ", if_else(is.na(value_as_number), "NA", as.character(value_as_number)))
    ) %>%
   select(visit_occurrence_id,news2_o2_dt, 
         raw_value, news2_o2) %>% 
  arrange(visit_occurrence_id, news2_o2_dt, raw_value) %>% 
  group_by(visit_occurrence_id, news2_o2_dt) %>% 
  summarise( # Combine measurements taken at the exact same datetime
    raw_value = str_c(unique(raw_value), collapse = "; "),
    news2_o2 = case_when(
      any(news2_o2==2) ~ "o2",
      TRUE ~ "no o2"
    ),
    .groups = "drop"
  )


vent_mode = rbind(vent_vent_mode, copd_vent_mode, coag_vent_mode, news2_vent_mode) %>% 
                    distinct()

vent_set = rbind(vent_vent_set, copd_vent_set, coag_vent_set, news2_vent_set) %>% 
                    distinct()

press_set = rbind(vent_press_set, copd_press_set, coag_press_set, news2_press_set)%>% 
                    distinct()

airway = rbind(vent_airway, copd_airway, coag_airway, news2_airway)%>% 
                    distinct()

oxygen_del_dev = rbind(vent_oxy_del_dev, copd_oxy_del_dev, coag_oxy_del_dev, news2_oxy_del_dev) %>% distinct()

oxygen = rbind(vent_oxy_use, copd_oxy_use, coag_oxy_use, news2_oxy_use)%>% 
                    distinct() 

oxygen_flow = rbind(vent_oxy_flow, copd_oxy_flow, coag_oxy_flow, news2_oxy_flow)%>% 
                    distinct() 
#issues with numbers 
oxygen_flow$o2FL = as.character(oxygen_flow$o2FL)

news2_o2 = news_news_o2

phenotype <- 
            list(vent_mode, vent_set, press_set, airway, oxygen_del_dev, oxygen,oxygen_flow, news2_o2) %>% 
            map(~ mutate(., dt = .[[2]])) %>% 
            bind_rows() %>% 
            select(visit_occurrence_id, dt, everything()) %>% 
            arrange(visit_occurrence_id, dt)


#Let's query the visit table, Care Site query
visit_occ_query <- paste0("SELECT visit_occurrence_id,
                                  visit_start_datetime,
                                  visit_end_datetime
                           FROM omop_03082021.visit_occurrence")
                                        
copd_visit_occ <- dbGetQuery(copd, visit_occ_query)

coag_visit_occ <- dbGetQuery(coag, visit_occ_query)

vent_visit_occ <- dbGetQuery(vent, visit_occ_query)

news2_visit_occ <- dbGetQuery(news2, visit_occ_query)
                                        
omop_visit_occurrence <- rbind(copd_visit_occ, coag_visit_occ, vent_visit_occ, news2_visit_occ) %>% 
                                          distinct()
                                     
                                        
                                        
#Join visit occurrence
phenotype <- phenotype %>% 
             inner_join(
             omop_visit_occurrence,
             by = "visit_occurrence_id") %>% 
             filter( visit_start_datetime <= dt,
                     dt <= visit_end_datetime) %>% 
            select(visit_occurrence_id, everything())

## start = 1 second before the earliest row of each patient
start_row <- phenotype %>% 
             group_by(visit_occurrence_id) %>% 
             summarise(dt = min(dt), .groups = "drop") %>% 
             mutate(dt = dt %m-% seconds(1))
                                        
## stop = 1 second after the latest row of each patient
stop_row <- phenotype %>% 
            group_by(visit_occurrence_id) %>% 
            summarise(dt = max(dt), .groups = "drop") %>% 
           mutate(dt = dt %m+% seconds(1))

## make sure `start_row` and `stop_row` have the same variables as 
## `phenotype`
for(i in names(phenotype)[3:ncol(phenotype)]){
                                          if(str_detect(i, "_dt$")) {
                                            start_row[[i]] <- start_row$dt
                                            stop_row[[i]] <- stop_row$dt
                                          } else if(i == "raw_value") {
                                            start_row[[i]] <- "start"
                                            stop_row[[i]] <- "stop"
                                          } else {
                                            start_row[[i]] <- "missing"
                                            stop_row[[i]] <- "stop"
                                          }
                                        }
                                        
## Add the start and stop rows and then carry all respiratory 
## measurements and their datetimes forward
phenotype %<>% 
select(-ends_with("_datetime")) %>% 
bind_rows(start_row, stop_row) %>% 
arrange(visit_occurrence_id, dt) %>% 
na.locf() 
                                        
## Combine all rows of a visit that relate to the exact same
## datetime by taking the last row of that time (which, because
## we carried all measurements forward, contains all measurements
## taken at that datetime)
phenotype %<>% 
select(-ends_with("_datetime")) %>% 
mutate(rn = row_number()) %>% 
arrange(visit_occurrence_id, dt, -rn) %>% 
filter(rn > lag(rn) | row_number() == 1)

# Classify ventilation status at each time point
phenotype2 = phenotype %>% mutate(
                                # 1. Does the evidence suggest the patient was on mechanical ventilation?
                                ventilated_dt = pmax(vent_set_dt, vent_mode_dt) , 
                                ventilated = 
                                ((vent_mode == "ventilation") & (vent_mode_dt == ventilated_dt)) |
                                ((vent_set  == "ventilation") & (vent_set_dt  == ventilated_dt)) |
                                ((press_set == "psv")          & (press_set_dt == ventilated_dt)) |
                                ((press_set == "cpap")       & (press_set_dt == ventilated_dt)),
                                          
                                # 2. Was the delivery method invasive, non-invasive (hood), or a simple mask?
                                delivery_dt = pmax(airway_dt, o2_dev_dt, press_set_dt),
                                delivery = case_when(
                                            ((airway    == "invasive")     & (airway_dt == delivery_dt))    |  
                                              ((o2_dev    == "invasive")     & (o2_dev_dt == delivery_dt))    ~ "invasive",
                                            ((airway    == "no device")     & (airway_dt == delivery_dt))    |
                                              ((o2_dev    == "non-invasive") & (o2_dev_dt == delivery_dt))    ~ "non-invasive",
                                            ((o2_dev    == "simple mask")  & (o2_dev_dt == delivery_dt))    ~ "simple mask",
                                            ((o2_dev    == "HFNO") & (o2_dev_dt == delivery_dt))    ~ "high flow",
                                            TRUE                                                        ~ "none"
                                          ),
                                          
                              # 3. Did the patient seem to have received supplementary oxygen?
                              o2_support_dt = pmax(o2_dt, o2_dev_dt),
                                             o2_support = 
                                            ((o2     == "o2")          & (o2_dt == o2_support_dt)) | 
                                            ((o2_dev == "simple mask") & (o2_dev_dt == o2_support_dt)),
                                          
                              # 4. Combine 1.-3. into a five-level ventilation phenotype, plus unknown
                               latest_dt = pmax(ventilated_dt, delivery_dt, o2_support_dt),
                                          ventilation_status = case_when(
                                            (ventilated & (delivery == "invasive"))                      |
                                              ((delivery == "invasive") & (delivery_dt == latest_dt))        ~ "invasive",
                                            (ventilated & (delivery == "non-invasive"))               |
                                              ((delivery == "non-invasive") & (delivery_dt == latest_dt)) ~ "non-invasive",
                                            ((delivery == "high flow") & (delivery_dt >= o2_support_dt)) |
                                              ((grepl("Oxygen nasal cannula",raw_value)) & (delivery_dt >= o2_support_dt) & (as.numeric(o2FL)>15 & (delivery_dt>=o2FL_dt)))  ~ "high flow",
                                            (o2_support & (o2_support_dt >= delivery_dt))               | 
                                              ((delivery == "simple mask") & (delivery_dt >= o2_support_dt)) |
                                              (( news2_o2=="o2") & (delivery_dt >= news2_o2_dt)) ~ "o2 support",
                                            (ventilated & (ventilated_dt >= latest_dt))    ~ "unknown-vent",
                                            TRUE ~ "no support"
                                          ))
                                        

phenotype2$hospital_site = as.character(phenotype2$visit_occurrence_id)
                                        
phenotype2$hospital_site =  substr(phenotype2$hospital_site,start=nchar(phenotype2$hospital_site), stop=nchar(phenotype2$hospital_site))
                                        
phenotype2$hospital_site = ifelse(phenotype2$hospital_site=="4", "UHB", "UCLH")

phenotype2 = phenotype2 %>% 
            mutate(covid_any=ifelse(visit_occurrence_id %in% omop_covid_all$visit_occurrence_id, "Yes", "No"),
                  covid_pcr=ifelse(visit_occurrence_id %in% omop_covid_pcr$visit_occurrence_id, "Yes", "No"))
                                        
phenotype2$vent_status_num = case_when(phenotype2$ventilation_status=="unknown-vent" ~ 0,
                             phenotype2$ventilation_status=="no support" ~ 1,
                             phenotype2$ventilation_status=="o2 support" ~ 2,
                             phenotype2$ventilation_status=="high flow" ~ 3,                                            phenotype2$ventilation_status=="non-invasive" ~  4,
                             phenotype2$ventilation_status=="invasive" ~ 5)
                                        
phenotype2$visit_occurrence_id_char = as.character(phenotype2$visit_occurrence_id)

phenotype2$vent_status_num = as.numeric(phenotype2$vent_status_num)
                                        

phenotype_max_any = phenotype2 %>% 
                    filter(!(raw_value %in% c("start" ,"stop"))) %>%
                    select(hospital_site, covid_any, visit_occurrence_id_char, vent_status_num) 
                               

phenotype_max2_any= sqldf("SELECT  hospital_site, 
                                   covid_any, 
                                   visit_occurrence_id_char, 
                                   MAX(vent_status_num) as  vent_status_num_max
                      FROM phenotype_max_any
                      GROUP BY hospital_site, 
                               covid_any, 
                               visit_occurrence_id_char")

summaryPhen_any = phenotype_max2_any %>% 
                  group_by(hospital_site, covid_any, vent_status_num_max) %>% 
                  dplyr::summarise(n=n_distinct(visit_occurrence_id_char))

#Note, for "No support" - those who have no respiratory measurements
#will not be counted and the difference will have to be taken by subtracting the
#number of visits across the phenotypes from the total visits for each category
#in Table 1 - this difference is then added to the "No support" phenotype.
write.csv(summaryPhen_any,"summaryPhen_any.csv", na="", row.names=FALSE )      

#repeat steps for PCR cases if of interest.