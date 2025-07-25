###############################################################################################
# #B Me##REMENTS - DECOVID Data DescLE 1b Measurements - DECOVID Data Descriptor Paper ####################
# ##############################################################################################


# Authors: Nicholas Bakewell, Rebecca Green, Hannah Nicholls
# Last Updated: 12 June 2022

# uncomment the following lines if the packages have not been installed previously.
# install,packages(c("DBI", "dplyr", "Hmisc", "purrr", "reshape2", "tidyr))
library(DBI) #for performing SQL queries
library(dplyr) #for data manipulation
library(Hmisc) #for describing variables
library(purrr) #for data manipulation
library(reshape2) #for use of dcast
library(tidyr)

source("common-functions.R")
source("common-database.R")
source("common-queries.R")

# Run PCR Only Queries - distinct() is used to remove duplicates
omop_covid_pcr <- dbGetQueryBothTrusts(db, covid_pcr_query) %>%
  distinct()

# Here, the COVID-19 cases based on clinical diagnoses (suspected and confirmed)
# are appended to the PCR only cases - again, distinct()
# is used to remove duplicates
omop_covid_all <- omop_covid_pcr %>%
  rbind(dbGetQueryBothTrusts(db, covid_obs_all_query)) %>%
  distinct()

# Before proceeding, specify the COVID-19 case type the data summaries should
# be based on. In the DECOVID Data Descriptor paper, the omop_covid_all cases
# are used. However, the omop_covid_pcr can be assigned to covid_case_type
# instead.
covid_case_type <- omop_covid_all

# ###############################################
# ###### Table 1 - Measurements/Obs data ########
# ###############################################

# What this query does is retrieves all records in the visit_detail table that
# have a measurement take place between the visit_detail interval (inclusive)

# Very important to note that a measurement datetime can be equal to
# visit_detail start_datetime (same for visit_occurrence start_datetime), and
# ceiling 0 would result in 0, although we want this to be 1 whether we are
# looking at hours or days. A case when statement has been included in the code
# to account for this.

# Care site query with the # of days in each visit detail record from
# visit_detail table
# This is the only we can calculate the number patient days in each level of
# care
care_site_query_measurements <- paste0(
  "SELECT
     *,
     DATEDIFF(minute,
              visit_detail_start_datetime,
              visit_detail_end_datetime) / 1440 AS patient_days
    FROM visit_detail"
)

omop_care <-
  dbGetQueryBothTrusts(db, care_site_query_measurements) %>%
  distinct() %>%
  mutate(
    care_site_id = as.character(care_site_id),
    covid = ifelse(
      visit_occurrence_id %in% covid_case_type$visit_occurrence_id,
      "Yes",
      "No"
    ),
    hospital_site = case_when(schema == "uhb" ~ "UHB",
                              schema == "uclh" ~ "UCLH")
  )

# Read in measurements of interest - which the csv can be modified
measurement <- read.csv("measurements_filtered.csv") %>%
  dplyr::rename(measurement_concept_id = concept_id)

# This query pulls all measurement records for the measurements read in the
# csv file above
measurement_query <-
  paste0(
    "SELECT DISTINCT d.*, measurement_concept_name FROM
     (SELECT visit_occurrence_id,
             day_number,
             length_of_stay,
             visit_detail_id,
             care_site_id,
             measurement_concept_id,
             COUNT(measurement_concept_id) as measurement_concept_id_count
      FROM
     (SELECT a.visit_occurrence_id,
             visit_detail_id,
             care_site_id,
             measurement_datetime,
             visit_detail_end_datetime,
             visit_detail_start_datetime,
             (CASE
               WHEN CEILING(DATEDIFF(minute,
                                     visit_detail_start_datetime,
                                     measurement_datetime) / 1440)=0 THEN 1
               ELSE CEILING(DATEDIFF(minute,
                                     visit_detail_start_datetime,
                                     measurement_datetime) / 1440) END)
                                     AS day_number,
            (DATEDIFF(minute,
                      visit_detail_start_datetime,
                      visit_detail_end_datetime) / 1440) AS length_of_stay,
                      measurement_concept_id
        FROM
          (SELECT visit_occurrence_id,
                  visit_detail_start_datetime,
                  visit_detail_end_datetime,
                  visit_detail_id,
                  care_site_id
           FROM visit_detail
           WHERE (DATEDIFF(minute,
                           visit_detail_start_datetime,
                           visit_detail_end_datetime) / 1440 >=1)) a
     LEFT JOIN
     (SELECT visit_occurrence_id,
             measurement_datetime,
             measurement_concept_id FROM measurement
         WHERE measurement_concept_id IN (",
    paste0(paste0(
      "'",
      measurement %>% filter(!(
        category %in% c(
          "blood_gas",
          "blood_pressure",
          "RR Man",
          "RR Spont",
          "news2Resp",
          "resp"
        )
      ))
      %>% pull(measurement_concept_id),
      "'",
      collapse = ","
    )),
    ") AND visit_occurrence_id IS NOT NULL
         GROUP BY visit_occurrence_id,
                  measurement_datetime,
                  measurement_concept_id) b
     ON (a.visit_occurrence_id = b.visit_occurrence_id)) c
     WHERE ((measurement_datetime >= visit_detail_start_datetime) AND
            (measurement_datetime <= visit_detail_end_datetime))
     GROUP BY visit_occurrence_id,
              visit_detail_id,
              day_number,
              length_of_stay,
              measurement_concept_id,
              care_site_id) d
     LEFT JOIN
     (SELECT
        concept_id as measurement_concept_id,
        concept_name as measurement_concept_name
      FROM concept) e
     ON (d.measurement_concept_id = e.measurement_concept_id)"
  )

# We have to separate the blood query, as we want to consider all SBP, DBP, Mean BP as one measurement
# type for the summary and it is easier to alise these measures in a separate query.
measurement_query_blood <- paste0(
  "SELECT DISTINCT *
   FROM
     (SELECT visit_occurrence_id,
             day_number,
             length_of_stay,
             visit_detail_id,
             care_site_id,
             measurement_concept_id,
             'blood_pressure' AS measurement_concept_name,
             COUNT(measurement_concept_id) as measurement_concept_id_count FROM
     (SELECT a.visit_occurrence_id,
             visit_detail_id,
             care_site_id,
             measurement_datetime,
             visit_detail_end_datetime,
             visit_detail_start_datetime,
            (CASE
               WHEN CEILING(DATEDIFF(minute,
                                     visit_detail_start_datetime,
                                     measurement_datetime) / 1440)=0 THEN 1
               ELSE CEILING(DATEDIFF(minute,
                                     visit_detail_start_datetime,
                                     measurement_datetime) / 1440) END)
                                     AS day_number,
            (DATEDIFF(minute,
                      visit_detail_start_datetime,
                      visit_detail_end_datetime) / 1440) AS length_of_stay,
                      measurement_concept_id
            FROM
            (SELECT visit_occurrence_id,
                    visit_detail_start_datetime,
                    visit_detail_end_datetime,
                    visit_detail_id,
                    care_site_id
             FROM visit_detail
            WHERE (DATEDIFF(minute,
                            visit_detail_start_datetime,
                            visit_detail_end_datetime) / 1440 >=1)) a
     LEFT JOIN
       (SELECT
          visit_occurrence_id,
          measurement_datetime,
          measurement_concept_id,
         'blood_pressure' AS measurement_concept_name
        FROM measurement
       	WHERE measurement_concept_id IN (",
  paste0(paste0(
    "'",
    measurement %>% filter(category == "blood_pressure") %>% pull(measurement_concept_id),
    "'",
    collapse = ","
  )),
  ") AND visit_occurrence_id IS NOT NULL
    GROUP BY
      visit_occurrence_id,
      measurement_datetime,
      measurement_concept_id) b
    ON (a.visit_occurrence_id = b.visit_occurrence_id)) c
    WHERE ((measurement_datetime >= visit_detail_start_datetime) AND
           (measurement_datetime <= visit_detail_end_datetime))
    GROUP BY visit_occurrence_id,
             visit_detail_id,
             day_number,
             length_of_stay,
             measurement_concept_id,
             care_site_id) d"
)


# Blood gases require additional tables to be queried, so this is also done separately.
bloodgas_query <- paste0(
  "SELECT g.*, measurement_concept_name FROM
     (SELECT DISTINCT
        visit_occurrence_id,
        day_number,
        length_of_stay,
        visit_detail_id,
        care_site_id,
        measurement_concept_id,
        COUNT(measurement_concept_id) as measurement_concept_id_count
      FROM
       (SELECT
         a.visit_occurrence_id,
         visit_detail_id,
         care_site_id,
         measurement_datetime,
         visit_detail_end_datetime,
         visit_detail_start_datetime,
         (CASE
            WHEN CEILING(DATEDIFF(minute,
                                  visit_detail_start_datetime,
                                  measurement_datetime) / 1440)=0 THEN 1
            ELSE CEILING(DATEDIFF(minute,
                                  visit_detail_start_datetime,
                                  measurement_datetime) / 1440) END)
                                  AS day_number,
          (DATEDIFF(minute,
                    visit_detail_start_datetime,
                    visit_detail_end_datetime) / 1440)
                    AS length_of_stay,
         measurement_concept_id
       FROM
       (SELECT
          visit_occurrence_id,
          visit_detail_start_datetime,
          visit_detail_end_datetime,
          visit_detail_id,
          care_site_id
        FROM visit_detail
        WHERE (DATEDIFF(minute,
                        visit_detail_start_datetime,
                        visit_detail_end_datetime) / 1440 >=1)) a
        LEFT JOIN
           (SELECT b.*, c.specimen_id, d.anatomic_site_concept_id FROM
           (SELECT
              visit_occurrence_id,
              measurement_id,
              measurement_datetime,
              measurement_concept_id
            FROM measurement
           WHERE measurement_concept_id IN (",
  paste0(paste0(
    "'",
    measurement %>% filter(category == "blood_gas") %>% pull(measurement_concept_id),
    "'",
    collapse = ","
  )),
  ") AND visit_occurrence_id IS NOT NULL
    GROUP BY visit_occurrence_id,
              measurement_datetime,
              measurement_concept_id,
              measurement_id) b
    INNER JOIN
    (SELECT fact_id_1 as measurement_id,
            fact_id_2 as specimen_id
             FROM fact_relationship
     WHERE domain_concept_id_1 = 21 AND domain_concept_id_2 = 36) c
    ON (b.measurement_id = c.measurement_id)
    INNER JOIN
    (SELECT specimen_id,
              anatomic_site_concept_id
     FROM specimen WHERE anatomic_site_concept_id = 4136257) d
    ON (c.specimen_id = d.specimen_id)) e
    ON (a.visit_occurrence_id = e.visit_occurrence_id)) f
    WHERE ((measurement_datetime >= visit_detail_start_datetime) AND
           (measurement_datetime <= visit_detail_end_datetime))
    GROUP BY visit_occurrence_id,
             day_number,
             length_of_stay,
             measurement_concept_id,
             visit_detail_id,
             care_site_id) g
     LEFT JOIN
    (SELECT
       concept_id as measurement_concept_id,
       concept_name as measurement_concept_name
     FROM concept) h
    ON (g.measurement_concept_id = h.measurement_concept_id)"
)

# Warning: this takes 15-20 minutes to run until the join.
omop_measure_all <-
  bind_rows(dbGetQueryBothTrusts(db, measurement_query),
            dbGetQueryBothTrusts(db, measurement_query_blood),
            dbGetQueryBothTrusts(db, bloodgas_query)) %>%
  distinct()

omop_measure_all <- omop_measure_all %>%
  mutate(
    covid = ifelse(
      visit_occurrence_id %in% covid_case_type$visit_occurrence_id,
      "Yes",
      "No"
    ),
    hospital_site = case_when(schema == "uhb" ~ "UHB",
                              schema == "uclh" ~ "UCLH")
  ) %>%
  left_join(measurement %>%
              select(measurement_concept_id, category),
            by = "measurement_concept_id")


# Let's make the category a character, and code the blood pressure
# category, as it is NA. In the final step, we make category a factor.
omop_measure_all$category = as.character(omop_measure_all$category)
omop_measure_all$category = ifelse(is.na(omop_measure_all$category),
                                   "blood_pressure",
                                   omop_measure_all$category)
omop_measure_all$category = as.factor(omop_measure_all$category)

# Converting other relevant bigint fields to character.
omop_measure_all$care_site_id_char = as.character(omop_measure_all$care_site_id)

# Level 2/3 Care
omop_measure_l2_l3_any_visit_type <- omop_measure_all %>%
  filter(care_site_id_char %in% c("1", "2", "3", "15", "20", "21", "22"))

# make level 2 and 3 copy of omop_care to get total 24-hour periods.
omop_care_l2_l3_any_visit_type <- omop_care %>%
  filter(care_site_id %in% c("1", "2", "3", "15", "20", "21", "22"))  %>%
  filter(patient_days >= 1)

# Set null vector listfor medians.
Medians_L23 = vector(mode = "list", length = length(unique(
  omop_measure_all$measurement_concept_name
)))

for (i in 1:length(unique(omop_measure_all$measurement_concept_name))) {
  cat(i, "\n")
  
  # Filter measurements to level 2/3 care
  Measurement_l2_l3 <- omop_measure_all %>%
    filter(
      as.character(care_site_id) %in% c("1", "2", "3", "15", "20", "21", "22"),
      measurement_concept_name ==
        unique(omop_measure_all$measurement_concept_name)[i]
    )
  
  # Get patient days for join from the visit_detail table.
  omop_care_l2_l3_any_visit_type_join <-
    omop_care_l2_l3_any_visit_type[, c("visit_detail_id", "patient_days")]
  
  # Need to convert the visit_detail_id field to character
  omop_care_l2_l3_any_visit_type_join$visit_detail_id <-
    as.character(omop_care_l2_l3_any_visit_type_join$visit_detail_id)
  
  # Need to convert the visit_detail_id field to character
  Measurement_l2_l3$visit_detail_id <-
    as.character(Measurement_l2_l3$visit_detail_id)
  
  # join the two tables
  Measurement_l2_l3 <- left_join(Measurement_l2_l3,
                                omop_care_l2_l3_any_visit_type_join,
                                by = c("visit_detail_id"))
  
  # We want to only include measurements that occurred within the last patient,
  # no partial days are to be included.
  Measurement_l2_l3 <- Measurement_l2_l3 %>%
    filter(day_number <= floor(patient_days))
  
  # Determine which visits do not have any measurements in complete 24-hour
  # blocks. This is to add 0s back into the dataset for median.
  NoMeasurements <- setdiff(
    as.character(omop_care_l2_l3_any_visit_type$visit_detail_id),
    as.character(Measurement_l2_l3$visit_detail_id)
  )
  
  # This is not necessary but an exploratory step to get an idea of when
  # measurements occur. Note, could save these data tables each loop.
  Measurement_l2_l3_sum <- Measurement_l2_l3 %>%
    group_by(visit_detail_id, measurement_concept_id) %>%
    dplyr::summarise(
      n_days = n(),
      count = sum(measurement_concept_id_count),
      earliest_day = min(day_number),
      patient_days = max(floor(patient_days)),
      .groups = "drop_last"
    )
  
  # Calculate frequency per patient day (24-hour blocks only) for each patient
  Measurement_l2_l3_sum$freq_per_day <- Measurement_l2_l3_sum$count /
    Measurement_l2_l3_sum$patient_days
  
  # Calculate percent missing for each patient to get an idea missingness at a
  # patient-level. Code below is also used to calculate this overall, and the
  # mean number of measurements if desired.
  Measurement_l2_l3_sum$percent_miss <- (1 - (
    Measurement_l2_l3_sum$n_days / Measurement_l2_l3_sum$patient_days
  )) * 100
  
  # Create dateframe for the patients in level 2/3 care without a specific
  # measurement
  Freq_per_day_add_zeros <- data.frame(
    visit_detail_id = NoMeasurements,
    freq_per_day = rep(0, length(NoMeasurements)),
    percent_miss = rep(100, length(NoMeasurements))
  )
  
  Freq_per_day_add_zeros <-
    rbind(
      Freq_per_day_add_zeros,
      Measurement_l2_l3_sum[, c("visit_detail_id",
                                "freq_per_day",
                                "percent_miss")]
      )
  
  # This is a table of the covid and hospital categories for each level
  # 2/3 visit.
  covid_hospital <- omop_care_l2_l3_any_visit_type %>%
    select(visit_detail_id, covid, hospital_site) %>%
    mutate(visit_detail_id = as.character(visit_detail_id)) %>%
    distinct()
  
  Freq_per_day_add_zeros <- left_join(Freq_per_day_add_zeros,
                                     covid_hospital,
                                     by = c("visit_detail_id"))
  
  # For the paper we only include median, but the first and third quartiles
  # have been included.
  Freq_per_day_add_zeros_sum <- Freq_per_day_add_zeros %>%
    group_by(covid, hospital_site) %>%
    dplyr::summarise(
      n = n(),
      med = quantile(freq_per_day, na.rm = T)[3],
      Q1 = quantile(freq_per_day, na.rm = T)[2],
      Q3 = quantile(freq_per_day, na.rm = T)[4],
      .groups = "drop_last"
    )
  
  # Paste the measurement name for the loop
  Freq_per_day_add_zeros_sum$measurement <-
    paste(unique(omop_measure_all$measurement_concept_name)[i])
  
  # Append to other medians previously calculated
  Medians_L23[[i]] <- Freq_per_day_add_zeros_sum
}

# Make into a data table
Medians_L2_L3 = bind_rows(Medians_L23, .id = "column_label")

# write to csv
write.csv(
  Medians_L2_L3,
  csv_file("Medians_L2_L3_Paper"),
  na = "",
  row.names = FALSE
)



# Calculate number of completed 24 hour windows in visit detail for only
# level 2 and 3 care types.
# for mean and % missing.
n_complete_windows_all_l2_l3_any_visit <-
  omop_care_l2_l3_any_visit_type %>%
  filter(patient_days >= 1) %>%
  group_by(covid, hospital_site) %>%
  dplyr::summarise(total_days = sum(floor(patient_days)),
                   .groups = "drop_last")


# Calculate mean and % missing for all measurements
omop_count_all_l2_l3_any <- omop_measure_l2_l3_any_visit_type %>%
  filter(day_number <= floor(length_of_stay)) %>%
  mutate(count = as.numeric(measurement_concept_id_count)) %>%
  group_by(covid, measurement_concept_name, hospital_site, category) %>%
  dplyr::summarise(days_for_calc = n(),
                   sum = sum(count),
                   .groups = "drop_last") %>%
  left_join(n_complete_windows_all_l2_l3_any_visit,
            by = c("covid", "hospital_site")) %>%
  mutate(mean = signif(sum / total_days, 2),
         missing = round(((total_days - days_for_calc) / total_days) * 100, 1))

# Write mean and missingness file. Mean has been extract too, in case there is
# interest to contrast with the median
rbind(
  omop_count_all_l2_l3_any %>%
    dcast(
      category + measurement_concept_name ~ hospital_site + covid,
      value.var = c("mean")
    ) %>%
    mutate("Summary" = rep("mean")),
  omop_count_all_l2_l3_any %>%
    dcast(category + measurement_concept_name ~ hospital_site + covid,
          value.var = c("missing")) %>%
    mutate("Summary" = rep("% missing"))
  ) %>%
  arrange(measurement_concept_name, category) %>%
  write.csv(
    csv_file("omop_measure_mean_missing_l2_l3_anyvisittype_Paper"),
    row.names = FALSE,
    na = ""
  )

###################################################### 
# Now, do the exact same thing, but for Level 1 care #
######################################################

omop_measure_l1_any_visit_type <- omop_measure_all %>%
  filter(care_site_id_char %in% c("7", "8", "17"))

omop_care_l1_any_visit_type = omop_care %>%
  filter(care_site_id %in% c("7", "8", "17")) %>%
  filter(patient_days >= 1)

MediansL1 = vector(mode = "list", length = length(unique(
  omop_measure_all$measurement_concept_name
)))

for (i in 1:length(unique(omop_measure_all$measurement_concept_name))) {
  cat(i, "\n")
  # Filter to level 1 care. Refer to comments above for level 2/3 care,
  # as they are equivalent here.
  Measurement_l1 <- omop_measure_all %>%
    filter(
      as.character(care_site_id) %in% c("7", "8", "17"),
      measurement_concept_name ==
        unique(omop_measure_all$measurement_concept_name)[i]
    )
  
  omop_care_l1_any_visit_type_join <- omop_care_l1_any_visit_type[, c("visit_detail_id", "patient_days")]
  
  omop_care_l1_any_visit_type_join$visit_detail_id <- as.character(omop_care_l1_any_visit_type_join$visit_detail_id)
  
  Measurement_l1$visit_detail_id <- as.character(Measurement_l1$visit_detail_id)
  
  Measurement_l1 <- left_join(Measurement_l1,
                             omop_care_l1_any_visit_type_join,
                             by = c("visit_detail_id"))
  
  Measurement_l1 <- Measurement_l1 %>%
    filter(day_number <= floor(patient_days))
  
  NoMeasurements <- setdiff(
    as.character(omop_care_l1_any_visit_type$visit_detail_id),
    as.character(Measurement_l1$visit_detail_id)
  )
  
  Measurement_l1_sum <- Measurement_l1 %>%
    group_by(visit_detail_id) %>%
    dplyr::summarise(
      n_days = n(),
      count = sum(measurement_concept_id_count),
      earliest_day = min(day_number),
      patient_days = max(floor(patient_days)),
      .groups = "drop_last"
    )
  
  Measurement_l1_sum$freq_per_day <-
    Measurement_l1_sum$count / Measurement_l1_sum$patient_days
  
  Measurement_l1_sum$percent_miss <- (1 - (
    Measurement_l1_sum$n_days / Measurement_l1_sum$patient_days
  )) * 100
  
  Freq_per_day_add_zeros <- data.frame(
    visit_detail_id = NoMeasurements,
    freq_per_day = rep(0, length(NoMeasurements)),
    percent_miss = rep(100, length(NoMeasurements))
  )
  
  Freq_per_day_add_zeros <-
    rbind(Freq_per_day_add_zeros,
          Measurement_l1_sum[, c("visit_detail_id",
                                 "freq_per_day",
                                 "percent_miss")])
  
  covid_hospital <- omop_care_l1_any_visit_type %>%
    select(visit_detail_id, covid, hospital_site) %>%
    mutate(visit_detail_id = as.character(visit_detail_id)) %>%
    distinct()
  
  Freq_per_day_add_zeros <- left_join(Freq_per_day_add_zeros,
                                     covid_hospital,
                                     by = c("visit_detail_id"))
  
  Freq_per_day_add_zeros_sum <- Freq_per_day_add_zeros %>%
    group_by(covid, hospital_site) %>%
    dplyr::summarise(
      n = n(),
      med = quantile(freq_per_day, na.rm = T)[3],
      Q1 = quantile(freq_per_day, na.rm = T)[2],
      Q3 = quantile(freq_per_day, na.rm = T)[4],
      .groups = "drop_last"
    )
  
  Freq_per_day_add_zeros_sum$measurement <-
    paste(unique(omop_measure_all$measurement_concept_name)[i])
  
  # Append to other measurements' data
  MediansL1[[i]] <-  Freq_per_day_add_zeros_sum
}

MediansL1_dataframe = bind_rows(MediansL1, .id = "column_label")

write.csv(MediansL1_dataframe, csv_file("MediansL1_Paper") , na = "")

# This sums all patient days for visits to level 1 care more than or equal
# to 1 day
n_complete_windows_all_l1_any_visit_type <-
  omop_care_l1_any_visit_type %>%
  filter(patient_days >= 1) %>%
  group_by(covid, hospital_site) %>%
  dplyr::summarise(total_days = sum(floor(patient_days)))

# This code summarises the data for each patient by measurement.
omop_count_all_l1_any_visit_type <-
  omop_measure_l1_any_visit_type %>%
  filter(day_number <= floor(length_of_stay)) %>%
  mutate(count = as.numeric(measurement_concept_id_count)) %>%
  group_by(covid, measurement_concept_name, hospital_site, category) %>%
  dplyr::summarise(days_for_calc = n(),
                   sum = sum(count)) %>%
  left_join(n_complete_windows_all_l1_any_visit_type,
            by = c("covid", "hospital_site")) %>%
  mutate(mean = signif(sum / total_days, 2),
         missing = round(((total_days - days_for_calc) / total_days) * 100, 1))

# As with level 2/3 care, write the missingness and mean measurements data,
# Mean has been extract too, in case there is
# interest to contrast with the median
rbind(
  omop_count_all_l1_any_visit_type %>%
    dcast(
      category + measurement_concept_name ~ hospital_site + covid,
      value.var = c("mean")
    ) %>%
    mutate("Summary" = rep("mean")),
  omop_count_all_l1_any_visit_type %>% dcast(
    category + measurement_concept_name ~ hospital_site + covid,
    value.var = c("missing")
  ) %>%
    mutate("Summary" = rep("% missing"))
) %>% arrange(measurement_concept_name, category) %>%
  write.csv(
    csv_file("omop_measure_l1_mean_missing_any_visit_type_Paper"),
    row.names = FALSE,
    na = ""
  )

omop_concept_ids <- omop_measure_all %>%
  select(measurement_concept_name, measurement_concept_id) %>%
  group_by(measurement_concept_name, measurement_concept_id) %>%
  dplyr::summarise()

# Extract concepts included in Table 1 in the paper
concepts_to_include_in_order <-
  c(
    # Clinical observations
    "4239408",
    "4302666",
    "4313591",
    "40762499",
    "5152194",
    "415470",
    "3007194",
    "37208354",
    
    # FBCs
    "3010813",
    "3013429",
    "3017732",
    "3019198",
    "3000963",
    "3007461",
    
    # U&Es
    "3020564",
    "3000285",
    "3005456",
    
    "3020460",
    
    # ABGs
    "3010421",
    "3013290",
    "3027315"
  )

pivot_and_format_for_table <- function(x,
                                       omop_concept_ids,
                                       concepts_to_include_in_order){
  x %>%
    mutate(med_missing = paste0(round(med, 1),
                                " [",
                                round(missing, 1),
                                "]")) %>%
    pivot_wider(
      id_cols = c(measurement_concept_name),
      names_from = c(hospital_site, covid),
      values_from = med_missing,
      names_sort = TRUE
    ) %>%
    left_join(omop_concept_ids) %>%
    filter(
      as.character(measurement_concept_id) %in% concepts_to_include_in_order |
        measurement_concept_name == "blood_pressure"
    ) %>%
    arrange(factor(measurement_concept_id,
                   levels = concepts_to_include_in_order)) %>%
    relocate(UCLH_No, .after = UCLH_Yes) %>%
    relocate(UHB_No, .after = UHB_Yes)
}

# Extract "median measurements/day (% missing)"
# and arrange in the format needed for the paper
l1_table <- MediansL1_dataframe %>%
  dplyr::rename(measurement_concept_name = "measurement") %>%
  left_join(omop_count_all_l1_any_visit_type) %>%
  pivot_and_format_for_table(omop_concept_ids, concepts_to_include_in_order)

write.csv(l1_table, file = csv_file("l1_table"))

# Extract "median measurements/day (% missing)"
# and arrange in the format needed for the paper
l2_table <- Medians_L2_L3 %>%
  dplyr::rename(measurement_concept_name = measurement) %>%
  left_join(omop_count_all_l2_l3_any) %>%
  pivot_and_format_for_table(omop_concept_ids, concepts_to_include_in_order)

write.csv(l2_table, file = csv_file("l2_table"))
