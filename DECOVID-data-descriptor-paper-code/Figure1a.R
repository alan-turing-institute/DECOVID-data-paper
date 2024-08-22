##################################################################################
#################### Figure 1a - DECOVID Data Descriptor Paper ###################
##################################################################################

#Authors: Nicholas Bakewell, Rebecca Green, Hannah Nicholls
#Last Updated: 18 July 2022


#install packages if not done previously
#install.packages(c("DBI","dplyr", "Hmisc", "purrr","lubridate", "aweek", "tidyr",
#                   "data.table", "stringr", "ggplot2", "epiR", "extrafont", "forcats", "reshape2","sqldf"))

library(DBI)
library(dplyr)
library(Hmisc)
library(purrr)
library(lubridate)
library(aweek)
library(tidyr)
library(stringr)
library(data.table)
library(ggplot2)
library(epiR)
library(extrafont)
loadfonts(device="win")
library(sqldf)
library(forcats)
library(reshape2)

source("common-functions.R")
source("common-database.R")
source("common-queries.R")

#Run PCR Only Queries
omop_covid_pcr <- dbGetQuery(db, covid_pcr_query) %>%
  distinct()

#Combine with all - this is used for the final Table 1.
omop_covid_all <- omop_covid_pcr %>%
  rbind(dbGetQuery(db, covid_obs_all_query)) %>%
  distinct()

#Query DECOVID for person-level summaries included
#in Figure 1a
person_level_table <- paste0("SELECT a.person_id % 10 as hospital_site,
                                     a.person_id,
                                     a.year_of_birth,
                                     b.visit_count,
                                     b.Index_Visit_Date,
                                    c.gender_concept_name,
                                    d.race_concept_name,
                                    e.LSOA_code,
                                    f.covid,
                                    f.COVID_Visit_Date,
                                    (CASE WHEN f.COVID_Visit_Date IS NULL THEN b.Index_Visit_Date
                                    ELSE f.COVID_Visit_Date END) AS Date_for_Age
                             FROM
                             (SELECT person_id,
                                     race_concept_id,
                                     gender_concept_id,
                                     location_id,
                                     year_of_birth
                             FROM person) a
                             LEFT JOIN
                             (SELECT person_id,
                                     MIN(visit_start_date) AS Index_Visit_Date,
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
                                      MIN(visit_start_date) as COVID_Visit_Date,
                                      COUNT(DISTINCT person_id) as covid
                             FROM visit_occurrence
                             WHERE visit_occurrence_id IN (",paste0(paste0("'", omop_covid_all %>%pull(visit_occurrence_id), "'", collapse=",")), ")
                             GROUP BY person_id) f
                             ON a.person_id=f.person_id")


omop_person_table <- dbGetQuery(db, person_level_table) %>%
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
         hospital_site = ifelse(hospital_site==4, "UHB", "UCLH"),
         visit_count=as.numeric(visit_count))

########################################
############ Ethnicity Plot ############
########################################

#Summarise data by ethnicity groups
Eth_Personlevel = omop_person_table %>%
  group_by(ethnicity_group,hospital_site, covid) %>%
  summarise(n=n())

#Make summary created above a data table
#for the data table operations to follow
Ethnicity <- data.table(Eth_Personlevel)

#Generate total column to compute percentage of all patients
Total <- Ethnicity[,list(sum=sum(n)), by=c("covid", "hospital_site")]

#Merge total with the Ethnicity table
Ethnicity <- merge(Ethnicity,Total,all.x=TRUE, by=c("covid", "hospital_site"))

#Calculate percentage within COVID and hospital site categories
Ethnicity$Percentage <- round((Ethnicity$n/Ethnicity$sum)*100,1)

#We may want to include the 0s in the tenth decimal place
#for those that round to whole numbers
Ethnicity$Percentage_label <- (format(round((Ethnicity$n/Ethnicity$sum)*100,1), nsmall=1))

#Recode Ethnicity groups
Ethnicity$Ethnicity <- ifelse(grepl("Asian", Ethnicity$ethnicity_group), "Asian or Asian British",
                              ifelse(grepl("Black", Ethnicity$ethnicity_group), "Black or African or Caribbean or Black British",
                                     ifelse(grepl("Mixed", Ethnicity$ethnicity_group), "Mixed or Multiple ethnic groups",
                                            ifelse(grepl("Unknown", Ethnicity$ethnicity_group),"Not stated/Unknown",
                                                   ifelse(grepl("White", Ethnicity$ethnicity_group), "White","Other")))))

#Make the new Ethnicity group a factor
Ethnicity$Ethnicity <- factor(Ethnicity$Ethnicity, levels=c("Not stated/Unknown","Other", "Mixed or Multiple ethnic groups","Black or African or Caribbean or Black British","Asian or Asian British","White"))

#Change naming of COVID status to align with paper.
Ethnicity$covid <- ifelse(Ethnicity$covid=="No", "Non-COVID-19", "COVID-19")

#Create the Ethnicity component of  Figure 1a of the paper
EthnicityComponent.FigA <- ggplot(Ethnicity,aes(fill=covid,x=Ethnicity,y=Percentage)) +
  geom_bar(position="dodge",stat="identity") +
  ylab("Percentage (%)") +
  xlab("") +
  scale_fill_manual(values=c("gold","darkgreen" )) +
  scale_y_continuous(expand=c(0,0), limits=c(0,75)) +
  scale_x_discrete(labels = function(x) str_wrap(x, width = 20)) +
  coord_flip() +
  facet_wrap(~hospital_site) +
  theme_classic() +
  theme(plot.title = element_text(hjust = 0.5),line = element_blank(),
        text=element_text(size=26, colour="black"), axis.title.x=element_blank(),
        axis.text.x=element_blank(),
        axis.text.y = element_text(colour="black", size=23),
        axis.ticks.x=element_blank(),legend.position="top")

#Add labels to bar - using the Percentage_label field
EthnicityComponent.FigA <- EthnicityComponent.FigA + geom_text(aes(label=Percentage_label), position=position_dodge(width=0.9), hjust=-0.2, size=8) +   labs(x="", fill ="COVID-19 Status")

#Save plot
ggsave("Ethnicity_Figure1a_DECOVIDPaper.png", height=6, width=14.2, plot=EthnicityComponent.FigA)

#Write to csv to save data
write.csv(Eth_Personlevel, "Eth_Personlevel_July2022.csv", na="", row.names=F)

########################################
########### Age-Sex Pyramid ############
########################################

#Age group splits by person . note, we assume
#a date of birth of July 2 of the year of birth,
#since year of birth is all that is available in DECOVID
Age_Sex_Split <- omop_person_table %>%
  filter(!(is.na(year_of_birth))) %>%
  #calculate age
  mutate(age=floor(decimal_date(
    as.Date(date_for_age, format="%Y-%m-%d")) -
      decimal_date(as.Date(
        paste0(year_of_birth, "-07-02"), format="%Y-%m-%d"
      ))),
    #create age bands
    age_band=cut(age, breaks=c(0,25,35,45,55,65,Inf), labels=
                   c("<25", "25-34", "35-44", "45-54","55-64"
                     ,"65+"), right=F)) %>%
  count(age_band, covid,
        hospital_site,
        gender_concept_name) %>%
  #We do not want unknown gender/sex at birth for this graphic
  filter(gender_concept_name!="Unknown")

#Create Age-Sex pyramid component of Figure 1a

PyramidData <- Age_Sex_Split

#We have to make the Female counts negative for the format of the
#age-sex pyramid
PyramidData$n <- ifelse(PyramidData$gender_concept_name=="Female", (-1)*(PyramidData$n), PyramidData$n)

#Rename columns
colnames(PyramidData) <- c("AgeGroup", "COVIDStatus", "Hospital", "Sex", "Count")

#Make data table - these next several lines are so we can have
#varying min and max between the panels by hospital Trust
PyramidData <- data.table(PyramidData)

PyramidData$Count_max <- ifelse(PyramidData$COVIDStatus=="Yes", 2500, 21000)
PyramidData$Count_min <- ifelse(PyramidData$COVIDStatus=="Yes", -2500, -21000)
PyramidData[, x_min := floor(  min(Count_min)), by = COVIDStatus]
PyramidData[, x_max := ceiling(max(Count_max)) , by = COVIDStatus]

PyramidData$COVIDStatus <- factor(PyramidData$COVIDStatus, levels=c("Yes", "No"))
levels(PyramidData$COVIDStatus) <- c("COVID-19", "Non-COVID-19")

#Make negative values, namely for the female counts, to appear
#positive by displaying the absolute value
abs_comma <- function (x, ...) {
  format(abs(x), ..., big.mark = ",", scientific = FALSE, trim = TRUE)
}

#Create plot for Age-Sex component of Figure 1a
Age_Sex_Pyramid <- ggplot(data = PyramidData,
                          mapping = aes(x = AgeGroup, fill = Hospital,
                                        y = Count)) +
  geom_bar(stat = "identity") +
  scale_y_continuous(labels=abs_comma) +
  geom_hline(yintercept=0, linetype="solid",color = "black", size=0.5) +
  labs(y = "", x ="Age Group", fill ="Hospital Trust") +
  scale_fill_manual(values=c("mediumseagreen", "midnightblue") )+
  coord_flip() +
  theme_classic() +
  theme(axis.line.y=element_blank(),axis.ticks.y = element_blank(),
        legend.position = "top", plot.title = element_text(hjust = 0.5),
        text=element_text(size=24, colour="black"), axis.text.x = element_text(colour="black", size=20),
        axis.text.y=element_text(colour="black", size=20),
        axis.ticks.length.x=unit(.3, "cm")) +
  facet_wrap(~COVIDStatus, scales = "free_x") +
  geom_blank(aes(y = x_min)) + geom_blank(aes(y = x_max))

#Save plot
ggsave("AgeSex_Pyramid_Figure1a_DECOVIDPaper.png", height=5, width=11.4, plot=Age_Sex_Pyramid)

#Write to csv to save data
write.csv(Age_Sex_Split, "Age_Sex_Split_July2022.csv", na="", row.names=F)

########################################
########## COVID Time-series ###########
########################################
covid_pcr_query_plot <- paste("SELECT a.visit_occurrence_id,
                                person_id,
                               'PCR' AS case_type,
                                specimen_date as covid_date
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
                                person_id,
                                visit_start_date,
                                visit_end_date
                                FROM visit_occurrence) d
                        ON (a.visit_occurrence_id = d.visit_occurrence_id)
                        WHERE ( DATEDIFF(day,  visit_start_date,  specimen_date) <= 14 AND (specimen_date <=visit_end_date))
                        OR ( DATEDIFF(day, visit_start_date, specimen_date) <= 14 AND (visit_end_date IS NULL))")

#Confirmed/suspected COVID-19 Query
covid_obs_all_query_plot <- paste("SELECT a.visit_occurrence_id, person_id, 'CONDITION' AS case_type,
                                  condition_start_date as covid_date
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


#Run PCR Only Queries
omop_covid_pcr_plot <- dbGetQuery(db, covid_pcr_query_plot) %>%
  distinct()

#Combine with all - this is used for the final Table 1.
omop_covid_all_plot <- omop_covid_pcr_plot %>%
  rbind(dbGetQuery(db, covid_obs_all_query_plot)) %>%
  distinct()

#Let's arrange by visit_occurrence_id, descending order by case type
#because we want PCR positive case type to take precedence over the CONDITION case type
#when remove duplicates
omop_covid_all_plot_sort <- omop_covid_all_plot %>%
  arrange((visit_occurrence_id), desc(case_type), covid_date)

#Remove duplicate records - take index COVID case diagnosis/positive test
omop_covid_all_plot_sort <- omop_covid_all_plot_sort[!duplicated(omop_covid_all_plot_sort$visit_occurrence_id),]

#Create field for hospital Trust
omop_covid_all_plot_sort$hospital_site <- ifelse(omop_covid_all_plot_sort$visit_occurrence_id %% 10 ==4, "UHB", "UCLH")

#Generate a column for week, as the plot will be by week
omop_covid_all_plot_sort$week <- cut((omop_covid_all_plot_sort$covid_date), "week")

#Aggregate data to create a weekly summary
omop_covid_weekly <- omop_covid_all_plot_sort %>%
  group_by(week,hospital_site) %>%
  dplyr::summarise(n=n())

#Make a week date for the plot
omop_covid_weekly$weekplot <- as.Date(omop_covid_weekly$week)

#We cannot show values <=10, so let's assume these are 10 for the plot
omop_covid_weekly$n_eq <- ifelse(omop_covid_weekly$n <= 10, 10, omop_covid_weekly$n)
omop_covid_weekly$week_graph <- as.Date(omop_covid_weekly$week)
omop_covid_weekly <- omop_covid_weekly %>%
  filter(week_graph>='2020-03-01')
WeekTSPlot <- ggplot(omop_covid_weekly, aes(x=week_graph, y=n_eq, fill=hospital_site)) +
  geom_bar(position="stack", stat="identity") +
  ylab("Weekly COVID-19 Case Count") +
  xlab("Week of PCR Test or Clinical Diagnosis") +
  scale_fill_manual(values=c("mediumseagreen", "midnightblue")) +
  theme(axis.text.x=element_text(angle=60, hjust=1, size=24),
        text=element_text(size=24, colour="black"), axis.title.x=element_blank(),
        axis.text.y = element_text(colour="black", size=24),legend.position="top")

WeekTSPlot <- WeekTSPlot + theme_classic() + scale_x_date(expand=c(0,0),  date_labels="%b %Y") + scale_y_continuous(expand=c(0,0)) +  theme(axis.text.x=element_text(colour="black"),
                                                                                                                                            text=element_text(size=20, colour="black"), axis.title.x=element_text(colour="black"),
                                                                                                                                            axis.text.y = element_text(colour="black"),legend.position="top", axis.ticks.length.x=unit(.3, "cm")) +
  labs(fill ="Hospital Trust") + guides(fill = guide_legend(nrow = 1, ncol=2))

#Save graph
ggsave("COVIDCasesTimeSeries_DECOVIDPaper.png", height=6.5, width=12, plot=WeekTSPlot)

#Save data for reference
write.csv(omop_covid_weekly, "omop_covid_weekly_DECOVIDPaper.csv", na="", row.names=F)

####################################################
########### Conditions/Comorbidities Plot ##########
####################################################

#Pull all condition data for all research questions.
omop_cond_all_v2 <- dbGetQuery(db, condition_q) %>%
  distinct()

#The following code serves as a way
#to look up comorbidities using Athena codes

#Load concept_relationship table
concept_relationship_q <- paste0("SELECT *
                                FROM concept_relationship")

concept_relationship_table <- dbGetQuery(db, concept_relationship_q)

#Load concept_ancestor table
concept_ancestor_q <- paste0("SELECT *
                              FROM concept_ancestor")

concept_ancestor_table <- dbGetQuery(db, concept_ancestor_q)

#Load concept table
concept_q <- paste0("SELECT *
                      FROM concept")

concept <- dbGetQuery(db, concept_q)

#Find all conditions that have a "standard" mapping
concept_relationship_table_standard_conditions <- concept_relationship_table %>%
  filter(relationship_id=="Maps to")

conditions_standard <- omop_cond_all_v2 %>%
  left_join(select(concept_relationship_table_standard_conditions, concept_id_2,concept_id_1),
            by=c("condition_concept_id"="concept_id_1")) %>%
  rename(concept_id_standard=concept_id_2)

#Find non-standard conditions
concept_relationship_replaces_cond <- concept_relationship_table %>%
  filter(relationship_id=="Concept replaces")

concept_relationship_replaces_cond <- conditions_standard %>%
  filter(is.na(concept_id_standard)) %>%
  inner_join(select(concept_relationship_replaces_cond, concept_id_2, concept_id_1),
             by=c("condition_concept_id"="concept_id_1")) %>%
  rename(concept_id_replaced=concept_id_2) %>%
  left_join(select(concept_relationship_table_standard_conditions, concept_id_2, concept_id_1),
            by=c("concept_id_replaced"="concept_id_1")) %>%
  select(-c(concept_id_standard, concept_id_replaced)) %>%
  rename(concept_id_standard=concept_id_2) %>%
  filter(!(is.na(concept_id_standard)))


conditions_standard <- conditions_standard %>%
  filter(!is.na(concept_id_standard))

conditions_standard <- rbind(conditions_standard,concept_relationship_replaces_cond)

#Check missing
missing_standard_cond <- setdiff(omop_cond_all_v2$condition_concept_id, conditions_standard$condition_concept_id)

Nonstandard <- concept %>% filter(concept_id %in% missing_standard_cond) %>% select(concept_id, concept_name)

omop_cond_all_v2$hospital_site <- ifelse(omop_cond_all_v2$visit_occurrence_id %% 10 ==4, "UHB", "UCLH")

#comorbidities

#Heart Disease
HDs <- concept_ancestor_table %>%
  filter(ancestor_concept_id==321588) %>%
  select(descendant_concept_id) %>%
  left_join(conditions_standard, by=c("descendant_concept_id"="concept_id_standard"))

#Just to check the diagnoses included in phenotype being created here
HDs_names <- data.frame(unique(HDs$concept_name_specific))

#Chronic respiratory disease
Resp <- concept_ancestor_table %>%
  filter(ancestor_concept_id==4063381) %>%
  select(descendant_concept_id) %>%
  left_join(conditions_standard, by=c("descendant_concept_id"="concept_id_standard"))

#Just to check the diagnoses included in phenotype being created here
Resp_names <- data.frame(unique(Resp$concept_name_specific))

#Chronic kidney disease
ChronicKidney <- concept_ancestor_table %>%
  filter(ancestor_concept_id==46271022) %>%
  select(descendant_concept_id) %>%
  left_join(conditions_standard, by=c("descendant_concept_id"="concept_id_standard"))

#Just to check the diagnoses included in phenotype being created here
ChronicKidney_names <- data.frame(unique(ChronicKidney$concept_name_specific))

#Diabetes
Diabetes <- concept_ancestor_table %>%
  filter(ancestor_concept_id==201820) %>%
  select(descendant_concept_id) %>%
  left_join(conditions_standard, by=c("descendant_concept_id"="concept_id_standard"))

#Just to check the diagnoses included in phenotype being created here
Diabetes_names <- data.frame(unique(Diabetes$concept_name_specific))

Comoborbidities = omop_person_table %>%
  mutate(HeartDisease=ifelse(person_id %in% (HDs$person_id), 1, 0),
         Diabetes=ifelse(person_id %in% (Diabetes$person_id), 1, 0 ),
         Resp=ifelse(person_id %in% (Resp$person_id), 1,0 ),
         ChronicKidney=ifelse(person_id %in% (ChronicKidney$person_id), 1, 0 ))

#In this instance, dplyr did not work
#for grouping data, so here we use SQL
Summary_Comoborbidities <- sqldf("SELECT hospital_site, covid, COUNT(person_id) as N,
                                         SUM(HeartDisease) AS HeartDisease,
                                         SUM(Diabetes) AS Diabetes,
                                         SUM(Resp) AS Resp,
                                         SUM(ChronicKidney) AS ChronicKidney
                                 FROM Comoborbidities
                                 GROUP BY hospital_site,
                                          covid")

Summary_Comoborbidities$covid <- factor(Summary_Comoborbidities$covid, levels=c("Yes", "No"))
levels(Summary_Comoborbidities$covid) <- c("COVID-19", "Non-COVID-19")

#Create long data set to create figure
Summary_Comoborbidities_long <- melt(Summary_Comoborbidities, id.vars=c("hospital_site", "covid", "N"), variable.name="condition")

#Calculate percentage that have a comorbidity by hospital and COVID status
Summary_Comoborbidities_long$Percent  <- round((Summary_Comoborbidities_long$value/Summary_Comoborbidities_long$N)*100, 1)

#Create a label as done for Ethnicity, where 0s are included
#in the tenth decimal place for those %s that round to a whole number
Summary_Comoborbidities_long$Percent_label <- format(round((Summary_Comoborbidities_long$value/Summary_Comoborbidities_long$N)*100, 1), nsmall=1)

#Create more labels for the conditions that are
#more formal than the ones used when creating the dataset
Summary_Comoborbidities_long$condition_formal_name <- ifelse(Summary_Comoborbidities_long$condition=="HeartDisease", "Heart Disease",
                                                             ifelse(Summary_Comoborbidities_long$condition=="Diabetes", "Diabetes Mellitus (any type)",
                                                                    ifelse(Summary_Comoborbidities_long$condition=="Resp", "Chronic Respiratory Disease",
                                                                           "Chronic Kidney Disase")))
Comoborbidities_Fig1a <- ggplot(Summary_Comoborbidities_long,aes(x=fct_reorder(condition_formal_name, Percent),y=(Percent), fill=covid)) +
  geom_bar(position="dodge",stat="identity") +
  ylab("Percentage (%)") +
  xlab("")+
  scale_fill_manual(values=c("gold","darkgreen" )) +
  scale_y_continuous(expand=c(0,0), limits=c(0,47)) +
  scale_x_discrete(labels = function(x) str_wrap(x, width = 20)) +
  coord_flip() +
  theme_classic() +
  theme(plot.title = element_text(hjust = 0.5),line = element_blank(),
        text=element_text(size=26,colour="black"), axis.title.x=element_blank(),
        axis.text.x=element_blank(),
        axis.text.y=element_text(colour="black", size=23),
        axis.ticks.x=element_blank(), legend.position="top") +
  facet_wrap(~hospital_site)

Comoborbidities_Fig1a <- Comoborbidities_Fig1a + geom_text(aes(label=(Percent_label)), position=position_dodge(width=0.9), hjust=-0.1, size=8) +  labs(x="",fill ="COVID-19 Status")

#Save plot for comorbidities component of Figure1a
ggsave("Comorbidities_Figure1a_DECOVIDPaper.png", height=6, width=14.2, plot=Comoborbidities_Fig1a)

#Save data for reference for later
write.csv(Summary_Comoborbidities_long, "Summary_Comoborbidities_long.csv", na="", row.names=F)

#All images were brought into a PowerPoint slide and formatted and exported as an image file
#for the final DECOVID Data Descriptor manuscript
