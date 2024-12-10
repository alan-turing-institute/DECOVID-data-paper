################################################################################
#################### Figure 1b - DECOVID Data Descriptor Paper #################
################################################################################

# Load libraries
# install.packages(c("DBI", "dplyr","Hmisc", "purrr"))
library(DBI) #for performing SQL queries
library(dplyr) #for data manipulation
library(Hmisc) #for describing variables
library(purrr) #for data manipulation

source("common-functions.R")
source("common-database.R")
source("common-queries.R")

# Concept IDs for measurements interest for figure 1b
MeasurementstoPull = c(
  37208354,
  3020716,
  3020460,
  3007194,
  4239408,
  4313591,
  3013502,
  4141684,
  3027315,
  40762499
)

# This query will pull the measurements of interest
measurement_query_patients <-
  paste0(
    "SELECT
      a.*,
      b.concept_name,
      d.concept_name as units,
      c.visit_detail_start_datetime,
      CEILING(DATEDIFF(minute,
                       a.measurement_datetime,
                       c.visit_detail_start_datetime)/ 60) AS hour
   FROM measurement a
   LEFT JOIN concept b
   ON a.measurement_concept_id=b.concept_id
   LEFT JOIN
   (SELECT MIN(visit_detail_start_datetime) AS visit_detail_start_datetime,
           MAX(visit_detail_end_datetime) AS visit_detail_end_datetime,
           visit_occurrence_id
   FROM visit_detail
   GROUP BY visit_occurrence_id) c
   ON a.visit_occurrence_id=c.visit_occurrence_id
   LEFT JOIN concept d
   ON a.unit_concept_id=d.concept_id
   WHERE measurement_concept_id IN (",
    paste0(paste0("'", MeasurementstoPull, "'", collapse =
                    ",")),
    ") AND
     (DATEDIFF(minute,
               c.visit_detail_start_datetime,
               a.measurement_datetime) >= 0
      AND
      DATEDIFF(minute,
               a.measurement_datetime,
               c.visit_detail_end_datetime) >= 0)"
  )


omop_measFigure1Pat <-
  dbGetQuery(db, measurement_query_patients) %>%
  distinct()

# Enter a visit occurrence id of interest
visit_occurrence_id_interest <- 1

singlepatientdata <- omop_measFigure1Pat %>%
  # only take measures with a positive hour, as some measurements may have
  # been taken in other levels of care prior
  filter(hour >= 0, visit_occurrence_id == visit_occurrence_id_interest) %>%
  filter(!grepl("Partial pressure|flow rate|Inhaled", concept_name)) %>%
  filter(!is.na(value_as_number)) %>%
  group_by(concept_name,
           measurement_concept_id,
           hour,
           visit_occurrence_id,
           person_id) %>%
  dplyr::summarise(avg = mean(value_as_number, na.rm =
                                T), n = n()) %>%
  arrange(visit_occurrence_id, measurement_concept_id)

singlepatientdata$concept_name <-
  as.character(singlepatientdata$concept_name)

# Save original concept names in another column just in case.
singlepatientdata$concept_name_org = singlepatientdata$concept_name

# Create new labels for some of the concepts
singlepatientdata$concept_name = case_when(
  grepl("C reactive",singlepatientdata$concept_name) ~
    "C-reactive Protein (mg/L)",
  grepl("flow rate", singlepatientdata$concept_name) ~
    "Delivered Oxygen (O2) Flow Rate (L/min)",
  grepl("Heart rate", singlepatientdata$concept_name) ~
    "Heart Rate (beats per minute)",
  grepl("Oxygen saturation", singlepatientdata$concept_name) ~
    "Oxygen (O2) Saturation (%)",
  grepl("NEWS2", singlepatientdata$concept_name) ~
    "NEWS2 Score",
  grepl("Respiratory rate", singlepatientdata$concept_name) ~
    "Respiratory Rate (breaths per minute)",
  grepl("Glasgow", singlepatientdata$concept_name) ~
    "Glasgow Coma Scale Total Sum Score"
)


# Calculate days
singlepatientdata$day <- singlepatientdata$hour / 24

# Breaks may change depending on patient
breaks <- seq(0, 11, 0.5)
labels <- as.character(breaks)
labels[!(breaks %% 1 == 0)] <- ''

MeasurementsPlot <-
  ggplot(data = singlepatientdata,
         aes(x = day, y = avg, colour = concept_name)) +
  geom_line() +
  geom_point(aes(shape = concept_name), size = 2) +
  scale_x_continuous(
    limits = c(0, 11.4),
    breaks = breaks,
    labels = labels ,
    expand = c(0, 0)
  ) +
  scale_y_continuous(
    limits = c(0, 122),
    breaks = seq(0, 122, 15),
    expand = c(0, 0)
  ) +
  scale_shape_manual(values = c(0:9)) +
  scale_color_brewer(palette = "Paired") +
  ylab("Measurement value") +
  xlab("Days since hospital admission") +
  theme_classic() +
  theme(
    text = element_text(size = 16),
    legend.position = c(0.52, 0.95),
    legend.title = element_blank()
  ) +  guides(colour = guide_legend(nrow = 2, ncol = 3),
              shape = guide_legend(nrow = 2, ncol = 3))
MeasurementsPlot
ggsave(png_file("Measurements_Figure1b"),
       width = 12,
       height = 5.5)

# Drugs/Procedures
drugs_query_patients <-
  paste0(
    "SELECT
       a.*,
       b.concept_name,
       d.concept_name as dose_units,
       c.visit_detail_start_datetime,
       CEILING(DATEDIFF(minute,
                        a.drug_exposure_start_datetime,
                        c.visit_detail_start_datetime) / 60) AS hour
     FROM drug_exposure a
     LEFT JOIN concept b
     ON a.drug_concept_id=b.concept_id
     LEFT JOIN
     (SELECT MIN(visit_detail_start_datetime) AS visit_detail_start_datetime,
             MAX(visit_detail_end_datetime) AS visit_detail_end_datetime,
             visit_occurrence_id
     FROM visit_detail
     GROUP BY visit_occurrence_id) c
     ON a.visit_occurrence_id=c.visit_occurrence_id
     LEFT JOIN concept d
     ON a.dose_unit_concept_id=d.concept_id
     WHERE (DATEDIFF(minute,
                     c.visit_detail_start_datetime,
                     drug_exposure_start_datetime) >= 0
            AND
            DATEDIFF(minute,
                     drug_exposure_start_datetime,
                     c.visit_detail_end_datetime) >= 0)"
  )


omop_drugFigure1Pat <- dbGetQuery(db, drugs_query_patients) %>%
  distinct()

# Dexamethasone
singlepatient_drugs <- omop_drugFigure1Pat %>%
  filter(
    grepl("Dexamethasone", concept_name) &
      hour >= 0,
    visit_occurrence_id == visit_occurrence_id_interest
  ) %>%
  group_by(concept_name, hour, visit_occurrence_id, person_id) %>%
  dplyr::summarise(n = n(), quantity = sum(quantity))

# O2 Flow rate - this is not in the drugs table
singlepatient_o2 <- omop_measFigure1Pat %>%
  filter(hour >= 0, visit_occurrence_id == visit_occurrence_id_interest) %>%
  filter(grepl("flow rate", concept_name)) %>%
  filter(!is.na(value_as_number)) %>%
  group_by(concept_name, hour, visit_occurrence_id, person_id) %>%
  summarise(n = n(),
            quantity = mean(value_as_number, na.rm = T))


Procedures <- rbind(singlepatient_drugs, singlepatient_o2)

Procedures$concept_name <- ifelse(
  grepl("oxygen", Procedures$concept_name),
  "Delivered Oxygen (O2) Flow Rate (L/min)",
  "Dexamethasone (mg)"
)

# Convert hours to days
Procedures$day <- Procedures$hour / 24

# Note, some of the limits may need to be modified based on patients
ProceduresPlot <-
  ggplot(data = Procedures,
         aes(x = day, y = quantity, colour = concept_name)) +
  geom_step(aes(linetype = factor(concept_name))) +
  geom_point(aes(shape = concept_name), size = 2) +
  scale_shape_manual(values = c(1, 8)) +
  scale_x_continuous(
    limits = c(0, 11.4),
    breaks = breaks,
    labels = labels ,
    expand = c(0, 0)
  ) +
  scale_y_continuous(
    limits = c(0, 17),
    breaks = seq(0, 17, 3),
    expand = c(0, 0)
  ) +
  ylab("Treatment value") +
  theme_classic() +
  scale_linetype_manual(
    values = c(
      "Delivered Oxygen (O2) Flow Rate (L/min)" = "solid",
      "Dexamethasone (mg)" = "blank"
    )
  ) +
  scale_colour_manual(values = c("blue", "darkgreen")) +
  theme(
    text = element_text(size = 16),
    legend.position = c(0.58, 0.95),
    legend.title = element_blank(),
    axis.text.x = element_blank(),
    axis.title.x = element_blank()
  ) +
  guides(
    colour = guide_legend(
      nrow = 1,
      ncol = 2,
      override.aes =  list(linetype = c(1, NA))
    ),
    shape = guide_legend(nrow = 1, ncol = 2),
    linetype = "none"
  )

ProceduresPlot

ggsave(png_file("Procedures_Figure1b"),
       width = 12,
       height = 3)

# Query visit of interest for progress line
visitd_query_patients <- paste0(
  paste(
    "SELECT
       a.*,
       b.visit_start_datetime,
       b.visit_end_datetime
     FROM visit_detail a
     LEFT JOIN visit_occurrence b
     ON a.visit_occurrence_id=b.visit_occurrence_id
     WHERE a.visit_occurrence_id=",
    visit_occurrence_id_interest
  )
)

omop_visdFigure1Pat <- dbGetQuery(db, visitd_query_patients) %>%
  distinct() %>%
  select(visit_detail_start_datetime,
         visit_detail_end_datetime,
         care_site_id)

# Create datatable to make a long dataset
omop_visdFigure1Pat = as.data.table(omop_visdFigure1Pat)

omop_visdFigure1Pat_exp = setDT(omop_visdFigure1Pat)[, .(
  care_site_id = care_site_id,
  hour = seq(visit_detail_start_datetime,
             visit_detail_end_datetime,
             by = "hour")
), by = 1:nrow(omop_visdFigure1Pat)]

omop_visdFigure1Pat_exp = omop_visdFigure1Pat_exp %>%
  arrange(hour)

omop_visdFigure1Pat_exp$hour2 = 1:nrow(omop_visdFigure1Pat_exp)

omop_visdFigure1Pat_exp$Height = 0

# Convert hours to days.
omop_visdFigure1Pat_exp$day = omop_visdFigure1Pat_exp$hour2 / 24

ProgressLine = ggplot(omop_visdFigure1Pat_exp, aes(
  x = day,
  y = Height,
  colour = factor(care_site_id)
)) +
  geom_path() +
  geom_point(shape = 15) +
  scale_y_continuous(expand = c(0, 0)) +
  scale_x_continuous(
    limits = c(0, 11.4),
    breaks = breaks,
    labels = labels ,
    expand = c(0, 0)
  ) +
  scale_colour_manual(values = c("white", "grey", "black")) +
  theme(legend.position = "none")

ggsave(png_file("ProgressLine_Figure1b"),
       height = 3,
       width = 12)

# all the figures from this code were then brought into a PowerPoint slide and
# arranged and text annotations were added.
