# DECOVID Data Descriptor Paper

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.16325641.svg)](https://doi.org/10.5281/zenodo.16325641)

This repository is archived on Zenodo:

Bakewell, N., Goudie, R. J. B., Gardiner, S., Karoune, E., Rockenschaub, P., Green, B., Nicholls, H., Whitaker, K. J., & Aslett, L. (2025). alan-turing-institute/DECOVID-data-paper: DECOVID data paper repository (Version V1). Zenodo. https://doi.org/10.5281/zenodo.16325641


## Introduction

The DECOVID dataset contains comprehensive electronic healthcare record (EHR) data collected from patients admitted to two large, digitally-mature teaching hospitals in the United Kingdom between 1st January 2020 and 28th February 2021,  with follow-up running until the 28th March 2021 and 13th April 2021, for the two hospitals respectively. The two hospital trusts involved were University Hospitals Birmingham and University College London Hosptials.

The development of the DECOVID database was motivated by the COVID-19 pandemic with the aim of answering clinically important questions to support the COVID-19 response.

Raw data were extracted from local EHRs and transformed into the [Observational Health Data Sciences and Informatics (OHDSI) Common Data Model version 5.3.1](https://ohdsi.github.io/CommonDataModel/). This standardises the dataset making it more useable for data analysts and interoperable for researchers outside of the DECOVID project.

These data include longitudinal physiology, treatments, laboratory findings, diagnoses and outcomes.

The database includes 165,420 patients across 256,804 hospital presentations; 16.7 million hours of clinical care; 3,752 deaths (both COVID-19- and non-COVID-19-related); 108 million measured clinical observations encompassing vital signs, acute physiology and laboratory findings; 2.64 million clinical diagnoses relating to both acute and chronic health conditions; and 15.19 million drug administration events.

## Data access information
See https://healthdatagateway.org/en/dataset/998

## Contact information


## Links:
Information about the dataset:
* **DECOVID Tabulations** - [link to folder](https://github.com/alan-turing-institute/DECOVID-data-paper/tree/main/Tabulations)
   * This folder contains high level tabulations of the concepts in the DECOVID dataset.
* **DECOVID exclusion lists** - [link to folder](https://github.com/alan-turing-institute/DECOVID-data-paper/tree/main/Exclusion-Lists)
  * This folder contains lists of excluded diagnoses
* **DECOVID care sites mapping** - [link to folder](https://github.com/alan-turing-institute/DECOVID-data-paper/tree/main/Mapping/care_sites.csv)
  * Contains the mapping used for care sites in the DECOVID database (this is a non-standard vocabulary)

Code:
* **DECOVID Code** - [link to folder](https://github.com/alan-turing-institute/DECOVID-data-paper/tree/main/DECOVID-data-descriptor-paper-code)
   * This folder contains the code used for the figures in the data paper.
* **DECOVID Data Definition Language (DDL)** - [link to folder](https://github.com/alan-turing-institute/DECOVID-data-paper/tree/main/Data-Definition-Language)
  * This folder the DDL for the dataset.

Other information:
* **OMOP Wiki** - [link to wiki](https://github.com/alan-turing-institute/DECOVID-data-paper/wiki/OMOP-wiki)
  * The wiki contains information about the specific version of OMOP used in this dataset. The OMOP data model version 5.3 is being used as the common data model for DECOVID.
* **DECOVID Protocol** - [link to folder](https://github.com/alan-turing-institute/DECOVID-data-paper/tree/main/Protocol/Protocol.pdf)
   * Original study protocol

## License:
### For documentation:
[![CC BY 4.0][cc-by-shield]][cc-by]

This work is licensed under a
[Creative Commons Attribution 4.0 International License][cc-by].

[![CC BY 4.0][cc-by-image]][cc-by]

[cc-by]: http://creativecommons.org/licenses/by/4.0/
[cc-by-image]: https://i.creativecommons.org/l/by/4.0/88x31.png
[cc-by-shield]: https://img.shields.io/badge/License-CC%20BY%204.0-lightgrey.svg

### For Software/code:
### The MIT License
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

This work is licensed under a [MIT license](https://opensource.org/licenses/MIT)

