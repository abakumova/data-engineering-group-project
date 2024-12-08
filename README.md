# Data Engineering ([LTAT.02.007](https://courses.cs.ut.ee/2024/dataeng/fall))
## Suicide Rate Investigation Project

This project investigates the relationship between suicide rates and the availability of mental health services (number of professionals and beds for care) in conjunction with GDP.

## Project Overview

- **Topic**: Analysis of the potential correlation between mental health service availability (number of professionals and beds) and GDP on suicide rates.
- **Goal**: To understand if and how mental health infrastructure and economic factors influence suicide rates.

## Prerequisites

- **Docker**: Ensure Docker is installed on your system.
- **Docker Compose**: Ensure Docker Compose is installed and available in your PATH.

## Setup Instructions

1. **Clone the Repository**  
2. ``docker-compose up airflow-init``
3. ``docker-compose up -d``
4. **Access the Airflow Web Interface**
http://localhost:8080

Use the following default credentials to log in:

- Username: **airflow**
- Password: **airflow**
   
5. **Access the Minio Interface**
http://localhost:9001/login

Use the following default credentials to log in:

- Username: **minioadmin**
- Password: **minioadmin**

Create bucket **warehouse**

## Datasets

1. **Suicide Rate**
   - Source: [WHO Suicide Rates](https://www.who.int/data/gho/data/themes/mental-health/suicide-rates)

2. **Beds for Mental Health**
   - General hospitals (per 100,000): [WHO Beds in General Hospitals](https://www.who.int/data/gho/data/indicators/indicator-details/GHO/beds-for-mental-health-in-general-hospitals-(per-100-000))
   - Community residential facilities (per 100,000): [WHO Beds in Community Residential Facilities](https://www.who.int/data/gho/data/indicators/indicator-details/GHO/beds-in-community-residential-facilities-(per-100-000))
   - Mental hospitals (per 100,000): [WHO Beds in Mental Hospitals](https://www.who.int/data/gho/data/indicators/indicator-details/GHO/beds-in-mental-hospitals-(per-100-000))

3. **Human Resources for Mental Health**
   - Psychiatrists (per 100,000): [WHO Psychiatrists](https://www.who.int/data/gho/data/indicators/indicator-details/GHO/psychiatrists-working-in-mental-health-sector-(per-100-000))
   - Nurses (per 100,000): [WHO Nurses](https://www.who.int/data/gho/data/indicators/indicator-details/GHO/nurses-working-in-mental-health-sector-(per-100-000))
   - Psychologists (per 100,000): [WHO Psychologists](https://www.who.int/data/gho/data/indicators/indicator-details/GHO/psychologists-working-in-mental-health-sector-(per-100-000))

4. **GDP Data**
   - Gross Domestic Product (in USD): [World Bank GDP Data](https://wits.worldbank.org/CountryProfile/en/country/by-country/startyear/ltst/endyear/ltst/indicator/NY-GDP-MKTP-CD)

4. **Mental Health**
   - Mental illnesses prevalence: [Dataset](https://www.kaggle.com/datasets/imtkaggleteam/mental-health)
## Research Questions

- Does the number of beds for mental health patients have a positive effect on suicide rates?
- Is there a correlation between GDP and mental health service availability?
- Does the number of mental health professionals influence suicide rates? Are some roles more impactful than others?
- Further exploratory questions may arise during data analysis.

## Technology Stack

- **Data Orchestration**: Apache Airflow
- **Database**: DuckDB
- **Data Transformation**: dbt
- **Versioning**: Apache Iceberg
- **Visualization**: Streamlit (for dashboards) and GeoPandas (for geospatial visualizations)
- **Additional**: Predictive modeling for further insights

UML Data Schema [link](//www.plantuml.com/plantuml/png/xLPDRzim3BtxL-ZMmv33sjjEHHDa2POKGT8D68QXu6pE4M1BWKHZ2zR-zscQvRYCMjvwwYryV2_aepxaQq_Wg6kRuaBDT8l6QsjyWOOq7K_8_T9KkWIoJlqfb7gdXdXgArGVr8wSDhNwjldqFUGdaqATA4CwW5-WwV3kUC0J-5YSbrlPe0m_-cgxGuYW2VceVxxc1XmQIkedlBFiE9Emr1d7N-vsWqxRgk-r3_BjnHmlilGh8FJq5S45_CRQ1i5aAysmjRM3mrwNlgyNak-5gVoZiA8HO_5bXdF9Orug9QuouYiUPyHPbq2mivqhCNaS07aBZCJZXGHJTr0BXJ_EDdt5gUqCkUEpPHBX5jgd_tcXvb2I8lZHO3f74RhVTRhK5Mw1i5-xY2zdGRyqX3Mw_FbXJSmZXHs972TjHGxp7SEoqgmZBROUixTT6ygKWJ1lBD0uzTck1rr6ihA_k6gKNUXwpz9JsxPeo0Vu3d_J_sPnRIlzKsFm6SEqi6diwn10iq1eQ7-3rUkD_ctqYwu5iqKVBRtUuDy67-ynUThZIuvxZO_H80j0-C-XjNZFRR69RwFVwuClpg-uHpkPF3palY5aykaBGEQczrTYylTJuBENg2XojudWKX1mfpAVkvNPtB6iuQxvGHOy2tJcM61plvmhk-KAqMlOv-Ui01RNQAgkRVu2)