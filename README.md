# Data Engineering ([LTAT.02.007](https://courses.cs.ut.ee/2024/dataeng/fall))
## Suicide Rate Investigation Project

This project investigates the relationship between suicide rates and the availability of mental health services (number of professionals and beds for care) in conjunction with GDP.

## Project Overview

- **Topic**: Analysis of the potential correlation between mental health service availability (number of professionals and beds) and GDP on suicide rates.
- **Goal**: To understand if and how mental health infrastructure and economic factors influence suicide rates.

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
