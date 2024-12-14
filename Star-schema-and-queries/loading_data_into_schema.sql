-- Insert Data into Dimension Tables
INSERT INTO Dimension_Location (LocationKey, LocationCode, LocationName, ParentLocation, LocationType)
VALUES
    (1, 'AFG', 'Afghanistan', 'Asia', 'Country'),
    (2, 'GBR', 'United Kingdom', 'Europe', 'Country'),
    (3, 'VEN', 'Venezuela', 'Americas', 'Country');

INSERT INTO Dimension_Period (PeriodKey, Year, PeriodType, IsLatestYear)
VALUES
    (1, 2017, 'Year', TRUE),
    (2, 1990, 'Year', FALSE),
    (3, 1991, 'Year', FALSE);

INSERT INTO Dimension_Indicator (IndicatorKey, IndicatorCode, IndicatorName, ValueType, Category, UnitOfMeasure)
VALUES
    (1, 'MH_12', 'Age-standardized suicide rates', 'numeric', 'Suicide Rates', 'per 100,000'),
    (2, 'MH_16', 'Beds in mental hospitals', 'numeric', 'Mental Health Beds', 'per 100,000');

INSERT INTO Dimension_DataSource (DataSourceKey, DataSourceName, URL)
VALUES
    (1, 'WHO', 'https://www.who.int'),
    (2, 'World Bank', 'https://data.worldbank.org');

INSERT INTO Dimension_Gender (GenderKey, GenderCode, GenderName)
VALUES
    (1, 'SEX_MLE', 'Male'),
    (2, 'SEX_FMLE', 'Female'),
    (3, 'SEX_BTSX', 'Both sexes');

-- Insert Data into Fact Table
INSERT INTO Fact_HealthMetrics (FactID, LocationKey, PeriodKey, IndicatorKey, DataSourceKey, GenderKey, FactValueNumeric, FactValueUoM, DateModified)
VALUES
    (1, 1, 1, 1, 1, 3, 0.32, 'per 100,000', '2021-02-08'),
    (2, 1, 2, 2, 1, 3, 0.703, 'per 100,000', '2019-04-24'),
    (3, 2, 1, 2, 1, 3, 11.09, 'per 100,000', '2019-04-24');
