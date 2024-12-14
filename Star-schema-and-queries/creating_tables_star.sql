-- Create Dimension Tables
CREATE TABLE Dimension_Location (
    LocationKey INTEGER PRIMARY KEY,
    LocationCode VARCHAR(10),
    LocationName VARCHAR(100),
    ParentLocation VARCHAR(100),
    LocationType VARCHAR(50)
);

CREATE TABLE Dimension_Period (
    PeriodKey INTEGER PRIMARY KEY,
    Year INTEGER,
    PeriodType VARCHAR(50),
    IsLatestYear BOOLEAN
);

CREATE TABLE Dimension_Indicator (
    IndicatorKey INTEGER PRIMARY KEY,
    IndicatorCode VARCHAR(20),
    IndicatorName VARCHAR(255),
    ValueType VARCHAR(50),
    Category VARCHAR(100),
    UnitOfMeasure VARCHAR(50)
);

CREATE TABLE Dimension_DataSource (
    DataSourceKey INTEGER PRIMARY KEY,
    DataSourceName VARCHAR(255),
    URL VARCHAR(255)
);

CREATE TABLE Dimension_Gender (
    GenderKey INTEGER PRIMARY KEY,
    GenderCode VARCHAR(10),
    GenderName VARCHAR(50)
);

-- Create Fact Table
CREATE TABLE Fact_HealthMetrics (
    FactID INTEGER PRIMARY KEY,
    LocationKey INTEGER REFERENCES Dimension_Location(LocationKey),
    PeriodKey INTEGER REFERENCES Dimension_Period(PeriodKey),
    IndicatorKey INTEGER REFERENCES Dimension_Indicator(IndicatorKey),
    DataSourceKey INTEGER REFERENCES Dimension_DataSource(DataSourceKey),
    GenderKey INTEGER REFERENCES Dimension_Gender(GenderKey),
    FactValueNumeric FLOAT,
    FactValueUoM VARCHAR(50),
    DateModified DATE
);
