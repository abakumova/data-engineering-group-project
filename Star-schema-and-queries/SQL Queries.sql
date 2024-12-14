-- Suicide Rates vs. Mental Hospital Beds in the Same Year and Country

SELECT
  loc.LocationName,
  p.Year,
  sr.FactValueNumeric AS SuicideRatePer100k,
  bm.FactValueNumeric AS BedsInMentalHospitalsPer100k
FROM Fact_HealthMetrics sr
JOIN Dimension_Indicator i_sr ON sr.IndicatorKey = i_sr.IndicatorKey
JOIN Dimension_Location loc ON sr.LocationKey = loc.LocationKey
JOIN Dimension_Period p ON sr.PeriodKey = p.PeriodKey
JOIN Fact_HealthMetrics bm ON bm.LocationKey = sr.LocationKey
                            AND bm.PeriodKey = sr.PeriodKey
JOIN Dimension_Indicator i_bm ON bm.IndicatorKey = i_bm.IndicatorKey
WHERE i_sr.IndicatorCode = 'MH_12'  -- Suicide rate
  AND i_bm.IndicatorCode = 'MH_16'  -- Beds in mental hospitals
  AND p.Year = 2017;                -- filter a specific year, adjust as needed


-- Suicide Rates vs. Psychiatrists per 100,000
SELECT
  loc.LocationName,
  p.Year,
  sr.FactValueNumeric AS SuicideRatePer100k,
  psy.FactValueNumeric AS PsychiatristsPer100k
FROM Fact_HealthMetrics sr
JOIN Dimension_Indicator i_sr ON sr.IndicatorKey = i_sr.IndicatorKey
JOIN Dimension_Location loc ON sr.LocationKey = loc.LocationKey
JOIN Dimension_Period p ON sr.PeriodKey = p.PeriodKey
JOIN Fact_HealthMetrics psy ON psy.LocationKey = sr.LocationKey
                             AND psy.PeriodKey = sr.PeriodKey
JOIN Dimension_Indicator i_psy ON psy.IndicatorKey = i_psy.IndicatorKey
WHERE i_sr.IndicatorCode = 'MH_12'   -- Suicide rates
  AND i_psy.IndicatorCode = 'MH_6'   -- Psychiatrists
  AND p.Year = 2017;


-- Suicide Rates vs. General Hospital Beds
SELECT
  loc.LocationName,
  p.Year,
  sr.FactValueNumeric AS SuicideRatePer100k,
  bm.FactValueNumeric AS BedsInGeneralHospitalsPer100k
FROM Fact_HealthMetrics sr
JOIN Dimension_Indicator i_sr ON sr.IndicatorKey = i_sr.IndicatorKey
JOIN Dimension_Location loc ON sr.LocationKey = loc.LocationKey
JOIN Dimension_Period p ON sr.PeriodKey = p.PeriodKey
JOIN Fact_HealthMetrics bm ON bm.LocationKey = sr.LocationKey
                            AND bm.PeriodKey = sr.PeriodKey
JOIN Dimension_Indicator i_bm ON bm.IndicatorKey = i_bm.IndicatorKey
WHERE i_sr.IndicatorCode = 'MH_12'  -- Suicide rate
  AND i_bm.IndicatorCode = 'MH_13'  -- Beds in general hospitals
  AND p.Year = 2017;                -- filter a specific year


-- Suicide Rates vs. Community Residential Facility Beds

SELECT
  loc.LocationName,
  p.Year,
  sr.FactValueNumeric AS SuicideRatePer100k,
  bm.FactValueNumeric AS BedsInCommunityFacilitiesPer100k
FROM Fact_HealthMetrics sr
JOIN Dimension_Indicator i_sr ON sr.IndicatorKey = i_sr.IndicatorKey
JOIN Dimension_Location loc ON sr.LocationKey = loc.LocationKey
JOIN Dimension_Period p ON sr.PeriodKey = p.PeriodKey
JOIN Fact_HealthMetrics bm ON bm.LocationKey = sr.LocationKey
                            AND bm.PeriodKey = sr.PeriodKey
JOIN Dimension_Indicator i_bm ON bm.IndicatorKey = i_bm.IndicatorKey
WHERE i_sr.IndicatorCode = 'MH_12'  -- Suicide rate
  AND i_bm.IndicatorCode = 'MH_15'  -- Beds in community facilities
  AND p.Year = 2017;


-- Suicide Rates vs. Psychiatrists

SELECT
  loc.LocationName,
  p.Year,
  sr.FactValueNumeric AS SuicideRatePer100k,
  hr.FactValueNumeric AS PsychiatristsPer100k
FROM Fact_HealthMetrics sr
JOIN Dimension_Indicator i_sr ON sr.IndicatorKey = i_sr.IndicatorKey
JOIN Dimension_Location loc ON sr.LocationKey = loc.LocationKey
JOIN Dimension_Period p ON sr.PeriodKey = p.PeriodKey
JOIN Fact_HealthMetrics hr ON hr.LocationKey = sr.LocationKey
                            AND hr.PeriodKey = sr.PeriodKey
JOIN Dimension_Indicator i_hr ON hr.IndicatorKey = i_hr.IndicatorKey
WHERE i_sr.IndicatorCode = 'MH_12'  -- Suicide rate
  AND i_hr.IndicatorCode = 'MH_6'   -- Psychiatrists
  AND p.Year = 2017;


-- GDP vs. Beds in Mental Hospitals

SELECT
  loc.LocationName,
  p.Year,
  gdp.FactValueNumeric AS GDP_USD,
  bm.FactValueNumeric AS BedsInMentalHospitalsPer100k
FROM Fact_HealthMetrics gdp
JOIN Dimension_Indicator i_gdp ON gdp.IndicatorKey = i_gdp.IndicatorKey
JOIN Dimension_Location loc ON gdp.LocationKey = loc.LocationKey
JOIN Dimension_Period p ON gdp.PeriodKey = p.PeriodKey
JOIN Fact_HealthMetrics bm ON bm.LocationKey = gdp.LocationKey
                             AND bm.PeriodKey = gdp.PeriodKey
JOIN Dimension_Indicator i_bm ON bm.IndicatorKey = i_bm.IndicatorKey
WHERE i_gdp.IndicatorCode = 'WITS_GDP'    -- GDP
  AND i_bm.IndicatorCode = 'MH_16'        -- Beds in mental hospitals
  AND p.Year = 2017;


-- GDP vs. Psychologists
SELECT
  loc.LocationName,
  p.Year,
  gdp.FactValueNumeric AS GDP_USD,
  hr.FactValueNumeric AS PsychologistsPer100k
FROM Fact_HealthMetrics gdp
JOIN Dimension_Indicator i_gdp ON gdp.IndicatorKey = i_gdp.IndicatorKey
JOIN Dimension_Location loc ON gdp.LocationKey = loc.LocationKey
JOIN Dimension_Period p ON gdp.PeriodKey = p.PeriodKey
JOIN Fact_HealthMetrics hr ON hr.LocationKey = gdp.LocationKey
                             AND hr.PeriodKey = gdp.PeriodKey
JOIN Dimension_Indicator i_hr ON hr.IndicatorKey = i_hr.IndicatorKey
WHERE i_gdp.IndicatorCode = 'WITS_GDP'    -- GDP
  AND i_hr.IndicatorCode = 'MH_9'         -- Psychologists
  AND p.Year = 2017;


-- Retrieving Country, Year, and Suicide Rates for Mapping

SELECT
  loc.LocationName AS country,
  p.Year,
  sr.FactValueNumeric AS SuicideRatePer100k
FROM Fact_HealthMetrics sr
JOIN Dimension_Indicator i_sr ON sr.IndicatorKey = i_sr.IndicatorKey
JOIN Dimension_Location loc ON sr.LocationKey = loc.LocationKey
JOIN Dimension_Period p ON sr.PeriodKey = p.PeriodKey
WHERE i_sr.IndicatorCode = 'MH_12'    -- Suicide rates
  AND p.Year = 2017;


-- GDP versus suicide rates
SELECT
  loc.LocationName,
  p.Year,
  gdp.FactValueNumeric AS GDP_USD,
  sr.FactValueNumeric AS SuicideRatePer100k
FROM Fact_HealthMetrics gdp
JOIN Dimension_Indicator i_gdp ON gdp.IndicatorKey = i_gdp.IndicatorKey
JOIN Dimension_Location loc ON gdp.LocationKey = loc.LocationKey
JOIN Dimension_Period p ON gdp.PeriodKey = p.PeriodKey
JOIN Fact_HealthMetrics sr ON sr.LocationKey = gdp.LocationKey
                             AND sr.PeriodKey = gdp.PeriodKey
JOIN Dimension_Indicator i_sr ON sr.IndicatorKey = i_sr.IndicatorKey
WHERE i_gdp.IndicatorCode = 'WITS_GDP'    -- GDP indicator
  AND i_sr.IndicatorCode = 'MH_12'        -- Suicide rate indicator
ORDER BY loc.LocationName, p.Year;