WITH suicide_rates AS (
    SELECT
        "SpatialDimValueCode" AS country_code,
        "Location" AS country,
        CAST("Period" AS INTEGER) AS year,
        CAST("FactValueNumeric" AS FLOAT) AS suicide_rate
    FROM {{ source('datasets', 'suicide_rates') }}
),

mental_illnesses AS (
    SELECT
        "Entity" AS country,
        "Code" AS country_code,
        CAST("Year" AS INTEGER) AS year,
        CAST("Schizophrenia disorders (share of population) - Sex: Both - Age: Age-standardized" AS FLOAT) AS schizophrenia_share,
        CAST("Depressive disorders (share of population) - Sex: Both - Age: Age-standardized" AS FLOAT) AS depressive_disorders_share,
        CAST("Anxiety disorders (share of population) - Sex: Both - Age: Age-standardized" AS FLOAT) AS anxiety_disorders_share
    FROM {{ source('datasets', 'mental_illnesses') }}
),

beds_general_hospitals AS (
    SELECT
        "SpatialDimValueCode" AS country_code,
        "Location" AS country,
        CAST("Period" AS INTEGER) AS year,
        CAST("FactValueNumeric" AS FLOAT) AS beds_in_general_hospitals
    FROM {{ source('datasets', 'beds_in_general_hospitals') }}
),

beds_community AS (
    SELECT
        "SpatialDimValueCode" AS country_code,
        "Location" AS country,
        CAST("Period" AS INTEGER) AS year,
        CAST("FactValueNumeric" AS FLOAT) AS beds_in_community
    FROM {{ source('datasets', 'beds_in_community') }}
),

beds_mental_hospitals AS (
    SELECT
        "SpatialDimValueCode" AS country_code,
        "Location" AS country,
        CAST("Period" AS INTEGER) AS year,
        CAST("FactValueNumeric" AS FLOAT) AS beds_in_mental_hospitals
    FROM {{ source('datasets', 'beds_in_mental_hospitals') }}
),

nurses AS (
    SELECT
        "SpatialDimValueCode" AS country_code,
        "Location" AS country,
        CAST("Period" AS INTEGER) AS year,
        CAST("FactValueNumeric" AS FLOAT) AS nurses_per_100k
    FROM {{ source('datasets', 'nurses') }}
),

psychiatrists AS (
    SELECT
        "SpatialDimValueCode" AS country_code,
        "Location" AS country,
        CAST("Period" AS INTEGER) AS year,
        CAST("FactValueNumeric" AS FLOAT) AS psychiatrists_per_100k
    FROM {{ source('datasets', 'psychiatrists') }}
),

psychologists AS (
    SELECT
        "SpatialDimValueCode" AS country_code,
        "Location" AS country,
        CAST("Period" AS INTEGER) AS year,
        CAST("FactValueNumeric" AS FLOAT) AS psychologists_per_100k
    FROM {{ source('datasets', 'psychologists') }}
),

gdp AS (
    SELECT
        "Country Name" AS country,
        "Country Code" AS country_code,
        CAST(UNNEST([1988, 1989, 1990, 1991, 1992]) AS INTEGER) AS year,
        CAST(UNNEST([3854235264.37171, 4539496562.95373, 5220825048.64637, 6226198934.83329, 6971383338.70803]) AS FLOAT) AS gdp_in_usd
    FROM {{ source('datasets', 'gdp') }}
)

SELECT
    COALESCE(suicide_rates.country, mental_illnesses.country, beds_general_hospitals.country) AS country,
    COALESCE(suicide_rates.year, mental_illnesses.year, beds_general_hospitals.year) AS year,
    suicide_rates.suicide_rate,
    mental_illnesses.schizophrenia_share,
    mental_illnesses.depressive_disorders_share,
    mental_illnesses.anxiety_disorders_share,
    beds_general_hospitals.beds_in_general_hospitals,
    beds_community.beds_in_community,
    beds_mental_hospitals.beds_in_mental_hospitals,
    nurses.nurses_per_100k,
    psychiatrists.psychiatrists_per_100k,
    psychologists.psychologists_per_100k,
    gdp.gdp_in_usd
FROM suicide_rates
LEFT JOIN mental_illnesses USING (country, year)
LEFT JOIN beds_general_hospitals USING (country, year)
LEFT JOIN beds_community USING (country, year)
LEFT JOIN beds_mental_hospitals USING (country, year)
LEFT JOIN nurses USING (country, year)
LEFT JOIN psychiatrists USING (country, year)
LEFT JOIN psychologists USING (country, year)
LEFT JOIN gdp USING (country, year)
ORDER BY country, year;
