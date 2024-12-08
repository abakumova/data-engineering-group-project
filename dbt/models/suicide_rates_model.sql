WITH raw_suicide_data AS (
    SELECT
        "SpatialDimValueCode" AS country_code,
        "Location" AS country,
        CAST("Period" AS INTEGER) AS year,
        "Dim1" AS sex,
        CAST("FactValueNumeric" AS FLOAT) AS suicide_rate
    FROM analytics.suicide_rates
),
cleaned_data AS (
    SELECT
        country_code,
        country,
        year,
        CASE
            WHEN LOWER(sex) = 'male' THEN 'Male'
            WHEN LOWER(sex) = 'female' THEN 'Female'
            ELSE 'Unknown'
        END AS sex,
        suicide_rate
    FROM raw_suicide_data
    WHERE suicide_rate IS NOT NULL
)
SELECT
    country_code,
    country,
    year,
    sex,
    suicide_rate
FROM cleaned_data
ORDER BY country, year, sex
