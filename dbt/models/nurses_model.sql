WITH raw_nurses_data AS (
    SELECT
        "SpatialDimValueCode" AS country_code,
        "Location" AS country,
        CAST("Period" AS INTEGER) AS year,
        CAST("FactValueNumeric" AS FLOAT) AS nurses_per_100k
    FROM analytics.nurses
),
cleaned_data AS (
    SELECT
        country_code,
        country,
        year,
        nurses_per_100k
    FROM raw_nurses_data
    WHERE nurses_per_100k IS NOT NULL
)
SELECT
    country_code,
    country,
    year,
    nurses_per_100k
FROM cleaned_data
ORDER BY country, year
