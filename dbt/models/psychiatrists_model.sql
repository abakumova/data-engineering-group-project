WITH raw_psychiatrists_data AS (
    SELECT
        "SpatialDimValueCode" AS country_code,
        "Location" AS country,
        CAST("Period" AS INTEGER) AS year,
        CAST("FactValueNumeric" AS FLOAT) AS psychiatrists_per_100k
    FROM analytics.psychiatrists
),
cleaned_data AS (
    SELECT
        country_code,
        country,
        year,
        psychiatrists_per_100k
    FROM raw_psychiatrists_data
    WHERE psychiatrists_per_100k IS NOT NULL
)
SELECT
    country_code,
    country,
    year,
    psychiatrists_per_100k
FROM cleaned_data
ORDER BY country, year
