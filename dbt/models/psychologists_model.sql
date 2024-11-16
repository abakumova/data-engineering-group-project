WITH raw_psychologists_data AS (
    SELECT
        "SpatialDimValueCode" AS country_code,
        "Location" AS country,
        CAST("Period" AS INTEGER) AS year,
        CAST("FactValueNumeric" AS FLOAT) AS psychologists_per_100k
    FROM {{ source('datasets', 'psychologists') }}
),
cleaned_data AS (
    SELECT
        country_code,
        country,
        year,
        psychologists_per_100k
    FROM raw_psychologists_data
    WHERE psychologists_per_100k IS NOT NULL
)
SELECT
    country_code,
    country,
    year,
    psychologists_per_100k
FROM cleaned_data
ORDER BY country, year;
