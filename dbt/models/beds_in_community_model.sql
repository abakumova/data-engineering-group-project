WITH raw_beds_community_data AS (
    SELECT
        SpatialDimValueCode AS country_code,
        Location AS country,
        CAST(Period AS INTEGER) AS year,
        CAST(FactValueNumeric AS DOUBLE) AS beds_per_100k
    FROM analytics.beds_in_community
),
cleaned_data AS (
    SELECT
        country_code,
        country,
        year,
        beds_per_100k
    FROM raw_beds_community_data
    WHERE beds_per_100k IS NOT NULL
)
SELECT
    country_code,
    country,
    year,
    beds_per_100k
FROM cleaned_data
ORDER BY country, year
