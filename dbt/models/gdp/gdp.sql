WITH raw_data AS (
    SELECT
        "Country Name" AS country_name,
        "Indicator Name" AS indicator_name,
        '2013' AS year, CAST("2013" AS FLOAT) AS value
        UNION ALL
        SELECT "Country Name", "Indicator Name", '2013', CAST("2013" AS FLOAT)
        UNION ALL
        SELECT "Country Name", "Indicator Name", '2014', CAST("2014" AS FLOAT)
        UNION ALL
        SELECT "Country Name", "Indicator Name", '2015', CAST("2015" AS FLOAT)
        UNION ALL
        SELECT "Country Name", "Indicator Name", '2016', CAST("2016" AS FLOAT)
        UNION ALL
        SELECT "Country Name", "Indicator Name", '2017', CAST("2017" AS FLOAT)
        UNION ALL
        SELECT "Country Name", "Indicator Name", '2018', CAST("2018" AS FLOAT)
        UNION ALL
        SELECT "Country Name", "Indicator Name", '2019', CAST("2019" AS FLOAT)
),
cleaned_data AS (
    SELECT
        country_name,
        indicator_name,
        year,
        value
    FROM raw_data
    WHERE value IS NOT NULL
)

SELECT * FROM cleaned_data;
