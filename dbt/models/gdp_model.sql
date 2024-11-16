WITH raw_gdp AS (
    SELECT
        "Country Name" AS country,
        "Indicator Name" AS indicator,
        *
    FROM {{ source('datasets', 'gdp') }}
    WHERE "Indicator Name" = 'GDP (current US$)'
),
gdp_long AS (
    SELECT
        country,
        CAST(years.column_name AS INTEGER) AS year,
        CAST(years.value AS FLOAT) AS gdp_in_usd
    FROM raw_gdp
    CROSS JOIN UNNEST(ARRAY[
        '1988', '1989', '1990', '1991', '1992', '1993', '1994', '1995', '1996', '1997',
        '1998', '1999', '2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007',
        '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017',
        '2018', '2019', '2020', '2021', '2022'
    ]) AS years(column_name, value)
    WHERE years.value IS NOT NULL
)
SELECT
    country,
    year,
    gdp_in_usd
FROM gdp_long
ORDER BY country, year;
