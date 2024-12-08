WITH raw_gdp AS (
    SELECT
        country_name AS country,
        indicator_name AS indicator,
        "1988", "1989", "1990", "1991", "1992", "1993", "1994", "1995", "1996", "1997",
        "1998", "1999", "2000", "2001", "2002", "2003", "2004", "2005", "2006", "2007",
        "2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017",
        "2018", "2019", "2020", "2021", "2022"
    FROM analytics.gdp
    WHERE indicator_name = 'GDP (current US$)'
),
gdp_long AS (
    SELECT country, '1988' AS year, CAST("1988" AS FLOAT) AS gdp_in_usd FROM raw_gdp WHERE "1988" IS NOT NULL
    UNION ALL
    SELECT country, '1989' AS year, CAST("1989" AS FLOAT) AS gdp_in_usd FROM raw_gdp WHERE "1989" IS NOT NULL
    UNION ALL
    SELECT country, '1990' AS year, CAST("1990" AS FLOAT) AS gdp_in_usd FROM raw_gdp WHERE "1990" IS NOT NULL
    UNION ALL
    SELECT country, '1991' AS year, CAST("1991" AS FLOAT) AS gdp_in_usd FROM raw_gdp WHERE "1991" IS NOT NULL
    UNION ALL
    SELECT country, '1992' AS year, CAST("1992" AS FLOAT) AS gdp_in_usd FROM raw_gdp WHERE "1992" IS NOT NULL
    UNION ALL
    SELECT country, '1993' AS year, CAST("1993" AS FLOAT) AS gdp_in_usd FROM raw_gdp WHERE "1993" IS NOT NULL
    UNION ALL
    SELECT country, '1994' AS year, CAST("1994" AS FLOAT) AS gdp_in_usd FROM raw_gdp WHERE "1994" IS NOT NULL
    UNION ALL
    SELECT country, '1995' AS year, CAST("1995" AS FLOAT) AS gdp_in_usd FROM raw_gdp WHERE "1995" IS NOT NULL
    UNION ALL
    SELECT country, '1996' AS year, CAST("1996" AS FLOAT) AS gdp_in_usd FROM raw_gdp WHERE "1996" IS NOT NULL
    UNION ALL
    SELECT country, '1997' AS year, CAST("1997" AS FLOAT) AS gdp_in_usd FROM raw_gdp WHERE "1997" IS NOT NULL
    UNION ALL
    SELECT country, '1998' AS year, CAST("1998" AS FLOAT) AS gdp_in_usd FROM raw_gdp WHERE "1998" IS NOT NULL
    UNION ALL
    SELECT country, '1999' AS year, CAST("1999" AS FLOAT) AS gdp_in_usd FROM raw_gdp WHERE "1999" IS NOT NULL
    UNION ALL
    SELECT country, '2000' AS year, CAST("2000" AS FLOAT) AS gdp_in_usd FROM raw_gdp WHERE "2000" IS NOT NULL
    UNION ALL
    SELECT country, '2001' AS year, CAST("2001" AS FLOAT) AS gdp_in_usd FROM raw_gdp WHERE "2001" IS NOT NULL
    UNION ALL
    SELECT country, '2002' AS year, CAST("2002" AS FLOAT) AS gdp_in_usd FROM raw_gdp WHERE "2002" IS NOT NULL
    UNION ALL
    SELECT country, '2003' AS year, CAST("2003" AS FLOAT) AS gdp_in_usd FROM raw_gdp WHERE "2003" IS NOT NULL
    UNION ALL
    SELECT country, '2004' AS year, CAST("2004" AS FLOAT) AS gdp_in_usd FROM raw_gdp WHERE "2004" IS NOT NULL
    UNION ALL
    SELECT country, '2005' AS year, CAST("2005" AS FLOAT) AS gdp_in_usd FROM raw_gdp WHERE "2005" IS NOT NULL
    UNION ALL
    SELECT country, '2006' AS year, CAST("2006" AS FLOAT) AS gdp_in_usd FROM raw_gdp WHERE "2006" IS NOT NULL
    UNION ALL
    SELECT country, '2007' AS year, CAST("2007" AS FLOAT) AS gdp_in_usd FROM raw_gdp WHERE "2007" IS NOT NULL
    UNION ALL
    SELECT country, '2008' AS year, CAST("2008" AS FLOAT) AS gdp_in_usd FROM raw_gdp WHERE "2008" IS NOT NULL
    UNION ALL
    SELECT country, '2009' AS year, CAST("2009" AS FLOAT) AS gdp_in_usd FROM raw_gdp WHERE "2009" IS NOT NULL
    UNION ALL
    SELECT country, '2010' AS year, CAST("2010" AS FLOAT) AS gdp_in_usd FROM raw_gdp WHERE "2010" IS NOT NULL
    UNION ALL
    SELECT country, '2011' AS year, CAST("2011" AS FLOAT) AS gdp_in_usd FROM raw_gdp WHERE "2011" IS NOT NULL
    UNION ALL
    SELECT country, '2012' AS year, CAST("2012" AS FLOAT) AS gdp_in_usd FROM raw_gdp WHERE "2012" IS NOT NULL
    UNION ALL
    SELECT country, '2013' AS year, CAST("2013" AS FLOAT) AS gdp_in_usd FROM raw_gdp WHERE "2013" IS NOT NULL
    UNION ALL
    SELECT country, '2014' AS year, CAST("2014" AS FLOAT) AS gdp_in_usd FROM raw_gdp WHERE "2014" IS NOT NULL
    UNION ALL
    SELECT country, '2015' AS year, CAST("2015" AS FLOAT) AS gdp_in_usd FROM raw_gdp WHERE "2015" IS NOT NULL
    UNION ALL
    SELECT country, '2016' AS year, CAST("2016" AS FLOAT) AS gdp_in_usd FROM raw_gdp WHERE "2016" IS NOT NULL
    UNION ALL
    SELECT country, '2017' AS year, CAST("2017" AS FLOAT) AS gdp_in_usd FROM raw_gdp WHERE "2017" IS NOT NULL
    UNION ALL
    SELECT country, '2018' AS year, CAST("2018" AS FLOAT) AS gdp_in_usd FROM raw_gdp WHERE "2018" IS NOT NULL
    UNION ALL
    SELECT country, '2019' AS year, CAST("2019" AS FLOAT) AS gdp_in_usd FROM raw_gdp WHERE "2019" IS NOT NULL
    UNION ALL
    SELECT country, '2020' AS year, CAST("2020" AS FLOAT) AS gdp_in_usd FROM raw_gdp WHERE "2020" IS NOT NULL
    UNION ALL
    SELECT country, '2021' AS year, CAST("2021" AS FLOAT) AS gdp_in_usd FROM raw_gdp WHERE "2021" IS NOT NULL
    UNION ALL
    SELECT country, '2022' AS year, CAST("2022" AS FLOAT) AS gdp_in_usd FROM raw_gdp WHERE "2022" IS NOT NULL
)
SELECT
    country,
    CAST(year AS INTEGER) AS year,
    gdp_in_usd
FROM gdp_long
ORDER BY country, year