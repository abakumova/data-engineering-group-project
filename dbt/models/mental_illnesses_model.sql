WITH raw_mental_illnesses_data AS (
    SELECT
        "entity" AS country,
        "code" AS country_code,
        CAST("year" AS INTEGER) AS year,
        CAST("schizophrenia_disorders_(share_of_population)_-_sex:_both_-_age:_age-standardized" AS FLOAT) AS schizophrenia_share,
        CAST("depressive_disorders_(share_of_population)_-_sex:_both_-_age:_age-standardized" AS FLOAT) AS depressive_disorders_share,
        CAST("anxiety_disorders_(share_of_population)_-_sex:_both_-_age:_age-standardized" AS FLOAT) AS anxiety_disorders_share,
        CAST("bipolar_disorders_(share_of_population)_-_sex:_both_-_age:_age-standardized" AS FLOAT) AS bipolar_disorders_share,
        CAST("eating_disorders_(share_of_population)_-_sex:_both_-_age:_age-standardized" AS FLOAT) AS eating_disorders_share
    FROM analytics.mental_illnesses
),
cleaned_data AS (
    SELECT
        country,
        country_code,
        year,
        schizophrenia_share,
        depressive_disorders_share,
        anxiety_disorders_share,
        bipolar_disorders_share,
        eating_disorders_share
    FROM raw_mental_illnesses_data
    WHERE year IS NOT NULL
      AND (schizophrenia_share IS NOT NULL
           OR depressive_disorders_share IS NOT NULL
           OR anxiety_disorders_share IS NOT NULL
           OR bipolar_disorders_share IS NOT NULL
           OR eating_disorders_share IS NOT NULL)
)
SELECT
    country,
    country_code,
    year,
    schizophrenia_share,
    depressive_disorders_share,
    anxiety_disorders_share,
    bipolar_disorders_share,
    eating_disorders_share
FROM cleaned_data
ORDER BY country, year