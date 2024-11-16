WITH raw_mental_illnesses_data AS (
    SELECT
        "Entity" AS country,
        "Code" AS country_code,
        CAST("Year" AS INTEGER) AS year,
        CAST("Schizophrenia disorders (share of population) - Sex: Both - Age: Age-standardized" AS FLOAT) AS schizophrenia_share,
        CAST("Depressive disorders (share of population) - Sex: Both - Age: Age-standardized" AS FLOAT) AS depressive_disorders_share,
        CAST("Anxiety disorders (share of population) - Sex: Both - Age: Age-standardized" AS FLOAT) AS anxiety_disorders_share,
        CAST("Bipolar disorders (share of population) - Sex: Both - Age: Age-standardized" AS FLOAT) AS bipolar_disorders_share,
        CAST("Eating disorders (share of population) - Sex: Both - Age: Age-standardized" AS FLOAT) AS eating_disorders_share
    FROM {{ source('datasets', 'mental_illnesses') }}
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
ORDER BY country, year;
