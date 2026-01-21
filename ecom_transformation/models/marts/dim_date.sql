WITH date_spine AS (
    -- Generates a list of days from 2016 to 2020 
    SELECT 
        CAST(date_day AS DATE) as full_date
    FROM generate_series(
        '2016-01-01'::date, 
        '2020-12-31'::date, 
        '1 day'::interval
    ) as date_day
)

SELECT
    -- Surrogate Key (Integers are often preferred for Date Keys, e.g., 20180101)
    CAST(TO_CHAR(full_date, 'YYYYMMDD') AS INTEGER) AS date_key,
    full_date AS date,
    CAST(EXTRACT(YEAR FROM full_date) AS INTEGER) AS year,
    CAST(EXTRACT(MONTH FROM full_date) AS INTEGER) AS month,
    CAST(EXTRACT(DAY FROM full_date) AS INTEGER) AS day,
    CAST(full_date AS TIMESTAMP) AS timestamp

FROM date_spine