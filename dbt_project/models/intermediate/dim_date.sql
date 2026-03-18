-- Got this from https://sekuel.com/learn-sql/duckdb-cookbook/generate-date-dimension-table/
-- I'm not really familiar with duckdb and this is probably overkill for the test, but it works
WITH generate_date AS (
    SELECT CAST(RANGE AS DATE) AS the_date
    FROM RANGE(DATE '2010-01-01', DATE '2030-12-31', INTERVAL 1 DAY)
)
SELECT 
    the_date,
    strftime(the_date, '%Y%m%d') AS date_key,
    DAYOFYEAR(the_date) AS day_of_year, 
    YEARWEEK(the_date) AS week_key,
    WEEKOFYEAR(the_date) AS week_of_year,
    DAYOFWEEK(the_date) AS day_of_week,
    ISODOW(the_date) AS iso_day_of_week,
    DAYNAME(the_date) AS day_name,
    DATE_TRUNC('week', the_date)::DATE AS first_day_of_week,
    DATE_TRUNC('week', the_date)::DATE + 6 AS last_day_of_week,
    YEAR(the_date) || RIGHT('0' || MONTH(the_date), 2) AS month_key,
    MONTH(the_date) AS month_of_year,
    DAYOFMONTH(the_date) AS day_of_month,
    LEFT(MONTHNAME(the_date), 3) AS month_name_short,
    MONTHNAME(the_date) AS month_name,
    DATE_TRUNC('month', the_date) AS first_day_of_month,
    LAST_DAY(the_date) AS last_day_of_month,
    CAST(YEAR(the_date) || QUARTER(the_date) AS INT) AS quarter_key,
    QUARTER(the_date) AS quarter_of_year,
    CAST(the_date - DATE_TRUNC('Quarter', the_date)::DATE + 1 AS INT) AS day_of_quarter,
    ('Q' || QUARTER(the_date)) AS quarter_desc_short,
    ('Quarter ' || QUARTER(the_date)) AS quarter_desc,
    DATE_TRUNC('quarter', the_date) AS first_day_of_quarter,
    LAST_DAY(DATE_TRUNC('quarter', the_date)::DATE + INTERVAL 2 MONTH) AS last_day_of_quarter,
    CAST(YEAR(the_date) AS INT) AS year_key,
    DATE_TRUNC('Year', the_date)::DATE AS first_day_of_year,
    DATE_TRUNC('Year', the_date)::DATE - 1 + INTERVAL 1 YEAR AS last_day_of_year,
    ROW_NUMBER() OVER (PARTITION BY YEAR(the_date), MONTH(the_date), DAYOFWEEK(the_date) ORDER BY the_date) AS ordinal_weekday_of_month
FROM generate_date