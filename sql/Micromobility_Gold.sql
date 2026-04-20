USE MICROMOBILITY_DB.SILVER;

CREATE OR REPLACE TABLE MICROMOBILITY_DB.GOLD.TRIPS_GOLD AS
WITH monthly_district_counts AS (
    SELECT
        TRIP_YEAR,
        TRIP_MONTH,
        COUNCIL_DISTRICT_START,
        COUNT(*) AS MONTHLY_DISTRICT_TRIPS
    FROM MICROMOBILITY_DB.SILVER.TRIPS_CLEAN
    GROUP BY TRIP_YEAR, TRIP_MONTH, COUNCIL_DISTRICT_START
),

district_ranks AS (
    SELECT
        TRIP_YEAR,
        TRIP_MONTH,
        COUNCIL_DISTRICT_START,
        MONTHLY_DISTRICT_TRIPS,
        RANK() OVER (
            PARTITION BY TRIP_YEAR, TRIP_MONTH
            ORDER BY MONTHLY_DISTRICT_TRIPS DESC
        ) AS DISTRICT_MONTHLY_RANK
    FROM monthly_district_counts
)

SELECT
    s.TRIP_ID,
    s.VEHICLE_TYPE,
    s.TRIP_YEAR,
    s.TRIP_MONTH,
    s.TRIP_DATE,
    s.TRIP_HOUR,
    s.DAY_OF_WEEK_NUM,
    s.DAY_OF_WEEK_NAME,
    s.DAY_TYPE,

    TO_VARCHAR(s.TRIP_DATE, 'Mon YYYY')                        AS MONTH_YEAR,
    TO_VARCHAR(s.TRIP_DATE, 'YYYY-MM')                         AS MONTH_SORT,

    s.TRIP_DURATION_SEC,
    s.TRIP_DURATION_MIN,
    s.TRIP_DISTANCE_M,
    ROUND(s.TRIP_DISTANCE_M / 1000.0, 3)                       AS TRIP_DISTANCE_KM,

    s.COUNCIL_DISTRICT_START,
    s.DISTRICT_NAME,
    s.ZONE_CLASS,
    s.TIME_SEGMENT,
    s.IS_PEAK_HOUR,
    d.MONTHLY_DISTRICT_TRIPS,
    d.DISTRICT_MONTHLY_RANK,

    s.UNLOCK_FEE,
    s.PER_MINUTE_RATE,
    s.ESTIMATED_REVENUE,
    s.REVENUE_PER_MIN,

    CASE
        WHEN s.TRIP_DURATION_MIN < 5  THEN '1. Under 5 min'
        WHEN s.TRIP_DURATION_MIN < 10 THEN '2. 5-10 min'
        WHEN s.TRIP_DURATION_MIN < 20 THEN '3. 10-20 min'
        WHEN s.TRIP_DURATION_MIN < 30 THEN '4. 20-30 min'
        ELSE                               '5. Over 30 min'
    END                                                        AS DURATION_BUCKET,

    CASE
        WHEN s.ESTIMATED_REVENUE < 3.00  THEN '1. Low (under $3)'
        WHEN s.ESTIMATED_REVENUE < 6.00  THEN '2. Medium ($3-$6)'
        WHEN s.ESTIMATED_REVENUE < 10.00 THEN '3. High ($6-$10)'
        ELSE                                  '4. Premium (over $10)'
    END                                                        AS REVENUE_TIER,

    CASE
        WHEN s.ESTIMATED_REVENUE >= AVG(s.ESTIMATED_REVENUE) OVER (
            PARTITION BY s.COUNCIL_DISTRICT_START
        ) THEN 'Above district average'
        ELSE 'Below district average'
    END                                                        AS REVENUE_VS_DISTRICT_AVG,

    CASE
        WHEN s.ESTIMATED_REVENUE >= AVG(s.ESTIMATED_REVENUE) OVER ()
        THEN 'Above overall average'
        ELSE 'Below overall average'
    END                                                        AS REVENUE_VS_OVERALL_AVG,

    CASE
        WHEN s.COUNCIL_DISTRICT_START IN (
            SELECT COUNCIL_DISTRICT_START
            FROM MICROMOBILITY_DB.SILVER.TRIPS_CLEAN
            GROUP BY COUNCIL_DISTRICT_START
            ORDER BY COUNT(*) DESC
            LIMIT 3
        ) THEN 'Top 3 district'
        ELSE 'Other district'
    END                                                        AS CONCENTRATION_FLAG,

    CASE
        WHEN s.TRIP_MONTH IN (12, 1, 2)  THEN '1. Winter'
        WHEN s.TRIP_MONTH IN (3, 4, 5)   THEN '2. Spring'
        WHEN s.TRIP_MONTH IN (6, 7, 8)   THEN '3. Summer'
        WHEN s.TRIP_MONTH IN (9, 10, 11) THEN '4. Fall'
    END                                                        AS SEASON,

    s.TRIP_START_TS,
    s.TRIP_END_TS

FROM MICROMOBILITY_DB.SILVER.TRIPS_CLEAN s
LEFT JOIN district_ranks d
    ON s.TRIP_YEAR = d.TRIP_YEAR
    AND s.TRIP_MONTH = d.TRIP_MONTH
    AND s.COUNCIL_DISTRICT_START = d.COUNCIL_DISTRICT_START;