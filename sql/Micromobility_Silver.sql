USE MICROMOBILITY_DB.BRONZE;

CREATE OR REPLACE TABLE MICROMOBILITY_DB.SILVER.TRIPS_CLEAN AS
WITH base AS (
    SELECT
        TRIP_ID,
        VEHICLE_TYPE,
        CAST(REPLACE(YEAR, ',', '') AS INTEGER)                        AS TRIP_YEAR,
        CAST(REPLACE(TRIP_DURATION, ',', '') AS INTEGER)               AS TRIP_DURATION_SEC,
        CAST(REPLACE(TRIP_DISTANCE, ',', '') AS FLOAT)                 AS TRIP_DISTANCE_M,
        TO_TIMESTAMP(START_TIME_CENTRAL, 'YYYY MON DD HH12:MI:SS PM') AS TRIP_START_TS,
        TO_TIMESTAMP(END_TIME_CENTRAL,   'YYYY MON DD HH12:MI:SS PM') AS TRIP_END_TS,
        CAST(COUNCIL_DISTRICT_START AS INTEGER)                        AS COUNCIL_DISTRICT_START
    FROM MICROMOBILITY_DB.BRONZE.TRIPS_RAW
    WHERE REPLACE(YEAR, ',', '') IN ('2021', '2022')
      AND LOWER(VEHICLE_TYPE) = 'scooter'
      AND START_TIME_CENTRAL IS NOT NULL
      AND END_TIME_CENTRAL IS NOT NULL
      AND COUNCIL_DISTRICT_START IS NOT NULL
      AND COUNCIL_DISTRICT_START != 'None'
      AND COUNCIL_DISTRICT_START != '0'
      AND CAST(REPLACE(TRIP_DURATION, ',', '') AS INTEGER) >= 60
      AND CAST(REPLACE(TRIP_DURATION, ',', '') AS INTEGER) <= 7200
      AND CAST(REPLACE(TRIP_DISTANCE, ',', '') AS FLOAT)  >= 100
      AND CAST(REPLACE(TRIP_DISTANCE, ',', '') AS FLOAT)  <= 15000
),
enriched AS (
    SELECT
        b.TRIP_ID,
        b.VEHICLE_TYPE,
        b.TRIP_YEAR,
        b.TRIP_DURATION_SEC,
        b.TRIP_DISTANCE_M,
        b.TRIP_START_TS,
        b.TRIP_END_TS,
        b.COUNCIL_DISTRICT_START,
        CAST(b.TRIP_START_TS AS DATE)                                  AS TRIP_DATE,
        EXTRACT(MONTH FROM b.TRIP_START_TS)                            AS TRIP_MONTH,
        EXTRACT(HOUR FROM b.TRIP_START_TS)                             AS TRIP_HOUR,
        DAYOFWEEK(b.TRIP_START_TS)                                     AS DAY_OF_WEEK_NUM,
        DAYNAME(b.TRIP_START_TS)                                       AS DAY_OF_WEEK_NAME,
        CASE
            WHEN DAYOFWEEK(b.TRIP_START_TS) IN (1, 7)
            THEN 'weekend' ELSE 'weekday'
        END                                                            AS DAY_TYPE,
        p.UNLOCK_FEE,
        p.PER_MINUTE_RATE,
        ROUND(
            p.UNLOCK_FEE + (b.TRIP_DURATION_SEC / 60.0 * p.PER_MINUTE_RATE),
        2)                                                             AS ESTIMATED_REVENUE,
        d.AREA_CLASSIFICATION                                          AS ZONE_CLASS,
        d.DISTRICT_NAME,
        t.TIME_SEGMENT,
        t.IS_PEAK_HOUR,
        ROUND(b.TRIP_DURATION_SEC / 60.0, 2)                          AS TRIP_DURATION_MIN,
        ROUND(
            (p.UNLOCK_FEE + (b.TRIP_DURATION_SEC / 60.0 * p.PER_MINUTE_RATE))
            / (b.TRIP_DURATION_SEC / 60.0),
        4)                                                             AS REVENUE_PER_MIN
    FROM base b
    LEFT JOIN MICROMOBILITY_DB.BRONZE.PRICING_REF p
        ON LOWER(b.VEHICLE_TYPE) = LOWER(p.VEHICLE_TYPE)
    LEFT JOIN MICROMOBILITY_DB.BRONZE.DISTRICT_REF d
        ON b.COUNCIL_DISTRICT_START = d.DISTRICT_ID
    LEFT JOIN MICROMOBILITY_DB.BRONZE.TIME_SEGMENT_REF t
        ON EXTRACT(HOUR FROM b.TRIP_START_TS) = t.HOUR
    WHERE DATEDIFF('second', b.TRIP_START_TS, b.TRIP_END_TS) > 0
)
SELECT * FROM enriched;


