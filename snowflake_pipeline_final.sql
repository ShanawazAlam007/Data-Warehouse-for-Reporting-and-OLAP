-- ==============================================================================
-- SNOWFLAKE DATA PIPELINE: YELP & WEATHER DATA
-- ==============================================================================

-- ==========================================
-- PHASE 2: STAGING LAYER
-- ==========================================
CREATE SCHEMA IF NOT EXISTS YELP_WEATHER_DB.STAGING;
USE SCHEMA YELP_WEATHER_DB.STAGING;

-- CREATE OR REPLACE FILE FORMAT definitions
CREATE OR REPLACE FILE FORMAT CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL');

CREATE OR REPLACE FILE FORMAT JSON_FORMAT
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE;

-- CREATE TABLE statements for all 8 staging tables
CREATE OR REPLACE TABLE STG_BUSINESS (src VARIANT);
CREATE OR REPLACE TABLE STG_CHECKIN (src VARIANT);
CREATE OR REPLACE TABLE STG_COVID (src VARIANT);
CREATE OR REPLACE TABLE STG_REVIEW (src VARIANT);
CREATE OR REPLACE TABLE STG_TIP (src VARIANT);
CREATE OR REPLACE TABLE STG_USER (src VARIANT);

CREATE OR REPLACE TABLE STG_PRECIPITATION (
    date VARCHAR,
    precipitation VARCHAR,
    precipitation_normal VARCHAR
);

CREATE OR REPLACE TABLE STG_TEMPERATURE (
    date VARCHAR,
    min FLOAT,
    max FLOAT,
    normal_min FLOAT,
    normal_max FLOAT
);

-- 8 explicit COPY INTO statements to load files from the internal stage
COPY INTO STG_BUSINESS FROM @YELP_STAGE/yelp_academic_dataset_business.json FILE_FORMAT = (FORMAT_NAME = JSON_FORMAT);
COPY INTO STG_CHECKIN FROM @YELP_STAGE/yelp_academic_dataset_checkin.json FILE_FORMAT = (FORMAT_NAME = JSON_FORMAT);
COPY INTO STG_COVID FROM @YELP_STAGE/yelp_academic_dataset_covid_features.json FILE_FORMAT = (FORMAT_NAME = JSON_FORMAT);
COPY INTO STG_REVIEW FROM @YELP_STAGE/yelp_academic_dataset_review.json FILE_FORMAT = (FORMAT_NAME = JSON_FORMAT);
COPY INTO STG_TIP FROM @YELP_STAGE/yelp_academic_dataset_tip.json FILE_FORMAT = (FORMAT_NAME = JSON_FORMAT);
COPY INTO STG_USER FROM @YELP_STAGE/yelp_academic_dataset_user.json FILE_FORMAT = (FORMAT_NAME = JSON_FORMAT);
COPY INTO STG_PRECIPITATION FROM @YELP_STAGE/precipitation-inch.csv FILE_FORMAT = (FORMAT_NAME = CSV_FORMAT);
COPY INTO STG_TEMPERATURE FROM @YELP_STAGE/temperature-degreef.csv FILE_FORMAT = (FORMAT_NAME = CSV_FORMAT);


-- ==========================================
-- PHASE 3: ODS LAYER (TRANSFORMATION & FLATTENING)
-- ==========================================
CREATE SCHEMA IF NOT EXISTS YELP_WEATHER_DB.ODS;
USE SCHEMA YELP_WEATHER_DB.ODS;

-- CREATE TABLE statements for all 8 structured ODS tables
CREATE OR REPLACE TABLE ODS_BUSINESS (
    business_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    address VARCHAR,
    city VARCHAR,
    state VARCHAR,
    postal_code VARCHAR,
    latitude FLOAT,
    longitude FLOAT,
    stars FLOAT,
    review_count INTEGER,
    is_open INTEGER,
    categories VARCHAR
);

CREATE OR REPLACE TABLE ODS_CHECKIN (
    business_id VARCHAR,
    checkin_date TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE ODS_COVID (
    business_id VARCHAR,
    highlights VARCHAR,
    delivery_or_takeout VARCHAR,
    grubhub_enabled VARCHAR,
    call_to_action_enabled VARCHAR,
    request_a_quote_enabled VARCHAR,
    covid_banner VARCHAR,
    temporary_closed_until VARCHAR,
    virtual_services_offered VARCHAR
);

CREATE OR REPLACE TABLE ODS_REVIEW (
    review_id VARCHAR PRIMARY KEY,
    user_id VARCHAR,
    business_id VARCHAR,
    stars FLOAT,
    useful INTEGER,
    funny INTEGER,
    cool INTEGER,
    text VARCHAR,
    date TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE ODS_TIP (
    user_id VARCHAR,
    business_id VARCHAR,
    text VARCHAR,
    date TIMESTAMP_NTZ,
    compliment_count INTEGER
);

CREATE OR REPLACE TABLE ODS_USER (
    user_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    review_count INTEGER,
    yelping_since TIMESTAMP_NTZ,
    useful INTEGER,
    funny INTEGER,
    cool INTEGER,
    fans INTEGER,
    average_stars FLOAT
);

CREATE OR REPLACE TABLE ODS_PRECIPITATION (
    date DATE PRIMARY KEY,
    precipitation FLOAT,
    precipitation_normal FLOAT
);

CREATE OR REPLACE TABLE ODS_TEMPERATURE (
    date DATE PRIMARY KEY,
    min_temp FLOAT,
    max_temp FLOAT,
    normal_min FLOAT,
    normal_max FLOAT
);

-- Ingestion and JSON flattening execution blocks
INSERT INTO ODS_BUSINESS
SELECT 
    src:business_id::VARCHAR,
    src:name::VARCHAR,
    src:address::VARCHAR,
    src:city::VARCHAR,
    src:state::VARCHAR,
    src:postal_code::VARCHAR,
    src:latitude::FLOAT,
    src:longitude::FLOAT,
    src:stars::FLOAT,
    src:review_count::INTEGER,
    src:is_open::INTEGER,
    src:categories::VARCHAR
FROM YELP_WEATHER_DB.STAGING.STG_BUSINESS;

INSERT INTO ODS_CHECKIN
SELECT 
    src:business_id::VARCHAR,
    f.value::TIMESTAMP_NTZ
FROM YELP_WEATHER_DB.STAGING.STG_CHECKIN,
LATERAL FLATTEN(INPUT => SPLIT(src:date, ', ')) f;

INSERT INTO ODS_COVID
SELECT 
    src:business_id::VARCHAR,
    src:highlights::VARCHAR,
    src:"delivery or takeout"::VARCHAR,
    src:"Grubhub enabled"::VARCHAR,
    src:"Call To Action enabled"::VARCHAR,
    src:"Request a Quote Enabled"::VARCHAR,
    src:"Covid Banner"::VARCHAR,
    src:"Temporary Closed Until"::VARCHAR,
    src:"Virtual Services Offered"::VARCHAR
FROM YELP_WEATHER_DB.STAGING.STG_COVID;

INSERT INTO ODS_REVIEW
SELECT 
    src:review_id::VARCHAR,
    src:user_id::VARCHAR,
    src:business_id::VARCHAR,
    src:stars::FLOAT,
    src:useful::INTEGER,
    src:funny::INTEGER,
    src:cool::INTEGER,
    src:text::VARCHAR,
    src:date::TIMESTAMP_NTZ
FROM YELP_WEATHER_DB.STAGING.STG_REVIEW;

INSERT INTO ODS_TIP
SELECT 
    src:user_id::VARCHAR,
    src:business_id::VARCHAR,
    src:text::VARCHAR,
    src:date::TIMESTAMP_NTZ,
    src:compliment_count::INTEGER
FROM YELP_WEATHER_DB.STAGING.STG_TIP;

INSERT INTO ODS_USER
SELECT 
    src:user_id::VARCHAR,
    src:name::VARCHAR,
    src:review_count::INTEGER,
    src:yelping_since::TIMESTAMP_NTZ,
    src:useful::INTEGER,
    src:funny::INTEGER,
    src:cool::INTEGER,
    src:fans::INTEGER,
    src:average_stars::FLOAT
FROM YELP_WEATHER_DB.STAGING.STG_USER;

-- Aligned strict format date parsing for climate records
INSERT INTO ODS_TEMPERATURE
SELECT DISTINCT
     TO_DATE(date, 'YYYYMMDD'),
     min,
     max,
     normal_min,
     normal_max
FROM YELP_WEATHER_DB.STAGING.STG_TEMPERATURE
WHERE date IS NOT NULL;

INSERT INTO ODS_PRECIPITATION
SELECT DISTINCT
     TO_DATE(date, 'YYYYMMDD'),
     TRY_CAST(precipitation AS FLOAT),
     TRY_CAST(precipitation_normal AS FLOAT)
FROM YELP_WEATHER_DB.STAGING.STG_PRECIPITATION
WHERE date IS NOT NULL;


-- ==========================================
-- PHASE 4: DATA WAREHOUSE LAYER (STAR SCHEMA)
-- ==========================================
CREATE SCHEMA IF NOT EXISTS YELP_WEATHER_DB.DWH;
USE SCHEMA YELP_WEATHER_DB.DWH;

-- Generate analytical dimensions
CREATE OR REPLACE TABLE DIM_BUSINESS AS
SELECT business_id, name, address, city, state, postal_code, latitude, longitude, stars, review_count, is_open, categories
FROM YELP_WEATHER_DB.ODS.ODS_BUSINESS;

CREATE OR REPLACE TABLE DIM_USER AS
SELECT user_id, name, review_count, yelping_since, average_stars
FROM YELP_WEATHER_DB.ODS.ODS_USER;

CREATE OR REPLACE TABLE DIM_TEMPERATURE AS
SELECT date AS date_key, min_temp, max_temp, normal_min, normal_max
FROM YELP_WEATHER_DB.ODS.ODS_TEMPERATURE;

CREATE OR REPLACE TABLE DIM_PRECIPITATION AS
SELECT date AS date_key, precipitation, precipitation_normal
FROM YELP_WEATHER_DB.ODS.ODS_PRECIPITATION;

-- Generate connected Fact Table matching on clean date keys
CREATE OR REPLACE TABLE FACT_REVIEW AS
SELECT 
    r.review_id,
    r.user_id,
    r.business_id,
    r.stars AS review_stars,
    r.date AS review_timestamp,
    CAST(r.date AS DATE) AS review_date,
    t.min_temp,
    t.max_temp,
    p.precipitation
FROM YELP_WEATHER_DB.ODS.ODS_REVIEW r
LEFT JOIN DIM_TEMPERATURE t ON CAST(r.date AS DATE) = t.date_key
LEFT JOIN DIM_PRECIPITATION p ON CAST(r.date AS DATE) = p.date_key;


-- ==========================================
-- FINAL OLAP EVALUATION QUERY
-- ==========================================
SELECT 
    b.name AS business_name,
    f.review_date,
    AVG(f.review_stars) AS avg_rating,
    COUNT(f.review_id) AS total_reviews,
    MAX(f.max_temp) AS max_daily_temp,
    SUM(f.precipitation) AS total_daily_precipitation
FROM FACT_REVIEW f
JOIN DIM_BUSINESS b ON f.business_id = b.business_id
WHERE f.max_temp IS NOT NULL
GROUP BY b.name, f.review_date
ORDER BY f.review_date DESC, total_reviews DESC
LIMIT 100;