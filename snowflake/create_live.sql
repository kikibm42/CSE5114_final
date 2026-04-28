-- Builds table in snowflake for live_data to be stored 

USE DATABASE DOLPHIN_DB;
USE SCHEMA PUBLIC;

CREATE TABLE IF NOT EXISTS LIVE_DATA (
    game_id        VARCHAR,
    h_team         VARCHAR,
    a_team         VARCHAR,
    h_points       INTEGER,
    a_points       INTEGER,
    quarter        INTEGER,
    clock          VARCHAR,
    game_status    VARCHAR,
    processed_time TIMESTAMP_NTZ
);

SHOW TABLES LIKE 'LIVE_DATA';

SELECT * FROM LIVE_DATA LIMIT 5;