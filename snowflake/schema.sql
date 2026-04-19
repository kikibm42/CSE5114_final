-- Snowflake NBA schema
-- Change these variables to retarget a different database/warehouse.
SET nba_db  = 'MINNOW_DB';
SET nba_wh  = 'MINNOW_WH';
SET nba_schema = 'PUBLIC';

USE DATABASE MINNOW_DB;
USE SCHEMA   PUBLIC;
USE WAREHOUSE MINNOW_WH;

-- ============================================================
-- DIMENSION TABLES
-- ============================================================

-- One row per NBA team (static / slowly-changing).
-- conference and division are not present in the raw box-score
-- extraction; seed them via the insert block below or a separate
-- nba_api call (e.g. commonteamroster / leaguestandings).
CREATE TABLE IF NOT EXISTS dim_team (
    team_id      INTEGER      PRIMARY KEY,
    abbreviation VARCHAR(5)   NOT NULL,
    name         VARCHAR(100) NOT NULL,
    city         VARCHAR(100),
    conference   VARCHAR(10),   -- 'East' | 'West'
    division     VARCHAR(20)
);

-- One row per NBA player (slowly-changing).
-- height and weight are NOT available in BoxScoreTraditionalV2;
-- populate them from nba_api.stats.endpoints.commonplayerinfo.
CREATE TABLE IF NOT EXISTS dim_player (
    player_id INTEGER      PRIMARY KEY,
    name      VARCHAR(100) NOT NULL,
    position  VARCHAR(5),
    height    VARCHAR(10),   -- e.g. '6-7'
    weight    INTEGER        -- lbs
);

-- One row per calendar date that has NBA activity.
-- season derived from SEASON_ID (e.g. 22023 → '2023-24').
-- season_type derived from the leading digit of SEASON_ID
--   (1 = Pre-Season, 2 = Regular Season, 4 = Playoffs).
CREATE TABLE IF NOT EXISTS dim_date (
    date        DATE        PRIMARY KEY,
    season      VARCHAR(10),   -- e.g. '2023-24'
    season_type VARCHAR(20)    -- 'Regular Season' | 'Playoffs' | 'Pre-Season'
);

-- ============================================================
-- FACT TABLES
-- ============================================================

-- One row per game.
-- LeagueGameFinder returns one row per team per game; home vs away
-- is determined by the MATCHUP column pattern:
--   "LAL vs. GSW"  → LAL is home
--   "GSW @ LAL"    → GSW is away
-- NOTE: Snowflake does not enforce FK constraints; included for docs.
CREATE TABLE IF NOT EXISTS fact_game (
    game_id      VARCHAR(20)  PRIMARY KEY,
    date         DATE         REFERENCES dim_date(date),
    home_team_id INTEGER      REFERENCES dim_team(team_id),
    away_team_id INTEGER      REFERENCES dim_team(team_id),
    home_score   INTEGER,
    away_score   INTEGER
);

-- One row per team per game (from BoxScoreTraditionalV2 dfs[1]).
-- FG3M/FG3A/FG3_PCT availability may vary across historical seasons;
-- NULLs are expected for older games where 3-point data is absent.
CREATE TABLE IF NOT EXISTS fact_game_team_stats (
    game_id    VARCHAR(20) REFERENCES fact_game(game_id),
    team_id    INTEGER     REFERENCES dim_team(team_id),
    fgm        INTEGER,
    fga        INTEGER,
    fg_pct     FLOAT,
    fg3m       INTEGER,
    fg3a       INTEGER,
    fg3_pct    FLOAT,
    ftm        INTEGER,
    fta        INTEGER,
    ft_pct     FLOAT,
    oreb       INTEGER,
    dreb       INTEGER,
    reb        INTEGER,
    ast        INTEGER,
    stl        INTEGER,
    blk        INTEGER,
    tov        INTEGER,
    pf         INTEGER,
    pts        INTEGER,
    plus_minus INTEGER,
    PRIMARY KEY (game_id, team_id)
);

-- One row per player per game (from BoxScoreTraditionalV2 dfs[0]).
-- min stored as VARCHAR because the source API returns "MM:SS" strings.
CREATE TABLE IF NOT EXISTS fact_game_player_stats (
    game_id    VARCHAR(20) REFERENCES fact_game(game_id),
    player_id  INTEGER     REFERENCES dim_player(player_id),
    team_id    INTEGER     REFERENCES dim_team(team_id),
    min        VARCHAR(10),
    pts        INTEGER,
    fgm        INTEGER,
    fga        INTEGER,
    fg_pct     FLOAT,
    fg3m       INTEGER,
    fg3a       INTEGER,
    fg3_pct    FLOAT,
    ftm        INTEGER,
    fta        INTEGER,
    ft_pct     FLOAT,
    oreb       INTEGER,
    dreb       INTEGER,
    reb        INTEGER,
    ast        INTEGER,
    stl        INTEGER,
    blk        INTEGER,
    tov        INTEGER,
    pf         INTEGER,
    plus_minus INTEGER,
    PRIMARY KEY (game_id, player_id)
);

-- ============================================================
-- LIVE STREAMING TABLE
-- ============================================================

-- Populated by spark_streaming_bones.py in 30-second batch windows.
-- Each row is a snapshot of a live game at processed_time.
CREATE TABLE IF NOT EXISTS LIVE_DATA (
    game_id        VARCHAR(20),
    h_team         VARCHAR(5),
    a_team         VARCHAR(5),
    h_points       INTEGER,
    a_points       INTEGER,
    quarter        INTEGER,
    clock          VARCHAR(10),
    game_status    VARCHAR(50),
    processed_time TIMESTAMP_NTZ
);
