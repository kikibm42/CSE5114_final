USE DATABASE MINNOW_DB;
USE SCHEMA   PUBLIC;
USE WAREHOUSE MINNOW_WH;

-- Row counts for every table
SELECT 'dim_team'                  AS tbl, COUNT(*) AS row_count FROM dim_team
UNION ALL
SELECT 'dim_player',               COUNT(*) FROM dim_player
UNION ALL
SELECT 'dim_date',                 COUNT(*) FROM dim_date
UNION ALL
SELECT 'fact_game',                COUNT(*) FROM fact_game
UNION ALL
SELECT 'fact_game_team_stats',     COUNT(*) FROM fact_game_team_stats
UNION ALL
SELECT 'fact_game_player_stats',   COUNT(*) FROM fact_game_player_stats
ORDER BY tbl;

-- Sample rows from each table
SELECT * FROM dim_team                LIMIT 5;
SELECT * FROM dim_player              LIMIT 5;
SELECT * FROM dim_date                LIMIT 5;
SELECT * FROM fact_game               LIMIT 5;
SELECT * FROM fact_game_team_stats    LIMIT 5;
SELECT * FROM fact_game_player_stats  LIMIT 5;
-- SELECT * FROM LIVE_DATA LIMIT 5;  -- uncomment once live pipeline is running

-- ── Diagnostics ────────────────────────────────────────────────

-- 1. Check Bulls team_id
SELECT team_id, name, abbreviation FROM dim_team WHERE abbreviation = 'CHI';

-- 2. For that team_id, how many games as home vs away?
-- Replace 1610612741 with the actual team_id from query 1
SELECT
    SUM(CASE WHEN home_team_id = 1610612741 THEN 1 ELSE 0 END) AS home_games,
    SUM(CASE WHEN away_team_id = 1610612741 THEN 1 ELSE 0 END) AS away_games
FROM fact_game;

-- 3. Sample fact_game rows for Bulls to verify scores look real
SELECT fg.game_id, fg.date, ht.abbreviation AS home, fg.home_score,
       at.abbreviation AS away, fg.away_score
FROM fact_game fg
JOIN dim_team ht ON fg.home_team_id = ht.team_id
JOIN dim_team at ON fg.away_team_id = at.team_id
WHERE fg.home_team_id = 1610612741 OR fg.away_team_id = 1610612741
ORDER BY fg.date DESC
LIMIT 10;

-- 4. Check dim_date season_type values that exist
SELECT season_type, COUNT(*) FROM dim_date GROUP BY season_type;
