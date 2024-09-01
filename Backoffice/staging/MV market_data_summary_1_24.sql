DROP MATERIALIZED VIEW IF EXISTS landing.market_data_summary_1_24;
CREATE MATERIALIZED VIEW landing.market_data_summary_1_24 as
WITH TargetTimestamps AS (
    SELECT
        s.id AS symbol_id,
		s.symbol as symbol_ticker,
		s.name as symbol_name,
        s.last_timestamp,
        s.last_timestamp - 3600 AS timestamp_1h_before,    -- Subtract 1 hour in seconds (3600 seconds)
        s.last_timestamp - 86400 AS timestamp_24h_before   -- Subtract 24 hours in seconds (86400 seconds)
    FROM
        public.symbols s
)
SELECT
    ld.symbol_id,
	ts.symbol_ticker,
	ts.symbol_name,
    ld.datetime,
    ld.time_unix,
    ld.open,
    ld.high,
    ld.low,
    ld.close,
    ld.volume_24h,
    ld.market_cap,
    ld.circulating_supply,
    ld.sentiment,
    ld.contributors_active,
    ld.contributors_created,
    ld.posts_active,
    ld.posts_created,
    ld.interactions,
    ld.social_dominance,
    ld.galaxy_score,
    ld.volatility,
    ld.alt_rank,
    ld.spam
FROM
    landing.lunar_data ld
JOIN TargetTimestamps ts ON ld.symbol_id = ts.symbol_id
WHERE
    ld.time_unix = ts.last_timestamp
    OR ld.time_unix = ts.timestamp_1h_before
    OR ld.time_unix = ts.timestamp_24h_before
ORDER BY
    ld.symbol_id, ld.datetime DESC;



