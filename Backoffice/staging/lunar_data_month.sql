WITH windowed_data AS (
    SELECT
        symbol_id,
        extract(year from datetime) as year,
        extract(month from datetime) as month,

        first_value(open) over (partition by symbol_id, extract(year from datetime), extract(month from datetime) order by datetime asc) as price_open,
        last_value(close) over (partition by symbol_id, extract(year from datetime), extract(month from datetime) order by datetime asc) as price_close,

        last_value(market_cap) over (partition by symbol_id, extract(year from datetime), extract(month from datetime) order by datetime asc) as market_cap,
        last_value(circulating_supply) over (partition by symbol_id, extract(year from datetime), extract(month from datetime) order by datetime asc) as circulating_supply,

        first_value(posts_active) over (partition by symbol_id, extract(year from datetime), extract(month from datetime) order by datetime asc) as posts_active_open,
        last_value(posts_active) over (partition by symbol_id, extract(year from datetime), extract(month from datetime) order by datetime asc) as posts_active_close,

        first_value(posts_created) over (partition by symbol_id, extract(year from datetime), extract(month from datetime) order by datetime asc) as posts_created_open,
        last_value(posts_created) over (partition by symbol_id, extract(year from datetime), extract(month from datetime) order by datetime asc) as posts_created_close,

        first_value(interactions) over (partition by symbol_id, extract(year from datetime), extract(month from datetime) order by datetime asc) as interactions_open,
        last_value(interactions) over (partition by symbol_id, extract(year from datetime), extract(month from datetime) order by datetime asc) as interactions_close,

        high, low, volume_24h, volatility, sentiment, contributors_active, contributors_created,
        posts_active, posts_created, interactions, social_dominance, galaxy_score, alt_rank
    FROM landing.lunar_data
    WHERE symbol_id = 3
)
SELECT 
    symbol_id,
    year,
    month,
    price_open,
    MAX(high) as price_high,
    MIN(low) as price_low,
    price_close,

    SUM(volume_24h) as volume_24h,
    market_cap,
    circulating_supply,
    AVG(volatility) as volatility,

    MIN(sentiment) as sentiment_min,
    MAX(sentiment) as sentiment_max,
    MIN(contributors_active) as contributors_active_min,
    MAX(contributors_created) as contributors_active_max,

    posts_active_open,
    MAX(posts_active) as posts_active_high,
    MIN(posts_active) as posts_active_low,
    posts_active_close,

    posts_created_open,
    MAX(posts_created) as posts_created_high,
    MIN(posts_created) as posts_created_low,
    posts_created_close,

    interactions_open,
    MAX(interactions) as interactions_high,
    MIN(interactions) as interactions_low,
    interactions_close,

    MIN(social_dominance) as social_dominance_min,
    MAX(social_dominance) as social_dominance_max,
    MIN(galaxy_score) as galaxy_score_min,
    MAX(galaxy_score) as galaxy_score_max,
    MIN(alt_rank) as alt_rank_min,
    MAX(alt_rank) as alt_rank_max
FROM windowed_data
GROUP BY 
    symbol_id,
    year,
    month,
    price_open,
    price_close,
    market_cap,
    circulating_supply,
    posts_active_open,
    posts_active_close,
    posts_created_open,
    posts_created_close,
    interactions_open,
    interactions_close;
