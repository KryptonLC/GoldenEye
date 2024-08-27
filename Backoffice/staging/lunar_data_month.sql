/*
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


;
*/

with intervals as (
	SELECT 
	    start,
	    (start + interval '1 hour' - interval '1 second') AS end
	FROM generate_series(
	    DATE_TRUNC('hour', NOW()) - interval '24 hours', 
	    DATE_TRUNC('hour', NOW()), 
	    interval '1 hour'
	) AS start
)

	
select distinct
	a.start
	,a.end
    ,extract(year from a.end) as year
    ,extract(month from a.end) as month
	,extract(day from a.end) as day
	,b.symbol_id
	
	,first_value(b.open) over w as open
	,max(b.high) over w as high
	,min(b.low) over w as low
	,last_value(b.close) over w as close
	,avg(volume_24h) over w as volume_24h
	,avg(market_cap) over w as market_cap
	,avg(circulating_supply) over w as circulating_supply
	,avg(volatility) over w as volatility

	,avg(sentiment) over w as sentiment

	,first_value(contributors_active) over w as contributors_active_open
	,max(contributors_active) over w as contributors_active_high
	,min(contributors_active) over w as contributors_active_low
	,last_value(contributors_active) over w as contributors_active_close

	,first_value(contributors_created) over w as contributors_created_open
	,max(contributors_created) over w as contributors_created_high
	,min(contributors_created) over w as contributors_created_low
	,last_value(contributors_created) over w as contributors_created_close

	,first_value(posts_active) over w as posts_active_open
	,max(posts_active) over w as posts_active_high
	,min(posts_active) over w as posts_active_low
	,last_value(posts_active) over w as posts_active_close

	,first_value(posts_created) over w as posts_created_open
	,max(posts_created) over w as posts_created_high
	,min(posts_created) over w as posts_created_low
	,last_value(posts_created) over w as posts_created_close

	,first_value(interactions) over w as interactions_open
	,max(interactions) over w as interactions_high
	,min(interactions) over w as interactions_low
	,last_value(interactions) over w as interactions_close

	,first_value(social_dominance) over w as social_dominance_open
	,max(social_dominance) over w as social_dominance_high
	,min(social_dominance) over w as social_dominance_low
	,last_value(social_dominance) over w as social_dominance_close

	,avg(galaxy_score) over w as galaxy_score
	,avg(alt_rank) over w as alt_rank
	
from intervals a
inner join landing.lunar_data b on
	b.datetime >= a.start 
	and b.datetime < a.end
where b.symbol_id between 1 and 10
	
window w as (
	partition by b.symbol_id, a.start 
	order by b.symbol_id,b.datetime asc 
	rows between unbounded preceding and unbounded following
	)
order by b.symbol_id,a.start