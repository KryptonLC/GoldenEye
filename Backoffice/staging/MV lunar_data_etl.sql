CREATE MATERIALIZED VIEW landing.lunar_data_etl AS
WITH include_etl AS 
(
    SELECT id, name, symbol, last_update
    FROM public.symbols 
    WHERE include_etl = TRUE
)
SELECT 
    b.symbol,
    a.* 
FROM 
    landing.lunar_data a 
INNER JOIN 
    include_etl b 
ON 
    a.symbol_id = b.id
    AND a.datetime >= b.last_update - INTERVAL '72 hours'
ORDER BY 
    a.symbol_id ASC, 
    a.datetime ASC;


select * from landing.lunar_data_etl
refresh materialized view landing.lunar_data_etl