select 
	b.symbol,
	date(datetime) as date,
	TO_CHAR(datetime, 'HH24:MI:SS') AS time,
	a.*
from landing.lunar_data a
left join public.symbols b on a.symbol_id = b.id
where symbol_id in (1,2,3,10)
	and EXTRACT(YEAR FROM datetime) = 2024