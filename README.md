# Status update
Notes. Relevant only for the developer 🤷‍♂️

## 27/08/2024 ##

* symbol_id list shortened to 80
* ETL on public.symbols include_etl = TRUE
* landing.lunar_data_etl -> goes to Pandas -> Dash. Refresh MV with 
```sql
refresh materialized view landing.lunar_data_etl
```

## 26/08/2024 ##

* Nie jest potrzebny OHLC w ogóle. Agregaty są predefiniowane w views. Dane dla pojedynczego symbol_id przetwarzane do OHLC przez Pandas. 
* [] Aggregates - last 24h - Top Gainers: price, posts, interactions

## 23/08/2024 ##

* Added save_code.py for providind Markdown file for chat

## 21/08/2024 ##

* Staging zaczety, ale nie dziala, jak powinien. OHLC można zrobić w Pythonie, ale to wymaga dużego data transfer. Lepiej postarać się o staging w PostgreSQL. 
* Sprawdzić czas działania obu metod. 
* OHLC dla social



## 20/08/2024 ##

* [+] landing_process
* [+] lunar_data - DF table partitioned
* [+] add API call counter
* [+] build main loop for continuous data loading 
* [+] dump_lunar_buffer

* API call handlers stored in utils.py
* new view: public.key_usage, new table: public.api_request_logs

## 19/08/2024 ##

[+] get_lunar_data<br>
[+] save_lunar_data<br>
[+] buffer_symbol_data - DF table<br>



* [IMPORTANT] Dane zapisywane przez save_lunar_data do buforowej tabeli. Bufor regularnie zrzucany do jednej, partycjonowanej tabeli. Dump trigerowany przez Staging. 

* [IMPORTANT] Initial data load co 2 lata w loop od 2018 do dzisiaj
```py
start_time = "01.01.2018"
end_time = "31.12.2020"
result, data_df = get_lunar_data(3, start_time, end_time)
``` 

## 18/08/2024 ##

**New functions - landing**
- [+] get_lunar_data
- [+] save_lunar_data. Lunar List must be joined with existing public.symbols table and checked only for changes. Overwriting will dump meta data (last update, flags, etc.)
- [+] read_lunar_data
- [+] put *_lunar_symbols funcs into one file


[] list error codes
9000-9999 - landing


[???] how to keep track on request limits? DB? global variable?

[] API Key shuold be handeled with a function. For now it's a direct read from config
```py
key_code = lunar_key["key_outlook"]["code"]
```

[+] Code clean-up<br>
[+] status do README.md<br>
[+] update gitignore



## 13/08/2024 ##
[] landing skracamy do listy coinów dostępnych na Binance i KuCoin. Pobierz Lunar. Pobierz Binance. Inner join. Flaga [] w [symbols]

**ETL**
* landing - get data from lunar & exchanges
* staging - transform, aggregate, rank
* loading - prepare for call from the website

**Naming convention**

read_* - select data from database<br>
get_* - receive data from external API<br>
save_* - save into DB<br>

[IMPORTANT] logging tylko z głównej aplikacji. Funkcje nie printują i nie loggują niczego

[+] update wsadu dla GPT


## 28/07/2024 ##

- GitHub Repository: GoldenEye-v2
```shell
git remote -v
```
