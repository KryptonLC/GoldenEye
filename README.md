# Status update
Notes. Relevant only for the developer 🤷‍♂️

## 18/07/2024 ##

**New functions - landing**
- [+] get_lunar_data
- [] save_lunar_data. Lunar List must be joined with existing public.symbols table and checked only for changes. Overwriting will dump meta data (last update, flags, etc.)
- [+] read_lunar_data



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
