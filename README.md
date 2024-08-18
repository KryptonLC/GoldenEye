# Status update
Notes. Relevant only for the developer 🤷‍♂️

## 18/07/2024 ##

[+] Code clean-up <br>
[+] status do README.md

**New functions - landing**
- [] get_lunar_data
- [] save_lunar_data
- [] read_lunar_data

## 13/08/2024 ##
[] landing skracamy do listy coinów dostępnych na Binance i KuCoin. Pobierz Lunar. Pobierz Binance. Inner join. Flaga [] w [symbols]

**ETL**
* landing - get data from lunar & exchanges
* staging - transform, aggregate, rank
* serving - prepare for call from the website

**Naming convention**

read_* - select data from database<br>
get_* - receive data from external API<br>
save_* - save into DB<br>

[IMPORTANT] logging tylko z głównej aplikacji. Funkcje nie printują i nie loggują niczego.

[+] update wsadu dla GPT


## 28/07/2024 ##

- GitHub Repository: GoldenEye-v2
```shell
git remote -v
```
