# DE_Final_Project_2

Анализ рынка валют

Общая задача: создать ETL-процесс формирования витрин данных для анализа изменений курса валют.
___
## Стек
  - Загрузка данных - **Python 3.7.15**
  - Хранение слепка данных **Postgres 15**
  - Оркестрация - **Airflow v2.5**
  - Выгрузка данных - **Pandas 1.3.5**

  ___
## Инструкция

1. Зарегистрируйте api на https://www.alphavantage.co/support/#api-key

2. Скопируйте выданный ключ в файл variables.py, переменную apikey

```python
apikey = "copy_apikey_here"
tickers = ["TSLA", "IBM", "NVDA", "AMD", "INTC"]
```
    При необходимости в этом файле можно добавить/поменять/убрать тикеры компаний. Но имейте в виду при бесплатном доступе API разрешает до 5 запросов в минуту. На каждый тикер проходит по 2 запроса (Один для выгрузки основных данных, второй для выгрузки названия), так что при большом количестве тикеров придётся подождать.

3. В командной строке пропишите  `docker compose up` , года запустится web сервер airflow можно будет зайти на него по адресу http://localhost:8080/ по умолчанию логин/пароль: airflow/airflow


4. Во вкладке DAGs находим load_stocks. После его запуска если задания отработали успешно, полученные данные запишутся в файл output.csv, он находится по пути `airflow/dags/output/output.csv` .
Проверить наличие даных в этом файле можно запуском скрипта  ` python3 check.py` .

___

### Описание полей таблицы:

  - **ticker** - Тикер краткое название биржевых инструментов
  - **name** - Полное название компании
  - **sum_volume** - Суммарный объём торгов за последние сутки
  - **open** - Цена открытия
  - **close** - Цена закрытия
  - **Diff_price** - Разница между ценой открытия и закрытия в %
  - **max_volume** - Максимальный объём торгов за сутки в интервале = 1мин
  - **time_max_volume** - Время начала интервала максимального объёма торгов
  - **max_price** - Максимальная цена за сутки в интервале = 1мин
  - **time_max_price** - Время начала интервала максимальной цены
  - **low_price** - Минимальная цена за сутки в интервале = 1мин
  - **time_low_price** - Время начала интервала минимальной цены


  Подкрутить настройки airflow можно в файле airflow.cfg