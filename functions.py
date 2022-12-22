import pandas as pd
import requests
import psycopg2
from sqlalchemy import Integer, String, DateTime, Float
def db_formation(ticker, engine, load_to_db=False):
    apikey = open("api")#В файле "api" должен лежать ваш ключ от API

    interval = "5min" #Интервал между точками данных во временном ряду. 
    #Поддерживаются следующие значения : 1min, 5min, 15min, 30min,60min
    
    #Тут мы достаём по тикеру из API данные по сделкам
    req = requests.get(f"https://www.alphavantage.co/query?"+
        f"function=TIME_SERIES_INTRADAY&"+
        f"outputsize=compact&"+
        f"symbol={ticker}&"+
        f"interval={interval}&"+
        f"apikey={apikey}")
    full_data = req.json()
    
    #Разделяем словарь на метаданные и сделки
    meta = full_data["Meta Data"]
    deals = full_data["Time Series (5min)"]
    
    #Поиск по тикеру, 
    #позже понадобится для выведения названия акции 
    search = requests.get(f"https://www.alphavantage.co/query?"+
        f"function=SYMBOL_SEARCH&"+
        f"keywords={meta['2. Symbol']}&"+
        f"apikey={apikey}")
    search_json = search.json()

    #Пересобираем в новый словарь с разделением на дату и время;
    #добавляем тикер из метаданных взятых из API;
    #и название взятое из поиска по тикеру;
    #ключём нового словаря теперь будет id,  
    #который просто после каждоко цикла увеличивается на 1
    id = len(deals)
    final_data = {}
    print(meta["2. Symbol"])
    for i in deals.items():
        final_data[id] = {"datetime":i[0], 
        "ticker":meta["2. Symbol"], "name":search_json["bestMatches"][0]["2. name"],
        "open":i[1]["1. open"], "high":i[1]["2. high"], 
        "low":i[1]["3. low"], "close":i[1]["4. close"], 
        "volume":i[1]["5. volume"]}
        id-=1
        
    #Заносим данные в DataFrame
    df = pd.DataFrame(final_data)
    #Для корректного отображения меняем местами строки и столбцы.
    df = df.T
    df = df.sort_index()
    #Если load_to_db True - открываем соединение с PostgreSQL и записываем туда сформированные данные
    if (load_to_db):
        df.to_sql("stocks", 
        engine, 
        index=True, 
        if_exists="replace", 
        dtype={
            "id": Integer,
            "datetime": DateTime,
            "ticker": String,
            "name": String,
            "open": Float,
            "high": Float,
            "low": Float,
            "close": Float,
            "volume": Integer
        })
        print("База загружена успешно")
    else:
        return df