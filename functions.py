import pandas as pd
import requests
import time
from sqlalchemy import Integer, String, DateTime, Float, inspect

def collect_data(ticker):
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
    try:
        meta = full_data["Meta Data"]
    except KeyError:
        print("Достигнуто ограничение по количеству запросов. Ждём 1мин, а затем продолжим.")
        time.sleep(60)
        req = requests.get(f"https://www.alphavantage.co/query?"+
        f"function=TIME_SERIES_INTRADAY&"+
        f"outputsize=compact&"+
        f"symbol={ticker}&"+
        f"interval={interval}&"+
        f"apikey={apikey}")
        full_data = req.json()
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
    
    index = 0
    final_data = {}
    print(meta["2. Symbol"])
    
    for i in deals.items():
        final_data[index] = {
        "datetime":i[0], 
        "ticker":meta["2. Symbol"], 
        "name":search_json["bestMatches"][0]["2. name"],
        "open":i[1]["1. open"], 
        "high":i[1]["2. high"], 
        "low":i[1]["3. low"], 
        "close":i[1]["4. close"], 
        "volume":i[1]["5. volume"],
        }
        index+=1
    df = pd.DataFrame(final_data)
    df = df.T
    df = df.sort_index()
    return df

def load_to_sql(df, engine):
    
    #В этом блоке мы проверяем наличие таблицы, считаем количество строк,
    #Генерируем список последоовательных чисел начиная с последней строки +1
    #Всё это для добавления index в таблицу
    index = []
    insp = inspect(engine)
    if (insp.has_table('stocks')):
        last_id = pd.read_sql(''' SELECT COUNT(*) FROM stocks ''', engine)
        last_id = last_id.values[0][0]
    else:
        last_id = 0
    for i in range(last_id+1, last_id+len(df)+1):
        index.append(i)
    df = df.set_index([pd.Index(index)])
    
    df.to_sql("stocks", 
    engine, 
    index=True, 
    if_exists="append", 
    dtype={
        "index": Integer,
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