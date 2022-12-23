import sqlalchemy as sa
from functions import *
import pandas as pd


tickers = ["TSLA","IBM","NVDA"] #Тикер акции
apikey = open("api")#В файле "api" должен лежать ваш ключ от API
engine = sa.create_engine("postgresql+psycopg2://admin:admin@localhost/final_project")

for ticker in tickers:
    #Проверяем существование таблицы
    insp = sa.inspect(engine)
    if (insp.has_table('stocks')):
        #Ищем строки по тикеру в нашей бд
        ticker_search= pd.read_sql(f'''SELECT * FROM stocks WHERE "ticker" = '{ticker}' ''', engine)
        #Если строк больше 0, 
        #вытаскиваем последний элемент из нашей бд, 
        #ищем в загруженном слепке с API этот элемент
        #и загружаем все данные начиная со следующей строки в базу данных
        if (len(ticker_search.index)>0):
            last_line = ticker_search["datetime"].iloc[-1]
            data = collect_data(ticker)
            entry_point = data[data["datetime"]==str(last_line)].index[0]
            slicer = data.iloc[entry_point:-1]
            load_to_sql(slicer, engine)
            
        else:
        #Если строк по этому тикеру нет - загружаем все данные. 
            load_to_sql(collect_data(ticker), engine)
        
        
            
    else:
        print("Таблицы не существует")
        load_to_sql(collect_data(ticker), engine)
 

