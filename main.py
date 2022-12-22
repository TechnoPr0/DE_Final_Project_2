import sqlalchemy as sa
import functions as func
import pandas as pd




ticker = "TSLA" #Тикер акции
apikey = open("api")#В файле "api" должен лежать ваш ключ от API
engine = sa.create_engine("postgresql+psycopg2://admin:admin@localhost/final_project")


insp = sa.inspect(engine)
if (insp.has_table('stocks')):
    ticker_search= pd.read_sql(f'''SELECT * FROM stocks WHERE "ticker" = '{ticker}' ''', engine)
    if (len(ticker_search.index)>0):
        print (ticker_search["datetime"])
    else:
        func.db_formation(ticker, engine, load_to_db = True)
        
         
else:
    print("Таблицы не существует")
    func.db_formation(ticker, engine, load_to_db = True) 
 

