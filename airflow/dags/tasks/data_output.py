import pandas as pd
import sqlalchemy as sa

#Подключение бд и запись в DataFrame
engine = sa.create_engine("postgresql+psycopg2://admin:admin@host.docker.internal:5430/final_project")
df = pd.read_sql("""SELECT * FROM stocks""", engine)

#Находим последнюю строку в столбце datetime, разделяем её на дату и время
max_datetime = df["datetime"].tail(1).to_string(index = False).split()

# DataFrame за последний день найденный в бд
df_last_date = df[(df['datetime'] >= max_datetime[0])]
df_last_date = df_last_date.set_index("index")


#Разделение на элементы витрины.
open_price = df_last_date[["ticker","name","open","datetime"]].groupby(by=["ticker"]).first()

close_price = df_last_date[["ticker","close","datetime"]].groupby(by=["ticker"]).last()

sum_volume = df_last_date.groupby(by=["ticker"])["volume"].sum()

max_volume = df_last_date[["ticker","volume","datetime"]].sort_values("volume", ascending = False).groupby("ticker").first()
max_volume["datetime"]=max_volume["datetime"].dt.time

max_price = df_last_date[["ticker", "high", "datetime"]].sort_values("high", ascending = False).groupby("ticker").first()
max_price["datetime"]=max_price["datetime"].dt.time

min_price = df_last_date[["ticker", "low", "datetime"]].sort_values("low").groupby("ticker").first()


#Сборка витрины.
showcase = pd.concat([
    open_price["name"],
    sum_volume.rename("sum_volume"), 
    open_price["open"], 
    close_price["close"],
    ((((close_price["close"]/open_price["open"])-1)*100).round(2).astype(str) + '%').rename("Diff_price"),
    max_volume["volume"].rename("max_volume"),
    max_volume["datetime"].rename("time_max_volume"),
    max_price["high"].rename("max_price"),
    max_price["datetime"].rename("time_max_price"),
    min_price["low"].rename("low_price"),
    min_price["datetime"].rename("time_low_price"),
], axis=1)
with open("/opt/airflow/dags/output/output.csv", "w") as file:
    showcase.to_csv(file)
    
    print("запись прошла успешно")
