import pandas as pd

df = pd.read_csv("./airflow/dags/output/output.csv")
print(df)