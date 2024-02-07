from bs4 import BeautifulSoup
import sqlite3
import pandas as pd
import numpy as np
import requests
from datetime import datetime

url = "https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks"
exchange_rate = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"
db_name = "Banks.db"
table_name = "Largest_banks"
output_csv_path = "./Largest_banks_data.csv"
table_attribs = ["Name", "MC_USD_Billion"]

def log_progress(message):
    timestamp_format = "%Y-%h-%d-%H-%M-%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open('log_text.txt', "a") as f:
        f.write(timestamp + " " + message + "\n")

def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page, "html.parser")
    df = pd.DataFrame(columns=table_attribs)

    tables = data.find_all("tbody")
    rows = tables[0].find_all("tr")

    for row in rows:
        col = row.find_all("td")
        if len(col) != 0:
            bank_name = col[1].find_all('a')[1].text
            market_cap = float(col[2].contents[0][:-1])
            data_dict = {"Name": bank_name, "MC_USD_Billion": market_cap}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)
    
    return df

def transform(df, csv_path):

    df_read = pd.read_csv(csv_path)
    dict = df_read.set_index("Currency").to_dict()["Rate"]
    
    try:
        df["MC_EUR_Billion"] = [np.round(x*dict["EUR"],2) for x in df["MC_USD_Billion"]]
        df["MC_GBP_Billion"] = [np.round(x*dict["GBP"],2) for x in df["MC_USD_Billion"]]
        df["MC_INR_Billion"] = [np.round(x*dict["INR"],2) for x in df["MC_USD_Billion"]]
    
    except KeyError:
        print(f"Incorrect key looped")
    
    
    return df

def load_to_csv(df, output_path):
    return df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    return df.to_sql(table_name, sql_connection, if_exists="replace", index=False)

def run_queries(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


log_progress("ETL process initiated")
log_progress("Preparing for extraction of data")
log_progress("Data extraction phase started")
df = extract(url, table_attribs)
log_progress("Data extraction phase finished")
log_progress("Data transformation phase initiated")
df = transform(df, exchange_rate)
log_progress("Data transformation phase finished")
log_progress("Loading transformed data to CSV")
load_to_csv(df, output_csv_path)
log_progress("Data loaded to CSV succesfully")
log_progress(f"Data loading to DB {db_name} initiated")
sql_connection = sqlite3.connect(db_name)
load_to_db(df, sql_connection, table_name)
log_progress(f"Data loaded succesfully to {db_name} database")
run_queries(f"SELECT * FROM {table_name}", sql_connection)
run_queries(f"SELECT AVG(MC_USD_Billion) FROM {table_name}", sql_connection)
run_queries(f"SELECT Name FROM {table_name} LIMIT 5", sql_connection)
log_progress("Queries ran succesfully")
log_progress("ETL process finished!")




