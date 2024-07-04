import requests
import pandas as pd
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime
import numpy as np

url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
db_name = "Banks.db"
table_name = "Largest_banks"
rate_csv_path = "FinalPythonProject\exchange_rate.csv"
csv_path = "./Largest_banks_data.csv"
log_file = "code_log.txt"
df = pd.DataFrame(['Name', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion'])
table_attribs = pd.DataFrame(['Name', 'MC_USD_Billion'])
sql_connection = sqlite3.connect(db_name)

def extract(url, table_attribs):
    response = requests.get(url)
    html = response.content
    soup = BeautifulSoup(html,'lxml')
    table = soup.find_all('table')
    table = table[0]
    tds = table.find_all('td')
    title = [td.text.strip(' \n') for td in tds]
    name = []
    for i in [1,4,7,10,13,16,19,22,25,28]:
        name.append(title[i])
    US_billion = []
    for i in [2,5,8,11,14,17,20,23,26,29]:
        US_billion.append(title[i])
    data = []
    j = 0
    for j in range(10):
        row = {'Name': name[j],
              'MC_USD_Billion': US_billion[j]}
        data.append(row)
    table_attribs = pd.DataFrame(data)
    return table_attribs

def transform(df, rate_csv_path):
    rate = pd.read_csv(rate_csv_path)
    exchange_rate = rate.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(float(x)*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(float(x)*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(float(x)*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]
    return df

def load_to_csv(transformed_data, csv_path):
    transformed_data.to_csv(csv_path)

def load_to_db(transformed_data, sql_connection, table_name):
    transformed_data.to_sql(table_name, sql_connection, if_exists = 'replace', index = False)

def run_query(query_statement, sql_connection):
    transformed_data = pd.read_sql(query_statement, sql_connection)
    print(transformed_data)


def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, 'a') as f:
        f.write(timestamp + ' : ' + message + '\n')

log_progress("ETL Job Started")
log_progress("Extract phase Started")
df = extract(url, table_attribs)
log_progress("Extract phase Ended")
log_progress("Transform Job Started")
transformed_data = transform(df, rate_csv_path)
log_progress("Transform Job Ended")
log_progress("Load Job Started")
load_to_csv(transformed_data, csv_path)
log_progress("SQL Connection initiated")
load_to_db(transformed_data, sql_connection, table_name)
log_progress("Load Job Ended")
query_statement = 'SELECT * FROM Largest_banks'
run_query(query_statement, sql_connection)
log_progress("Server Connection closed")
log_progress("ETL Job Ended")