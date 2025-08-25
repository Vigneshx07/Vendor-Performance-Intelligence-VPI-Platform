import os
import pandas as pd
import sqlite3
import logging
import time

logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

conn = sqlite3.connect("inventory.db")


def ingest_db(df, table_name, conn):
    '''this function will ingest the dataframe into database table'''
    df.to_sql(table_name, con=conn, if_exists='replace', index=False)
    print(f"Inserted {df.shape[0]} rows into '{table_name}'")

def load_raw_data():
    '''this function will load the CSVs as dataframe and ingest into db'''
    start = time.time()
    folder_path = "data"
    for file in os.listdir(folder_path):
        if file.endswith(".csv"):
            df = pd.read_csv(os.path.join(folder_path, file))
            logging.info(f'Ingesting {file} in db')
            ingest_db(df, file[:-4], conn)
    end = time.time()
    total_time = (end - start)/60
    logging.info('---------Ingestion Complete---------')
    
    logging.info(f'\nTotal Time Taken: {total_time} minutes')
    conn.close()
    
if __name__ == '__main__':
    load_raw_data()