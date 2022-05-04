import sqlite3
import pandas as pd
import argparse


def get_args():
    parser = argparse.ArgumentParser(description ='Enter csv file path and database name')
    parser.add_argument('-source','--source', type = str, help ='path to csv file')
    parser.add_argument('-db','--db', type = str, help = 'name of the database you want to import the csv file to')
    return parser.parse_args()

def create_byo_funds_table(csv_file_path):
    c.executescript('''
    DROP TABLE IF EXISTS byo_fund_manager;
              
   CREATE TABLE byo_fund_manager(
       fm_id INTEGER NOT NULL PRIMARY KEY,
       fm_name TEXT NOT NULL  
       );
   ''')
    byo_fund_manager = pd.read_csv(byo_fund_manager_path)
    byo_fund_manager.to_sql('byo_fund_manager', conn, if_exists='replace', index = False)

if __name__ =='__main__':
    args = get_args()
    byo_fund_manager_path = args.source
    database_name = args.db
    conn = sqlite3.connect(database_name)
    c= conn.cursor()
    create_byo_funds_table(byo_fund_manager_path)
    conn.commit()
    if conn:
        conn.close()