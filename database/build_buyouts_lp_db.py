import sqlite3
import pandas as pd
import argparse

def get_args():
    parser = argparse.ArgumentParser(description ='Enter csv file path and database name')
    parser.add_argument('-db','--db', type = str, help = 'name of the database you want to import the csv file to')
    return parser.parse_args()

def drop_table():
    c.executescript('''
    DROP TABLE IF EXISTS byo_lp;
    DROP TABLE IF EXISTS byo_lp_type;
    DROP TABLE IF EXISTS byo_lp_strategy;
    DROP TABLE IF EXISTS byo_lp_sector;
    DROP TABLE IF EXISTS byo_lp_commitment;
    DROP TABLE IF EXISTS byo_lp_office;
    DROP TABLE IF EXISTS byo_lp_contact;
   ''')

def create_byo_lp_tables():
    
     c.executescript('''         
     CREATE TABLE byo_lp(
         lp_id INTEGER NOT NULL PRIMARY KEY,
         lp_name TEXT NOT NULL,
         type_id INTEGER NOT NULL,
         asset_amount DOUBLE PRECISION NULL,
         currency TEXT NULL,
         hq_city TEXT NOT NULL,
         hq_country TEXT NOT NULL,
         FOREIGN KEY(type_id)
         REFERENCES byo_lp_type(type_id)
         );
     
     CREATE TABLE byo_lp_type(
         lp_type_id INTEGER NOT NULL PRIMARY KEY,
         type TEXT NOT NULL
         );
     
     CREATE TABLE byo_lp_strategy(
         lp_strategy_id INTEGER NOT NULL PRIMARY KEY,
         lp_id INTEGER NOT NULL,
         regions TEXT NOT NULL,
         strategy TEXT NOT NULL,
         FOREIGN KEY(lp_id)
         REFERENCES byo_lp(lp_id)
         );
     
      CREATE TABLE byo_lp_sector(
          lp_sector_id INTEGER NOT NULL PRIMARY KEY,
          lp_id INTEGER NOT NULL,
          sector TEXT NOT NULL,
          has_appetite INTEGER NOT NULL,
          FOREIGN KEY(lp_id)
          REFERENCES byo_lp(lp_id)
          );
      
      CREATE TABLE byo_lp_commitment(
          lp_commitment_id INTEGER NOT NULL PRIMARY KEY,
          lp_id INTEGER NOT NULL,
          fund_id INTEGER NOT NULL,
          fund_name TEXT NOT NULL,
          fund_manager_id INTEGER NOT NULL,
          fund_manager TEXT NOT NULL,
          sectors TEXT NOT NULL,
          strategies TEXT NOT NULL,
          fund_size_amt DOUBLE PRECISION NOT NULL,
          fund_size_currency TEXT NULL,
          commitment_amt DOUBLE PRECISION NULL,
          commitment_currency TEXT NULL,
          regions TEXT NOT NULL,
          vintage_year INTEGER NOT NULL,
          FOREIGN KEY(lp_id)
          REFERENCES byo_lp(lp_id),
          FOREIGN KEY(fund_id)
          REFERENCES byo_fund(fund_id),
          FOREIGN KEY(fund_id)
          REFERENCES byo_historical_fund(fund_id)
          );
      
      CREATE TABLE byo_lp_office(
          lp_office_id INTEGER NOT NULL PRIMARY KEY,
          lp_id INTEGER NOT NULL,
          email TEXT NULL,
          phone TEXT NULL,
          web_address TEXT NULL,
          street_address TEXT NOT NULL,
          city TEXT NOT NULL,
          county_state TEXT NULL,
          postal_code TEXT NULL,
          country TEXT NOT NULL,
          region TEXT NOT NULL,
          head_office INTEGER NOT NULL,
          FOREIGN KEY(lp_id)
          REFERENCES byo_lp(lp_id)
          );
      
       CREATE TABLE byo_lp_contact(
           lp_contact_id INTEGER NOT NULL PRIMARY KEY,  
           lp_id INTEGER NOT NULL,
           firstname TEXT NOT NULL,
           surname TEXT NOT NULL,
           title TEXT NOT NULL,
           job_title TEXT NOT NULL,
           city TEXT NOT NULL,
           country TEXT NOT NULL,
           email TEXT NULL,
           phone TEXT NULL,
           FOREIGN KEY(lp_id)
           REFERENCES byo_lp(lp_id)
           );
   ''')

def import_csv_files():
    
    buyouts_lp_final = pd.read_csv('/Users/AmeliaMazer/Documents/2022_svb_github/buyouts/buyouts_lp/buyouts_lp_csv/buyouts_lp_final.csv')
    buyouts_lp_final.to_sql('byo_lp', conn, if_exists='replace', index=False) 
    
    buyouts_lp_type_id = pd.read_csv('/Users/AmeliaMazer/Documents/2022_svb_github/buyouts/buyouts_lp/buyouts_lp_csv/lp_types.csv')
    buyouts_lp_type_id.to_sql('byo_lp_type', conn, if_exists='replace', index=False)
    
    lp_strategy = pd.read_csv('/Users/AmeliaMazer/Documents/2022_svb_github/buyouts/buyouts_lp/buyouts_lp_csv/lp_strategy.csv')
    lp_strategy.to_sql('byo_lp_strategy', conn, if_exists='replace', index=False) 
    
    lp_sector = pd.read_csv('/Users/AmeliaMazer/Documents/2022_svb_github/buyouts/buyouts_lp/buyouts_lp_csv/lp_sector.csv')
    lp_sector.to_sql('byo_lp_sector', conn, if_exists='replace', index=False)
    
    lp_commitments = pd.read_csv('/Users/AmeliaMazer/Documents/2022_svb_github/buyouts/buyouts_lp/buyouts_lp_csv/lp_commitments.csv')
    lp_commitments.to_sql('byo_lp_commitment', conn, if_exists='replace', index=False)
    
    lp_office = pd.read_csv('/Users/AmeliaMazer/Documents/2022_svb_github/buyouts/buyouts_lp/buyouts_lp_csv/lp_office.csv')
    lp_office.to_sql('byo_lp_office', conn, if_exists='replace', index=False)
    
    lp_contact = pd.read_csv('/Users/AmeliaMazer/Documents/2022_svb_github/buyouts/buyouts_lp/buyouts_lp_csv/lp_contact.csv')
    lp_contact.to_sql('byo_lp_contact', conn, if_exists='replace', index=False)
    
def main():
   drop_table()
   create_byo_lp_tables()
   import_csv_files()
   
if __name__ =='__main__':
   args = get_args()
   database_name = args.db
   conn = sqlite3.connect(database_name)
   c = conn.cursor()
   main()   
    
    
