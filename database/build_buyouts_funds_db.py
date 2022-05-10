import sqlite3
import pandas as pd
import argparse


def get_args():
    parser = argparse.ArgumentParser(description ='Enter csv file path and database name')
    parser.add_argument('-db','--db', type = str, help = 'name of the database you want to import the csv file to')
    return parser.parse_args()

def drop_table():
    c.executescript('''
    DROP TABLE IF EXISTS byo_fund_manager;
    DROP TABLE IF EXISTS byo_fund;
    DROP TABLE IF EXISTS byo_fm_fund_history;
    DROP TABLE IF EXISTS byo_fm_contact;
    DROP TABLE IF EXISTS byo_fm_office;
    DROP TABLE IF EXISTS byo_fm_sector;
    DROP TABLE IF EXISTS byo_fm_strategy;
    DROP TABLE IF EXISTS byo_historical_fund;
   ''')

def create_byo_funds_tables():
    c.executescript('''
                    
    CREATE TABLE byo_fund_manager(
        fm_id INTEGER NOT NULL PRIMARY KEY,
        fm_name TEXT NOT NULL  
        );

    CREATE TABLE byo_fund(
        fund_id INTEGER NOT NULL PRIMARY KEY,
        fund_name   TEXT NOT NULL,
        fm_id INTEGER NOT NULL,
        strategy TEXT NOT NULL,
        region TEXT NOT NULL,
        country TEXT NULL,
        target_fund_size_amt DOUBLE PRECISION NOT NULL,
        target_fund_size_currency TEXT NOT NULL,
        fund_size_amt DOUBLE PRECISION NULL,
        FOREIGN KEY(fm_id)
        REFERENCES byo_fund_manager(fm_id)
    );

    CREATE TABLE byo_fm_fund_history(
        fm_fund_history_id INTEGER NOT NULL PRIMARY KEY,
        fm_id INTEGER NOT NULL,
        fund_id INTEGER NOT NULL,
        fund_name TEXT NOT NULL,
        vintage_year INTEGER NOT NULL,
        region TEXT NOT NULL,
        sector TEXT NULL,
        strategy TEXT NULL,
        fund_size_amt DOUBLE PRECISION NULL,
        find_currency TEXT NULL,
        target_fund_size_amt DOUBLE PRECISION NULL,
        target_fund_size_currency TEXT  NOT NULL,
        FOREIGN KEY(fm_id)
        REFERENCES byo_fund_manager(fm_id),
        FOREIGN KEY(fund_id)
        REFERENCES byo_historical_fund(fund_id)
        );

    CREATE TABLE byo_fm_contact(
        fm_contacts_id INTEGER NOT NULL PRIMARY KEY,
        fm_id INTEGER NOT NULL,
        first_name TEXT NOT NULL,
        surname TEXT NOT NULL,
        title TEXT NULL,
        job_title TEXT NULL,
        email TEXT NULL,
        city TEXT NOT NULL,
        country TEXT NOT NULL,
        phone TEXT NULL,
        FOREIGN KEY(fm_id)
        REFERENCES byo_fund_manager(fm_id)
    
    );


    CREATE TABLE byo_fm_office(
        fm_office_id INTEGER NOT NULL PRIMARY KEY,
        fm_id INTEGER NOT NULL,
        region TEXT NOT NULL,
        country TEXT NOT NULL,
        state TEXT NULL,
        city TEXT NULL,
        street_address TEXT NULL,
        postal_code TEXT NULL,
        phone TEXT NULL,
        email TEXT NULL,
        web_address TEXT NULL,
        head_office BOOLEAN NOT NULL,
        FOREIGN KEY(fm_id)
        REFERENCES byo_fund_manager(fm_id)
    
    );

    CREATE TABLE byo_fm_sector(
        fm_sector_id INTEGER NOT NULL PRIMARY KEY,
        fm_id INTEGER NOT NULL,
        sector TEXT NULL,
        has_appetite BOOLEAN NULL,
        FOREIGN KEY(fm_id)
        REFERENCES byo_fund_manager(fm_id)
    
    );

    CREATE TABLE byo_fm_strategy(
        fm_strategy_id INTEGER NOT NULL PRIMARY KEY,
        fm_id INTEGER NOT NULL,
        regions TEXT NULL,
        strategy TEXT NOT NULL,
        FOREIGN KEY(fm_id)
        REFERENCES byo_fund_manager(fm_id)
        
    );
    
    CREATE TABLE byo_historical_fund(
        fund_id INTEGER NOT NULL PRIMARY KEY,
        fund_name   TEXT NOT NULL,
        manager_id INTEGER NOT NULL,
        manager TEXT NULL,
        close_year INT NOT NULL,
        strategy TEXT NOT NULL,
        fund_size_amt DOUBLE PRECISION NULL,
        fund_size_currency TEXT  NULL,
        FOREIGN KEY(manager_id)
        REFERENCES byo_fund_manager(fm_id)
    
    );

    ''')

def import_csv_files():
    byo_fund_manager = pd.read_csv('/Users/AmeliaMazer/Documents/svb_practicum/buyouts/buyouts_funds/buyouts_funds_csv/buyouts_fm_id.csv')
    byo_fund_manager.to_sql('byo_fund_manager', conn, if_exists='replace', index = False)

    byo_funds = pd.read_csv('/Users/AmeliaMazer/Documents/svb_practicum/buyouts/buyouts_funds/buyouts_funds_csv/buyouts_funds_final.csv')
    byo_funds.to_sql('byo_fund',conn, if_exists='replace', index = False)

    byo_fm_fund_history = pd.read_csv('/Users/AmeliaMazer/Documents/svb_practicum/buyouts/buyouts_funds/buyouts_funds_csv/buyouts_fm_fund_history.csv')
    byo_fm_fund_history.to_sql('byo_fm_fund_history',conn, if_exists='replace', index = False)

    byo_fm_contact = pd.read_csv('/Users/AmeliaMazer/Documents/svb_practicum/buyouts/buyouts_funds/buyouts_funds_csv/buyouts_fm_contact.csv')
    byo_fm_contact.to_sql('byo_fm_contact',conn, if_exists='replace', index = False)

    byo_fm_office = pd.read_csv('/Users/AmeliaMazer/Documents/svb_practicum/buyouts/buyouts_funds/buyouts_funds_csv/buyouts_fm_office.csv')
    byo_fm_office.to_sql('byo_fm_office',conn, if_exists='replace', index = False)

    byo_fm_sectors = pd.read_csv('/Users/AmeliaMazer/Documents/svb_practicum/buyouts/buyouts_funds/buyouts_funds_csv/buyouts_fm_sectors.csv')
    byo_fm_sectors.to_sql('byo_fm_sector',conn, if_exists='replace', index = False)

    byo_fm_strategies = pd.read_csv('/Users/AmeliaMazer/Documents/svb_practicum/buyouts/buyouts_funds/buyouts_funds_csv/buyouts_fm_strategy.csv')
    byo_fm_strategies.to_sql('byo_fm_strategy',conn, if_exists='replace', index = False )
    
    byo_historical_fund = pd.read_csv('/Users/AmeliaMazer/Documents/svb_practicum/buyouts/buyouts_funds/buyouts_funds_csv/buyouts_historical_funds.csv')
    byo_historical_fund.to_sql('byo_historical_fund',conn, if_exists='replace', index = False )
    

def main():
   drop_table()
   create_byo_funds_tables()
   import_csv_files()
   
     
    
if __name__ =='__main__':
   args = get_args()
   database_name = args.db
   conn = sqlite3.connect(database_name)
   c = conn.cursor()
   main()