import sqlite3
import pandas as pd
import argparse


def get_args():
    parser = argparse.ArgumentParser(description ='Enter csv file path and database name')
    parser.add_argument('-db','--db', type = str, help = 'name of the database you want to import the csv file to')
    return parser.parse_args()

###byouts

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
        REFERENCES byo_fund(fund_id)
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
  
###pitchbook

def drop_table():
    c.executescript('''
    DROP TABLE IF EXISTS pb_investor;
    DROP TABLE IF EXISTS pb_lp;
    DROP TABLE IF EXISTS pb_lp_investor;
   ''')

def create_pb_funds_tables():
    c.executescript('''

    CREATE TABLE pb_investor(
        investor_id INTEGER NOT NULL PRIMARY KEY,
        investor_name TEXT NOT NULL，
        investment_num   INTEGER NULL,
        total_funds_open INTEGER NULL,
        total_funds_closed INTEGER NULL,
        AUM INTEGER NULL,
        dry_powder INTEGER NULL,
        last_invesgtment_type INTEGER NULL,
        last_investment_valuation INTEGER NULL,
        last_investment_size INTEGER NULL,
        last_investment_class TEXT NULL,
        investments_last_5_yrs INTEGER NULL,
        investments_last_2_yrs INTEGER NULL,
        investments_last_12_months INTEGER NULL,
        investments_last_6_months INTEGER NULL,
        active_portfolio INTEGER NULL,
        exits INTEGER NULL,
        last_closed_fund_size INTEGER NULL,
        preferred_industry_1 TEXT NULL,
        preferred_industry_2 TEXT NULL,
        primary_investor_type TEXT NULL,
        max_fund_size INTEGER NULL,
        median_fund_size INTEGER NULL,
        min_fund_size INTEGER NULL,
        fund_closed_num INTEGER NULL,
        total_investments_last_5_yrs INTEGER NULL,
        total_investments_last_2_yrs INTEGER NULL,
        total_investments_last_12_months INTEGER NULL,
        total_investments_last_6_months INTEGER NULL,
        total_active_portfolio INTEGER NULL,
        total_exits INTEGER NULL,
        total_investments INTEGER NULL
        ); 
    
    CREATE TABLE pb_lp(
        lp_id INTEGER NOT NULL PRIMARY KEY,
        lp_name TEXT NOT NULL，  
        cmmt_in_vc_funds  INTEGER NULL,
        total_cmmt_in_vc_funds INTEGER NULL,
        limited_partner_type TEXT NULL,
        AUM INTEGER NULL,
        affliated_funds_num INTEGER NULL,
        affliated_investors_num INTEGER NULL,
        commitments INTEGER NULL,
        total_cmmt INTEGER NULL,
        hq_location TEXT NULL
        );
    
       CREATE TABLE pb_lp_investor(
       investment_id INTEGER NOT NULL PRIMARY KEY,
       investor_name TEXT NOT NULL,
       limmited_partner_name TEXT NOT NULL,
       commitments INTEGER NULL,
       FOREIGN KEY(investor_name)
       REFERENCES pb_investor(investor_name),
       FOREIGN KEY(limited_partner_name)
       RFERENCES pb_lp(lp_name)
        );
    
    ''')

###ImportCsvFiles
###Importbyo
    
def import_csv_files():
    pb_lp_info = pd.read_csv('/Users/AmeliaMazer/Documents/2022_svb_github/pitchbook_data/lp_info.csv')
    pb_lp_info.to_sql('pb_lp_info',conn, if_exists='replace', index = False)
    
    pb_investor_info = pd.read_csv('/Users/AmeliaMazer/Documents/2022_svb_github/pitchbook_data/investor_info.csv')
    pb_investor_info.to_sql('pb_investor_info',conn, if_exists='replace', index = False)
    
    pb_investor_lp = pd.read_csv('/Users/AmeliaMazer/Documents/2022_svb_github/pitchbook_data/investor_lp.csv')
    pb_investor_lp.to_sql('pb_invsetor_lp',conn, if_exists='replace', index = False)
    

   

def main():
   drop_table()
   create_pb_funds_tables()
   import_csv_files()

    
if __name__ =='__main__':
   args = get_args()
   database_name = args.db
   conn = sqlite3.connect(database_name)
   c = conn.cursor()
   main()