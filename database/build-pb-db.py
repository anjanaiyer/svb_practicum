import sqlite3
import pandas as pd
import argparse


def get_args():
    parser = argparse.ArgumentParser(description ='Enter csv file path and database name')
    parser.add_argument('-db','--db', type = str, help = 'name of the database you want to import the csv file to')
    return parser.parse_args()
  


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
        investor_name TEXT NOT NULL,
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
        lp_name TEXT NOT NULL, 
        cmmt_in_vc_funds  INTEGER NULL,
        total_cmmt_in_vc_funds INTEGER NULL,
        limited_partner_type TEXT NULL,
        AUM INTEGER NULL,
        affliated_funds_num INTEGER NULL,
        affliated_investors_num INTEGER NULL,
        commitments INTEGER NULL,
        total_cmmt INTEGER NULL,
        hq_location TEXT NULL,
        hq_region TEXT NULL
        );
    
       CREATE TABLE pb_lp_investor(
       investment_id INTEGER NOT NULL PRIMARY KEY,
       investor_name TEXT NOT NULL,
       limited_partner_name TEXT NOT NULL,
       commitments INTEGER NULL,
       FOREIGN KEY(investor_name)
       REFERENCES pb_investor(investor_name),
       FOREIGN KEY(limited_partner_name)
       REFERENCES pb_lp(lp_name)
        );
    
    ''')

###ImportCsvFiles
###Importbyo
    
def import_csv_files():
    pb_lp_info = pd.read_csv('/Users/AmeliaMazer/Documents/2022_svb_github/pitchbook_data_2/lp_info.csv')
    pb_lp_info.to_sql('pb_lp',conn, if_exists='replace', index = False)
    
    pb_investor_info = pd.read_csv('/Users/AmeliaMazer/Documents/2022_svb_github/pitchbook_data_2/investor_info.csv')
    pb_investor_info.to_sql('pb_investor',conn, if_exists='replace', index = False)
    
    pb_investor_lp = pd.read_csv('/Users/AmeliaMazer/Documents/2022_svb_github/pitchbook_data_2/investor_lp.csv')
    pb_investor_lp.to_sql('pb_lp_investor',conn, if_exists='replace', index = False)
    

   

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