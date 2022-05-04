import sqlite3
import pandas as pd



conn = sqlite3.connect('svb_practicum.db')
c= conn.cursor()

c.executescript('''
DROP TABLE IF EXISTS fund_manager;
DROP TABLE IF EXISTS fund;
DROP TABLE IF EXISTS fm_fund_history;
DROP TABLE IF EXISTS fm_contact;
DROP TABLE IF EXISTS fm_office;
DROP TABLE IF EXISTS fm_sector;
DROP TABLE IF EXISTS fm_strategy;
DROP TABLE IF EXISTS byo_lp;
DROP TABLE IF EXISTS byo_lp_type;
DROP TABLE IF EXISTS byo_lp_strategy;
DROP TABLE IF EXISTS byo_lp_sector;
DROP TABLE IF EXISTS byo_lp_commitment;
DROP TABLE IF EXISTS byo_lp_office;
DROP TABLE IF EXISTS byo_lp_contact;

CREATE TABLE fund_manager(
    fm_id INTEGER NOT NULL PRIMARY KEY,
    fm_name TEXT NOT NULL  
);

CREATE TABLE fund(
    fund_id INTEGER NOT NULL PRIMARY KEY,
    name   TEXT NOT NULL,
    manager_id INTEGER NOT NULL,
    strategy TEXT NOT NULL,
    region TEXT NOT NULL,
    country TEXT NULL,
    target_fund_size_amt DOUBLE PRECISION NOT NULL,
    target_fund_size_currency TEXT NOT NULL,
    fund_size_amt DOUBLE PRECISION NULL,
    FOREIGN KEY(manager_id)
    REFERENCES fund_managers(fm_id)
    
);

CREATE TABLE fm_fund_history(
    fm_fund_id INTEGER NOT NULL PRIMARY KEY,
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
    REFERENCES fund_managers(fm_id),
    FOREIGN KEY(fund_id)
    REFERENCES funds(fund_id)
);

CREATE TABLE fm_contact(
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
    REFERENCES fund_managers(fm_id)
    
);


CREATE TABLE fm_office(
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
    REFERENCES fund_managers(fm_id)
    
);

CREATE TABLE fm_sector(
    fm_sector_id INTEGER NOT NULL PRIMARY KEY,
    fm_id INTEGER NOT NULL,
    sector TEXT NULL,
    has_appetite BOOLEAN NULL,
    FOREIGN KEY(fm_id)
    REFERENCES fund_managers(fm_id)
    
);

CREATE TABLE fm_strategy(
    fm_strategy_id INTEGER NOT NULL PRIMARY KEY,
    fm_id INTEGER NOT NULL,
    regions TEXT NULL,
    strategy TEXT NOT NULL,
    FOREIGN KEY(fm_id)
    REFERENCES fund_managers(fm_id)
    
);

CREATE TABLE byo_lp_type(
         type_id INTEGER NOT NULL PRIMARY KEY,
         type TEXT NOT NULL
         );

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
    
CREATE TABLE byo_lp_strategy(
         strategy_id INTEGER NOT NULL PRIMARY KEY,
         lp_id INTEGER NOT NULL,
         regions TEXT NOT NULL,
         strategy TEXT NOT NULL,
         FOREIGN KEY(lp_id)
         REFERENCES byo_lp(lp_id)
         );
     
CREATE TABLE byo_lp_sector(
          sector_id INTEGER NOT NULL PRIMARY KEY,
          lp_id INTEGER NOT NULL,
          sector TEXT NOT NULL,
          has_appetite INTEGER NOT NULL,
          FOREIGN KEY(lp_id)
          REFERENCES byo_lp(lp_id)
          );
      
CREATE TABLE byo_lp_commitment(
          commitment_id INTEGER NOT NULL PRIMARY KEY,
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
          office_id INTEGER NOT NULL PRIMARY KEY,
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
           contact_id INTEGER NOT NULL PRIMARY KEY, 
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


fund_managers = pd.read_csv('/Users/AmeliaMazer/Documents/2022_SVB_GitHub/buyouts/buyouts_funds/buyouts_funds_cleaned_csv/buyouts_fm_id.csv')
fund_managers.to_sql('fund_managers', conn, if_exists='replace', index = False)

funds = pd.read_csv('/Users/AmeliaMazer/Documents/2022_SVB_GitHub/buyouts/buyouts_funds/buyouts_funds_csv/buyouts_funds_final.csv')
funds.to_sql('funds',conn, if_exists='replace', index = False)

fm_fund_history = pd.read_csv('/Users/AmeliaMazer/Documents/2022_SVB_GitHub/buyouts/buyouts_funds/buyouts_funds_cleaned_csv/buyouts_fm_fund_history.csv')
fm_fund_history.to_sql('fm_fund_history',conn, if_exists='replace', index = False)

fm_contacts = pd.read_csv('/Users/AmeliaMazer/Documents/2022_SVB_GitHub/buyouts/buyouts_funds/buyouts_funds_cleaned_csv/buyouts_fm_contact.csv')
fm_contacts.to_sql('fm_contacts',conn, if_exists='replace', index = False)

fm_office = pd.read_csv('/Users/AmeliaMazer/Documents/2022_SVB_GitHub/buyouts/buyouts_funds/buyouts_funds_cleaned_csv/buyouts_fm_office.csv')
fm_office.to_sql('fm_offices',conn, if_exists='replace', index = False)

fm_sectors = pd.read_csv('/Users/AmeliaMazer/Documents/2022_SVB_GitHub/buyouts/buyouts_funds/buyouts_funds_cleaned_csv/buyouts_fm_sectors.csv')
fm_sectors.to_sql('fm_sectors',conn, if_exists='replace', index = False)

fm_strategies = pd.read_csv('/Users/AmeliaMazer/Documents/2022_SVB_GitHub/buyouts/buyouts_funds/buyouts_funds_cleaned_csv/buyouts_fm_strategy.csv')
fm_strategies.to_sql('fm_strategies',conn, if_exists='replace', index = False )

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
    
lp_contact = pd.read_csv('')
lp_contact.to_sql('byo_lp_contact', conn, if_exists='replace', index=False)




