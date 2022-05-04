import pandas as pd
import numpy as np
import regex as re
import sqlite3 
import argparse

def get_args():
    parser = argparse.ArgumentParser(description='Enter CSV file path and database name')
    parser.add_argument('-source', '--source', type=str, help='path to CSV file')
    parser.add_argument('-db', '--db', type=str, help='database to import CSV file into')
    return parser.parse_args()


def create_buyouts_lp_final_table(csv_file_path):
    c.executescript('''
    DROP TABLE IF EXISTS buyouts_lp_final;

    CREATE TABLE buyouts_lp_final(
        lp_id INTEGER NOT NULL PRIMARY KEY,
        lp_name TEXT NOT NULL,
        type_id INTEGER NOT NULL,
        asset_amount DOUBLE PRECISION NULL,
        currency TEXT NULL,
        hq_city TEXT NOT NULL,
        hq_country TEXT NOT NULL
        );
    ''')
    buyouts_lp_final = pd.read_csv(buyouts_lp_final_path)
    buyouts_lp_final.to_sql('buyouts_lp_final', conn, if_exists='replace', index=False) 


def create_buyouts_lp_type_id_table(csv_file_path):
    c.executescript('''
    DROP TABLE IF EXISTS buyouts_lp_type_id;

    CREATE TABLE buyouts_lp_type_id(
        type_id INTEGER NOT NULL,
        type TEXT NOT NULL,
        FOREIGN KEY(type_id)
        REFERENCES buyouts_lp_final(type_id)
        );
    ''')
    buyouts_lp_type_id = pd.read_csv(buyouts_lp_type_id_path)
    buyouts_lp_type_id.to_sql('buyouts_lp_type_id', conn, if_exists='replace', index=False)


def create_lp_strategy_table(csv_file_path):
    c.executescript('''
    DROP TABLE IF EXISTS lp_strategy;

    CREATE TABLE lp_strategy(
        strategy_id INTEGER NOT NULL PRIMARY KEY,
        lp_id INTEGER NOT NULL,
        regions TEXT NOT NULL,
        strategy TEXT NOT NULL,
        FOREIGN KEY(lp_id)
        REFERENCES buyouts_lp_final(lp_id)
        );
    ''')
    lp_strategy = pd.read_csv(lp_strategy_path)
    lp_strategy.to_sql('lp_strategy', conn, if_exists='replace', index=False) 


def create_lp_sector_table(csv_file_path):
    c.executescript('''
    DROP TABLE IF EXISTS lp_sector;
    
    CREATE TABLE lp_sector(
        sector_id INTEGER NOT NULL PRIMARY KEY,
        lp_id INTEGER NOT NULL,
        sector TEXT NOT NULL,
        has_appetite INTEGER NOT NULL,
        FOREIGN KEY(lp_id)
        REFERENCES buyouts_lp_final(lp_id)
        );
    ''')
    lp_sector = pd.read_csv(lp_sector_path)
    lp_sector.to_sql('lp_sector', conn, if_exists='replace', index=False)


def create_lp_commitments_table(csv_file_path):
    c.executescript('''
    DROP TABLE IF EXISTS lp_commitments;

    CREATE TABLE lp_commitments(
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
        REFERENCES byo_fund(fund_id)
        FOREIGN KEY(fund_id)
        REFERENCES byo_historical_fund(fund_id)
        );
    ''')
    lp_commitments = pd.read_csv(lp_commitments_path)
    lp_commitments.to_sql('lp_commitments', conn, if_exists='replace', index=False)


def create_lp_office_table(csv_file_path):
    c.executescript('''
    DROP TABLE IF EXISTS lp_office;

    CREATE TABLE lp_office(
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
        REFERENCES buyouts_lp_final(lp_id)
        );
    ''')
    lp_office = pd.read_csv(lp_office_path)
    lp_office.to_sql('lp_office', conn, if_exists='replace', index=False)


def create_lp_contact_table(csv_file_path):
    c.executescript('''
    DROP TABLE IF EXISTS lp_contact;

    CREATE TABLE lp_contact(
        lp_id INTEGER NOT NULL PRIMARY KEY,
        firstname TEXT NOT NULL,
        surname TEXT NOT NULL,
        title TEXT NOT NULL,
        job_title TEXT NOT NULL,
        city TEXT NOT NULL,
        country TEXT NOT NULL,
        email TEXT NULL,
        phone TEXT NULL,
        );
    ''')
    lp_contact = pd.read_csv(lp_contact_path)
    lp_contact.to_sql('lp_contact', conn, if_exists='replace', index=False)


if __name__ == '__main__':
    args = get_args()

    buyouts_lp_final_path = args.source
    buyouts_lp_type_id_path = args.source
    lp_strategy_path = args.source
    lp_sector_path = args.source
    lp_commitments_path = args.source
    lp_office_path = args.source
    lp_contact_path = args.source

    database_name = args.db

    conn = sqlite3.connect(database_name)
    c = conn.cursor()

    create_buyouts_lp_final_table(buyouts_lp_final_path)
    create_buyouts_lp_type_id_table(buyouts_lp_type_id_path)
    create_lp_strategy_table(lp_strategy_path)
    create_lp_sector_table(lp_sector_path)
    create_lp_commitments_table(lp_commitments_path)
    create_lp_office_table(lp_office_path)
    create_lp_contact_table(lp_contact_path)

    conn.commit()
    if conn:
        conn.close()