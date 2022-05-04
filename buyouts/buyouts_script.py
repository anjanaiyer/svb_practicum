import pandas as pd
import numpy as np
import regex as re


def funds_normalization():
    df = pd.read_csv('buyouts_funds/buyouts_funds_orig.csv')

    df = df.set_index(['ID', 'Name', 'FundManager', 'FundManagerID', 'Region', 'Sector', 'VintageYear','YearOpened', 'MonthsOpened', 'TargetSize' ,'Currency']).stack().str.split('/', expand=True).stack().unstack(-2).reset_index(-1, drop=True).reset_index()

    df = df.set_index(['ID', 'Name', 'FundManager', 'FundManagerID','Strategy', 'Sector', 'VintageYear','YearOpened', 'MonthsOpened', 'TargetSize' ,'Currency']).stack().str.split('/', expand=True).stack().unstack(-2).reset_index(-1, drop=True).reset_index()

    df = df.set_index(['ID', 'Name', 'FundManager', 'FundManagerID','Strategy', 'Region', 'VintageYear','YearOpened', 'MonthsOpened', 'TargetSize' ,'Currency']).stack().str.split('/', expand=True).stack().unstack(-2).reset_index(-1, drop=True).reset_index()

    df.columns = ['fund_id', 'fund_name', 'fund_manager', 'fund_manager_id', 'strategy', 'region', 'vintage_year', 'year_opened', 'month_launched', 'target_size', 'currency', 'sector']

    list_col =  df.columns.to_list()
    df = df.reindex(columns = list_col)
    df.to_csv('buyouts_funds/funds_1NF.csv', index = False)

    df2 = df.copy()
    df2.drop('fund_manager_id', axis = 1, inplace = True)
    df2['month_launched'] = df2['month_launched'].apply(str)
    df2['year_opened'] = df2['year_opened'].apply(str)
    df2['open_date'] = df2['year_opened']+ '-' + df2['month_launched']
    df2.drop(columns =['month_launched','year_opened'], axis = 1, inplace = True)
    list_col = df2.columns.tolist()
    list_col.insert(df2.columns.get_loc('fund_manager') + 1,list_col.pop(-1))
    df2 = df2.reindex(columns = list_col)
    df2.to_csv('buyouts_funds/funds_3NF.csv', index = False)
    df2.to_csv('buyouts_funds/byo_funds.csv',index = False)


def lp_normalization():
    df1 = pd.read_csv('buyouts_lp/buyouts_lp_orig.csv')

    df1.drop('Unnamed: 0', axis=1, inplace = True)

    df1['HQ'].replace('\.','', regex=True, inplace = True)
    df1['HQ'].replace('\, DC', ' DC', regex=True, inplace = True)
    df1.groupby('HQ')['HQ'].count()
    df1[['hq_city', 'hq_country']] = df1['HQ'].str.split(',',1, expand = True)
    df1.drop('HQ', axis = 1, inplace = True)
    df1.columns =['lp_id', 'lp_name', 'type', 'type_id', 'asset_amount', 'currency', 'hq_city', 'hq_country']
    list_col1 = df1.columns.tolist()
    df1 = df1.reindex(columns = list_col1)
    df1.to_csv('buyouts_lp/lp_1NF.csv', index = False)

    df3 = df1.copy()
    types = df3.filter(['type_id', 'type'], axis=1)
    types = types.sort_values("type_id")
    types = types.drop_duplicates(subset = "type_id", keep = "first")
    types.to_csv('buyouts_lp/lp_types.csv')

    lp_types = df3.filter(['lp_id', 'type_id'], axis=1)
    lp_types = lp_types.sort_values('lp_id')
    lp_types = lp_types.drop_duplicates(subset = "lp_id", keep = "first")
    #lp_types.to_csv('lp_typ_associative.csv')


def main():
    funds_normalization()
    lp_normalization() 


if __name__ =='__main__':
    main()