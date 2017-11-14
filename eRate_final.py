# -*- coding: utf-8 -*-
"""
Created on Tue Aug 08 08:33:35 2017

@author: yuhapen
"""

import pandas as pd
import xlsxwriter, xlrd, re
import numpy as np
import os.path, copy
import csv
import smtplib
import math
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate

# output the K12 file more correctly
# BEN corresponding to multiple customer code, which is not correct, filter it based on the T12MDollar
# case 1 : more than 1 customer code's T12MDollar > 0, remove this BEN
# case 2 : only one customer code's T12MDollar > 0, select this customer code to match the BEN
# case 3 : none customer code's T12MDollar > 0, selection based on the date(more recently)

def k12_data_filter():
    k12_indx = ['Customer Code', 'USAC BEN', 'USAC Enrollment', 'Primary AM']
    k12_data = pd.read_excel('K12 Book Mapped to BEN.xlsx', usecols = k12_indx, converters = {'Customer Code': lambda x: str(x)}) # 18181
    
    # filter the BEN that has more than one customercode with non-zero T12Mcost
    BEN_mis_count = pd.read_excel('BEN_count_for_T12MDollar.xlsx') # file with the number of customer code that has non-zero T12MDollar for each BEN
    k12_data = k12_data.rename(columns = {'Customer Code' : 'CustomerCode', 'USAC BEN' : 'BEN', 'USAC Enrollment' : 'Enrollment', 'Primary AM' : 'PAM'})
    k12_data = pd.merge(k12_data, BEN_mis_count, how = 'left', on = 'BEN')
    k12_data = k12_data.fillna(-1)
    k12_data = k12_data[k12_data.Count < 2] # 17871
    
    # filter the BEN that has one customercode with non-zero T12Mcost(select the customercode that has non-zero T12MCost)
    T12M_data_indx = ['CustomerSeq', 'T12OrderDollarAmount']
    customerseq_code = pd.read_csv('customerseq.txt', sep = '\t', converters = {'CustomerCode': lambda x: str(x), 'CustomerSeq': lambda x: str(x)})
    T12M_data = pd.read_csv('T12M_CustomerSeq_unique.csv', sep = ',', usecols = T12M_data_indx, converters = {'CustomerCode': lambda x: str(x), 'CustomerSeq': lambda x: str(x)})
    T12M_data = pd.merge(T12M_data, customerseq_code, how = 'left', on = 'CustomerSeq')
    T12M_data = T12M_data[pd.notnull(T12M_data['CustomerCode'])]
    k12_data = pd.merge(k12_data, T12M_data, how = 'left', on = 'CustomerCode') # 17871
    k12_data_sub = k12_data[k12_data.Count == 1][k12_data.T12OrderDollarAmount > 0.0]
    k12_data = k12_data[k12_data.Count != 1] # 17261
    k12_data = k12_data.append([k12_data_sub], ignore_index = True) # 17549
    
    # filter the BEN that has zero customercode with non-zero T12Mcost(select the earlier startdata)
    Date_data = pd.read_csv('CustomerCode_CreatedDate.csv', sep = ',', converters = {'CustomerCode': lambda x: str(x)})
    k12_data = pd.merge(k12_data, Date_data, how = 'left', on = 'CustomerCode')
    k12_data_sub = k12_data[k12_data.Count == 0]
    unique_BEN = k12_data_sub.BEN.unique()
    customer_codes = []
    for ben in unique_BEN:
        data = k12_data_sub[:][k12_data_sub.BEN == ben]
        most_recent = data['AccountCreatedDateSeq'][data.BEN == ben].max()
        customer_code = data.loc[data['AccountCreatedDateSeq'] == most_recent, 'CustomerCode'].iloc[0]
        customer_codes.append(customer_code)
    # print(customer_codes)
    data = {'CustomerCode' : pd.Series(customer_codes, index = range(len(customer_codes)))}
    data_frame = pd.DataFrame(data)
    k12_data_sub = pd.merge(data_frame, k12_data_sub, how = 'left', on = 'CustomerCode')
    # k12_data_sub = k12_data_sub[pd.notnull(k12_data_sub['CustomerSeq'])]
    k12_data = k12_data[k12_data.Count != 0]
    k12_data = k12_data.append([k12_data_sub], ignore_index = True)
    # k12_data = k12_data[pd.notnull(k12_data['CustomerSeq'])]
    k12_data = k12_data.ix[:, ['CustomerCode', 'BEN', 'PAM', 'Enrollment']]# 17500
    
    return k12_data

# Join 15, 16, 17 raw data with "K12 book" to obtain the customercode and Primary AM
def read_raw_data(k12_data):
    # for 15 raw data
    raw_data_15_index = ['Applicant', 'BEN', 'State', 'E-rateDisc', 'ServiceProvider', 'ProductType (group)', 'EquipMake', 'TotalPreDiscAnnual (SUM)', 'RequestAmt']
    raw_data_15 = pd.read_excel('FFL FY2015 Item 21 Final Data Set 2015-06-05 NT C2 Data Only.xlsx', skiprows = 1, usecols = raw_data_15_index)
    raw_data_15 = raw_data_15.rename(columns = {'E-rateDisc' : 'Discpct', 'ProductType (group)' : 'ProductType', 'EquipMake' : 'Brand', 'TotalPreDiscAnnual (SUM)' : 'TotalSpend'})
    raw_data_15_with_k12 = pd.merge(raw_data_15, k12_data, how = 'left', on = 'BEN')
    # drop those rows which without customercode and Primary AM
    raw_data_15_with_k12 = raw_data_15_with_k12[pd.notnull(raw_data_15_with_k12['CustomerCode'])]
    raw_data_15_with_k12['AccountManagerCode'] = raw_data_15_with_k12['PAM']
    raw_data_15_with_k12['AccountManagerCode'] = raw_data_15_with_k12['AccountManagerCode'].map(lambda x: x[x.find("(") + 1 : x.find(")")])
    
    # for 16 raw data
    raw_data_16_index = ['Applicant', 'BEN', 'State', 'Discpct', 'SvcProvider', 'ProductType', 'EquipMake', 'GrandTotalPreDisc (SUM)', 'Request (SUM)']
    raw_data_16 = pd.read_excel('FFL FY2016 DOS 2016-08-07NT C2 Data Only.xlsx', usecols = raw_data_16_index)
    raw_data_16 = raw_data_16.rename(columns = {'SvcProvider' : 'ServiceProvider', 'EquipMake' : 'Brand', 'GrandTotalPreDisc (SUM)' : 'TotalSpend', 'Request (SUM)' : 'RequestAmt'})
    raw_data_16_with_k12 = pd.merge(raw_data_16, k12_data, how = 'left', on = 'BEN')
    # drop those rows which without customercode and Primary AM
    raw_data_16_with_k12 = raw_data_16_with_k12[pd.notnull(raw_data_16_with_k12['CustomerCode'])]
    raw_data_16_with_k12['AccountManagerCode'] = raw_data_16_with_k12['PAM']
    raw_data_16_with_k12['AccountManagerCode'] = raw_data_16_with_k12['AccountManagerCode'].map(lambda x: x[x.find("(") + 1 : x.find(")")])

    # for 17 raw data
    raw_data_17_index = ['applicant_name', 'BEN', 'applicant_state', 'discount_rate', 'service_provider', 'product_type', 'manufacturer', 'total_cost', 'request']
    raw_data_17 = pd.read_excel('Copy of Just Data FY17 Final.xlsx', skiprows = 2, usecols = raw_data_17_index)
    raw_data_17 = raw_data_17.rename(columns = {'applicant_name' : 'Applicant', 'applicant_state' : 'State', 'discount_rate' : 'Discpct', 'service_provider' : 'ServiceProvider', 'product_type' : 'ProductType', 'manufacturer' : 'Brand', 'total_cost' : 'TotalSpend', 'request' : 'RequestAmt'})
    raw_data_17 = raw_data_17.fillna(0)
    # select the manufacturers and replace the whitespace with other
    raw_data_17.Brand.replace(to_replace = 0, value = 'Other', inplace = True)
    raw_data_17_with_k12 = pd.merge(raw_data_17, k12_data, how = 'left', on = 'BEN')
    # drop those rows which without customercode and Primary AM
    raw_data_17_with_k12 = raw_data_17_with_k12[pd.notnull(raw_data_17_with_k12['CustomerCode'])]
    raw_data_17_with_k12['AccountManagerCode'] = raw_data_17_with_k12['PAM']
    raw_data_17_with_k12['AccountManagerCode'] = raw_data_17_with_k12['AccountManagerCode'].map(lambda x: x[x.find("(") + 1 : x.find(")")])
    
    return raw_data_15_with_k12, raw_data_16_with_k12, raw_data_17_with_k12
    
# step 2 : Generate customerseq from customercode, EmailAddress from Primary AM
def info_appending(raw_data_15_with_k12, raw_data_16_with_k12, raw_data_17_with_k12):
    customerseq_code = pd.read_csv('customerseq.txt', sep = '\t', converters = {'CustomerCode': lambda x: str(x)})
    am_emailaddress = pd.read_csv('accountmanager_emailaddress.csv', sep = '\t')
    
    # for 15 data with customerseq and Primary AM
    raw_data_15_with_k12_seq = pd.merge(raw_data_15_with_k12, customerseq_code, how = 'left', on = 'CustomerCode')
    raw_data_15_with_k12_seq = raw_data_15_with_k12_seq[pd.notnull(raw_data_15_with_k12_seq['CustomerSeq'])]
    raw_data_15_with_k12_seq_email = pd.merge(raw_data_15_with_k12_seq, am_emailaddress, how = 'left', on = 'AccountManagerCode')
    raw_data_15_with_k12_seq_email = raw_data_15_with_k12_seq_email[pd.notnull(raw_data_15_with_k12_seq_email['EMailAddress'])]
    raw_data_15_with_k12_seq_email['OrderYear'] = 2015
    
    # for 16 data with customerseq and Primary AM
    raw_data_16_with_k12_seq = pd.merge(raw_data_16_with_k12, customerseq_code, how = 'left', on = 'CustomerCode')
    raw_data_16_with_k12_seq = raw_data_16_with_k12_seq[pd.notnull(raw_data_16_with_k12_seq['CustomerSeq'])]
    raw_data_16_with_k12_seq_email = pd.merge(raw_data_16_with_k12_seq, am_emailaddress, how = 'left', on = 'AccountManagerCode')
    raw_data_16_with_k12_seq_email = raw_data_16_with_k12_seq_email[pd.notnull(raw_data_16_with_k12_seq_email['EMailAddress'])]
    raw_data_16_with_k12_seq_email['OrderYear'] = 2016
    
    # for 17 data with customerseq and Primary AM
    raw_data_17_with_k12_seq = pd.merge(raw_data_17_with_k12, customerseq_code, how = 'left', on = 'CustomerCode')
    raw_data_17_with_k12_seq = raw_data_17_with_k12_seq[pd.notnull(raw_data_17_with_k12_seq['CustomerSeq'])]
    raw_data_17_with_k12_seq_email = pd.merge(raw_data_17_with_k12_seq, am_emailaddress, how = 'left', on = 'AccountManagerCode')
    raw_data_17_with_k12_seq_email = raw_data_17_with_k12_seq_email[pd.notnull(raw_data_17_with_k12_seq_email['EMailAddress'])]
    raw_data_17_with_k12_seq_email['OrderYear'] = 2017
    
    # print(raw_data_17_with_k12_seq_email)
    # step 3 : Generate num of orders and num of quotes from customerseq
    num_of_orders = pd.read_csv('order_num_and_dollars.csv')
    num_of_quotes = pd.read_csv('number_Quotes.csv')
    num_of_T12M = pd.read_csv('T12M_CustomerSeq_unique.csv', sep = ',', converters = {'CustomerCode': lambda x: str(x)})
    # for 15 data with customercode, Primary AM, customerseq, AM emailaddress
    raw_data_15_final = pd.merge(raw_data_15_with_k12_seq_email, num_of_orders, how = 'left', on = ['CustomerSeq', 'OrderYear'])
    raw_data_15_final = pd.merge(raw_data_15_final, num_of_quotes, how = 'left', on = ['CustomerSeq', 'OrderYear'])
    raw_data_15_final = pd.merge(raw_data_15_final, num_of_T12M, how = 'left', on = ['CustomerSeq'])
    
    raw_data_15_final = raw_data_15_final.fillna(0)
    raw_data_15_final.ProductType.replace(to_replace = 0, value = 'Unknown', inplace = True)
    raw_data_15_final.Brand.replace(to_replace = 0, value = 'Unknown', inplace = True)
    
    # for 16 data with customercode, Primary AM, customerseq, AM emailaddress
    raw_data_16_final = pd.merge(raw_data_16_with_k12_seq_email, num_of_orders, how = 'left', on = ['CustomerSeq', 'OrderYear'])
    raw_data_16_final = pd.merge(raw_data_16_final, num_of_quotes, how = 'left', on = ['CustomerSeq', 'OrderYear'])
    raw_data_16_final = pd.merge(raw_data_16_final, num_of_T12M, how = 'left', on = ['CustomerSeq'])
    raw_data_16_final = raw_data_16_final.fillna(0)
    raw_data_16_final.ProductType.replace(to_replace = 0, value = 'Unknown', inplace = True)
    raw_data_16_final.Brand.replace(to_replace = 0, value = 'Unknown', inplace = True)
    
    # for 17 data with customercode, Primary AM, customerseq, AM emailaddress
    raw_data_17_final = pd.merge(raw_data_17_with_k12_seq_email, num_of_orders, how = 'left', on = ['CustomerSeq', 'OrderYear'])
    raw_data_17_final = pd.merge(raw_data_17_final, num_of_quotes, how = 'left', on = ['CustomerSeq', 'OrderYear'])
    raw_data_17_final = pd.merge(raw_data_17_final, num_of_T12M, how = 'left', on = ['CustomerSeq'])
    raw_data_17_final = raw_data_17_final.fillna(0)
    raw_data_17_final.ProductType.replace(to_replace = 0, value = 'Unknown', inplace = True)
    raw_data_17_final.Brand.replace(to_replace = 0, value = 'Unknown', inplace = True)
    
    # raw_data_17_final = raw_data_17_final[pd.notnull(raw_data_17_final['TotalDollars'])]
    raw_data_three_years = raw_data_15_final.append([raw_data_16_final, raw_data_17_final], ignore_index = True)
    
    return raw_data_three_years

# make the total spend table for each year:
# Year, TotalCost, RequestCost, % of $ w/CDW, % of $ with top competitor, Top Category Purchased
def total_spend_table(df, year):    
    total_spend = df[:][df.OrderYear == year]
    CDW_total = (total_spend['TotalSpend'][total_spend.ServiceProvider == 'CDW Government LLC']).sum()
    
    # find the '$ of Top competitor'
    UniqueServiceProvider = total_spend.ServiceProvider.unique()
    DataFrameDict = {elem : pd.DataFrame for elem in UniqueServiceProvider}
    maximum = 0
    max_company = ''
    for key in DataFrameDict.keys():
        DataFrameDict[key] = (total_spend['TotalSpend'][total_spend.ServiceProvider == key]).sum()
        if DataFrameDict[key] > maximum and key != 'CDW Government LLC':
            max_company = key
            maximum = DataFrameDict[key]    
    # find the 'Top Category Purchased'
    UniqueProductType = total_spend.ProductType.unique()
    DataFrameDict_product = {elem : pd.DataFrame for elem in UniqueProductType}
    max_category_name = ''
    max_category = 0
    for key in DataFrameDict_product.keys():
        DataFrameDict_product[key] = (total_spend['TotalSpend'][total_spend.ProductType == key]).sum()
        if DataFrameDict_product[key] > max_category:
            max_category = DataFrameDict_product[key]
            max_category_name = key
    total_table = []
    if not total_spend.empty:
        total_table = [[year, total_spend['TotalSpend'].sum(), total_spend['RequestAmt'].sum(), (CDW_total + 1) / (total_spend['TotalSpend'].sum() + 1), (maximum + 1) / (total_spend['TotalSpend'].sum() + 1), max_category_name]]
#     total_table = pd.DataFrame(total_table, index = [year], columns = ['Year', 'TotalCost', 'RequestCost', '% of $ w/CDW', '% of $ with top competitor', 'Top category purchased'])    
    return total_table

# create the company table
def create_company_table(df, year):
    each_year_dataframe = df[:][df.OrderYear == year][df.ServiceProvider != 'CDW Government LLC']
    # obtain the product list for this customer
    UniqueProductType = df.ProductType.unique()
    # find the 1st, 2nd, 3rd competitor in this year
    UniqueServiceProvider = df.ServiceProvider.unique()
    DataFrameDict_Provider = {elem : pd.DataFrame for elem in UniqueServiceProvider}
    
    max_1_name = ''
    max_2_name = ''
    max_3_name = ''
    
    max_1_current_year = 0
    max_2_current_year = 0
    max_3_current_year = 0
    for key in DataFrameDict_Provider.keys():
        DataFrameDict_Provider[key] = (df['TotalSpend'][df.ServiceProvider == key]).sum()
        # compare the three years total cost, but record just current year
        if (DataFrameDict_Provider[key] > max_1) and (key in each_year_dataframe.ServiceProvider.unique()):
            max_3 = max_2
            max_3_current_year = max_2_current_year
            max_3_name = max_2_name
            max_2 = max_1
            max_2_current_year = max_1_current_year
            max_2_name = max_1_name
            max_1 = DataFrameDict_Provider[key]
            max_1_current_year = (each_year_dataframe['TotalSpend'][each_year_dataframe.ServiceProvider == key]).sum()
            max_1_name = key
        elif (DataFrameDict_Provider[key] > max_2) and (key in each_year_dataframe.ServiceProvider.unique()):
            max_3 = max_2
            max_3_current_year = max_2_current_year
            max_3_name = max_2_name
            max_2 = DataFrameDict_Provider[key]
            max_2_current_year = (each_year_dataframe['TotalSpend'][each_year_dataframe.ServiceProvider == key]).sum()
            max_2_name = key
        elif (DataFrameDict_Provider[key] > max_3) and (key in each_year_dataframe.ServiceProvider.unique()):
            max_3 = DataFrameDict_Provider[key]
            max_3_current_year = (each_year_dataframe['TotalSpend'][each_year_dataframe.ServiceProvider == key]).sum()
            max_3_name = key

    # title for the company table
    dp = list(UniqueProductType)
    company_1 = []
    company_2 = []
    company_3 = []
    cdw = []
    top_3_company_list = [max_1_name, max_2_name, max_3_name]
    top_3_company_list_not_null = [name for name in top_3_company_list if name]
    # obtain the value of each item for each competitor
    for item_c in top_3_company_list_not_null:
        for item_p in dp:
            if item_c == max_1_name:
                value = (each_year_dataframe['TotalSpend'][each_year_dataframe.ProductType == item_p][each_year_dataframe.ServiceProvider == item_c]).sum()
                if value == 0:                    
                    company_1.append('')
                else:
                    company_1.append(value)
            elif item_c == max_2_name:
                value = (each_year_dataframe['TotalSpend'][each_year_dataframe.ProductType == item_p][each_year_dataframe.ServiceProvider == item_c]).sum()
                if value == 0:
                    company_2.append('')
                else:
                    company_2.append(value)
            elif item_c == max_3_name:
                value = (each_year_dataframe['TotalSpend'][each_year_dataframe.ProductType == item_p][each_year_dataframe.ServiceProvider == item_c]).sum()
                if value == 0:                    
                    company_3.append('')
                else:
                    company_3.append(value)

    for item_p in dp:
        value = (df['TotalSpend'][df.OrderYear == year][df.ProductType == item_p][df.ServiceProvider == 'CDW Government LLC']).sum()
        if value == 0:
            cdw.append('')
        else:
            cdw.append(value)

    # make the three list same length
    if not company_1:
        company_1 += [''] * len(dp)
    if not company_2:
        company_2 += [''] * len(dp)
    if not company_3:
        company_3 += [''] * len(dp)
        
    if max_1_current_year == 0:
        company_1 = [max_1_name, ''] + company_1
    else:
        company_1 = [max_1_name, max_1_current_year] + company_1
    if max_2_current_year == 0:
        company_2 = [max_2_name, ''] + company_2
    else:
        company_2 = [max_2_name, max_2_current_year] + company_2
    if max_3_current_year == 0:
        company_3 = [max_3_name, ''] + company_3
    else:
        company_3 = [max_3_name, max_3_current_year] + company_3
    
    cdw_max = (df['TotalSpend'][df.OrderYear == year][df.ServiceProvider == 'CDW Government LLC']).sum()

    if cdw_max == 0:
        cdw = ['CDW-G', ''] + cdw
    else:
        cdw = ['CDW-G', cdw_max] + cdw
    
    dp = ['ServiceProvider'] + ['TotalSpend'] + dp
    
    # 1 -> cdw, 2 -> cdw + company_1, 3 -> cdw + company_1 + company_2, 4 -> cdw + company_1 + company_2 + company_3
    flag_2 = 0
    final_dataframe = pd.DataFrame()
    if max_1_current_year == 0:
        company_total = [cdw]
        final_dataframe = pd.DataFrame(company_total, index = [year], columns = dp)
        final_dataframe = final_dataframe[dp]
        flag_2 = 1
    elif max_2_current_year == 0:
        company_total = [cdw, company_1]
        final_dataframe = pd.DataFrame(company_total, index = [year, year], columns = dp)
        final_dataframe = final_dataframe[dp]
        flag_2 = 2
    elif max_3_current_year == 0:
        company_total = [cdw, company_1, company_2]
        final_dataframe = pd.DataFrame(company_total, index = [year, year, year], columns = dp)
        final_dataframe = final_dataframe[dp]
        flag_2 = 3
    else:
        company_total = [cdw, company_1, company_2, company_3]
        final_dataframe = pd.DataFrame(company_total, index = [year, year, year, year], columns = dp)
        final_dataframe = final_dataframe[dp]
        flag_2 = 4
    
    return final_dataframe, flag_2

# create the company table
def create_brand_table(df, year):
    each_year_dataframe = df[:][df.OrderYear == year]
    # obtain the product list for this customer
    UniqueProductType = df.ProductType.unique()
    # find the 1st, 2nd, 3rd competitor in this year
    UniqueBrand = each_year_dataframe.Brand.unique()
    DataFrameDict_Brand = {elem : pd.DataFrame for elem in UniqueBrand}

    # dp = list(UniqueProductType)
    brands = []
    for br in UniqueBrand:
        brand = []
        total_current_year = (each_year_dataframe['TotalSpend'][each_year_dataframe.Brand == br]).sum()
        # append the brand name
        brand.append(br)
        # append the total value of this brand in one year
        if total_current_year == 0:
            brand.append('')
        else:
            brand.append(total_current_year)
        for item_p in dp:
            value = (each_year_dataframe['TotalSpend'][each_year_dataframe.ProductType == item_p][each_year_dataframe.Brand == br]).sum()
            if value == 0:
                brand.append('')
            else:
                brand.append(value)
        brands.append(copy.deepcopy(brand))
    
    dp = ['Brand'] + ['TotalSpend'] + dp
    final_dataframe = pd.DataFrame(brands, index = [year] * len(brands), columns = dp)
    
    return final_dataframe, UniqueBrand, len(brands) - 1

# all summary sheet table 
def make_summary_sheet_table(writer, workbook, detail_dataframe, worksheet_summary_name, num_of_product_type):
    # for the total spend and brand table in 2015
    total_spend_for_15 = total_spend_table(detail_dataframe, 2015)
    brand_for_15, brand_name_15, numbrand_15 = create_brand_table(detail_dataframe, 2015)
    
    # for the total spend and brand table in 2016
    total_spend_for_16 = total_spend_table(detail_dataframe, 2016)
    brand_for_16, brand_name_16, numbrand_16 = create_brand_table(detail_dataframe, 2016)
    
    # for the total spend and brand table in 2017
    total_spend_for_17 = total_spend_table(detail_dataframe, 2017)
    brand_for_17, brand_name_17, numbrand_17 = create_brand_table(detail_dataframe, 2017)
    
    # 1 -> 15, 16, 17; 2 -> 15, 16; 3 -> 15; 4 -> 15, 17; 5 -> 17; 6 -> 16; 7 -> 16, 17; 0 -> null
    flag = 0
    brand_location_three_years = []
    brand_name = []
    num_brand_location_three_years = []
#     total_spend_three_years = pd.DataFrame()
    # union three together and write into file
    if total_spend_for_15 and total_spend_for_16 and total_spend_for_17:
        total_spend_for_15 = pd.DataFrame(total_spend_for_15, index = [2015], columns = ['Year', 'TotalSpend', 'RequestCost', '% of $ w/CDW', '% of $ with top competitor', 'Top category purchased'])
        total_spend_for_16 = pd.DataFrame(total_spend_for_16, index = [2016], columns = ['Year', 'TotalSpend', 'RequestCost', '% of $ w/CDW', '% of $ with top competitor', 'Top category purchased'])
        total_spend_for_17 = pd.DataFrame(total_spend_for_17, index = [2017], columns = ['Year', 'TotalSpend', 'RequestCost', '% of $ w/CDW', '% of $ with top competitor', 'Top category purchased'])
        total_spend_three_years = total_spend_for_15.append([total_spend_for_16, total_spend_for_17], ignore_index = True)
        total_spend_three_years.to_excel(writer, sheet_name = worksheet_summary_name, startrow = 68, startcol = 0, index = False)
        
        brand_three_years = brand_for_15.append([brand_for_16, brand_for_17])
        brand_three_years.to_excel(writer, sheet_name = worksheet_summary_name, startrow = 73, startcol = 0)
        
        brand_location_three_years.append("='" + worksheet_summary_name + "'!$D$75" + ":$" + str(chr(ord('D') + num_of_product_type)) + "$" + str(numbrand_15 + 75))
        start_16 = numbrand_15 + 75 + 1
        brand_location_three_years.append("='" + worksheet_summary_name + "'!$D$" + str(start_16) + ":$" + str(chr(ord('D') + num_of_product_type)) + "$" + str(start_16 + numbrand_16))
        start_17 = start_16 + numbrand_16 + 1
        brand_location_three_years.append("='" + worksheet_summary_name + "'!$D$" + str(start_17) + ":$" + str(chr(ord('D') + num_of_product_type)) + "$" + str(start_17 + numbrand_17))
        
        num_brand_location_three_years.append(numbrand_15)
        num_brand_location_three_years.append(numbrand_16)
        num_brand_location_three_years.append(numbrand_17)
        
        brand_name.append(brand_name_15)
        brand_name.append(brand_name_16)
        brand_name.append(brand_name_17)
        flag = 1
#         print(total_spend_three_years)
    elif total_spend_for_15 and total_spend_for_16 and not total_spend_for_17:
        total_spend_for_15 = pd.DataFrame(total_spend_for_15, index = [2015], columns = ['Year', 'TotalSpend', 'RequestCost', '% of $ w/CDW', '% of $ with top competitor', 'Top category purchased'])
        total_spend_for_16 = pd.DataFrame(total_spend_for_16, index = [2016], columns = ['Year', 'TotalSpend', 'RequestCost', '% of $ w/CDW', '% of $ with top competitor', 'Top category purchased'])
        total_spend_three_years = total_spend_for_15.append([total_spend_for_16], ignore_index = True)
        total_spend_three_years.to_excel(writer, sheet_name = worksheet_summary_name, startrow = 64, startcol = 0, index = False)
        
        brand_three_years = brand_for_15.append([brand_for_16])
        brand_three_years.to_excel(writer, sheet_name = worksheet_summary_name, startrow = 68, startcol = 0)
        
        brand_location_three_years.append("='" + worksheet_summary_name + "'!$D$70" + ":$" + str(chr(ord('D') + num_of_product_type)) + "$" + str(numbrand_15 + 70))
        start_16 = numbrand_15 + 70 + 1
        brand_location_three_years.append("='" + worksheet_summary_name + "'!$D$" + str(start_16) + ":$" + str(chr(ord('D') + num_of_product_type)) + "$" + str(start_16 + numbrand_16))
        
        num_brand_location_three_years.append(numbrand_15)
        num_brand_location_three_years.append(numbrand_16)
        
        brand_name.append(brand_name_15)
        brand_name.append(brand_name_16)
        flag = 2
#         print(total_spend_three_years)
    elif total_spend_for_15 and not total_spend_for_16 and not total_spend_for_17:
        total_spend_for_15 = pd.DataFrame(total_spend_for_15, index = [2015], columns = ['Year', 'TotalSpend', 'RequestCost', '% of $ w/CDW', '% of $ with top competitor', 'Top category purchased'])
#         total_spend_three_years = total_spend_for_15.append([total_spend_for_16, total_spend_for_17], ignore_index = True)
        total_spend_for_15.to_excel(writer, sheet_name = worksheet_summary_name, startrow = 60, startcol = 0, index = False)
        
        brand_for_15.to_excel(writer, sheet_name = worksheet_summary_name, startrow = 64, startcol = 0)
        
        brand_location_three_years.append("='" + worksheet_summary_name + "'!$D$66" + ":$" + str(chr(ord('D') + num_of_product_type)) + "$" + str(numbrand_15 + 66))
        
        num_brand_location_three_years.append(numbrand_15)
        
        brand_name.append(brand_name_15)
        flag = 3
#         print(total_spend_for_15)
    elif total_spend_for_15 and not total_spend_for_16 and total_spend_for_17:
        total_spend_for_15 = pd.DataFrame(total_spend_for_15, index = [2015], columns = ['Year', 'TotalSpend', 'RequestCost', '% of $ w/CDW', '% of $ with top competitor', 'Top category purchased'])
        total_spend_for_17 = pd.DataFrame(total_spend_for_17, index = [2017], columns = ['Year', 'TotalSpend', 'RequestCost', '% of $ w/CDW', '% of $ with top competitor', 'Top category purchased'])
        total_spend_three_years = total_spend_for_15.append([total_spend_for_17], ignore_index = True)
        total_spend_three_years.to_excel(writer, sheet_name = worksheet_summary_name, startrow = 64, startcol = 0, index = False)
        
        brand_three_years = brand_for_15.append([brand_for_17])
        brand_three_years.to_excel(writer, sheet_name = worksheet_summary_name, startrow = 68, startcol = 0)
        
        brand_location_three_years.append("='" + worksheet_summary_name + "'!$D$70" + ":$" + str(chr(ord('D') + num_of_product_type)) + "$" + str(numbrand_15 + 70))
        start_17 = numbrand_15 + 70 + 1
        brand_location_three_years.append("='" + worksheet_summary_name + "'!$D$" + str(start_17) + ":$" + str(chr(ord('D') + num_of_product_type)) + "$" + str(start_17 + numbrand_17))
        
        num_brand_location_three_years.append(numbrand_15)
        num_brand_location_three_years.append(numbrand_17)
        
        brand_name.append(brand_name_15)
        brand_name.append(brand_name_17)
        flag = 4
#         print(total_spend_three_years)
    elif not total_spend_for_15 and not total_spend_for_16 and total_spend_for_17:
        total_spend_for_17 = pd.DataFrame(total_spend_for_17, index = [2017], columns = ['Year', 'TotalSpend', 'RequestCost', '% of $ w/CDW', '% of $ with top competitor', 'Top category purchased'])
#         total_spend_three_years = total_spend_for_15.append([total_spend_for_16, total_spend_for_17], ignore_index = True)
        total_spend_for_17.to_excel(writer, sheet_name = worksheet_summary_name, startrow = 60, startcol = 0, index = False)
        
        brand_for_17.to_excel(writer, sheet_name = worksheet_summary_name, startrow = 64, startcol = 0)
        
        brand_location_three_years.append("='" + worksheet_summary_name + "'!$D$66" + ":$" + str(chr(ord('D') + num_of_product_type)) + "$" + str(numbrand_17 + 66))
        
        num_brand_location_three_years.append(numbrand_17)
        
        brand_name.append(brand_name_17)
        flag = 5
#         print(total_spend_for_17)
    elif not total_spend_for_15 and total_spend_for_16 and not total_spend_for_17:
        total_spend_for_16 = pd.DataFrame(total_spend_for_16, index = [2016], columns = ['Year', 'TotalSpend', 'RequestCost', '% of $ w/CDW', '% of $ with top competitor', 'Top category purchased'])
#         total_spend_three_years = total_spend_for_15.append([total_spend_for_16, total_spend_for_17], ignore_index = True)
        total_spend_for_16.to_excel(writer, sheet_name = worksheet_summary_name, startrow = 60, startcol = 0, index = False)
        
        brand_for_16.to_excel(writer, sheet_name = worksheet_summary_name, startrow = 64, startcol = 0)
        
        brand_location_three_years.append("='" + worksheet_summary_name + "'!$D$66" + ":$" + str(chr(ord('D') + num_of_product_type)) + "$" + str(numbrand_16 + 66))
        
        num_brand_location_three_years.append(numbrand_16)

        brand_name.append(brand_name_16)
        flag = 6
#         print(total_spend_for_16)
    elif not total_spend_for_15 and total_spend_for_16 and total_spend_for_17:
        total_spend_for_16 = pd.DataFrame(total_spend_for_16, index = [2016], columns = ['Year', 'TotalSpend', 'RequestCost', '% of $ w/CDW', '% of $ with top competitor', 'Top category purchased'])
        total_spend_for_17 = pd.DataFrame(total_spend_for_17, index = [2017], columns = ['Year', 'TotalSpend', 'RequestCost', '% of $ w/CDW', '% of $ with top competitor', 'Top category purchased'])
        total_spend_three_years = total_spend_for_16.append([total_spend_for_17], ignore_index = True)
        total_spend_three_years.to_excel(writer, sheet_name = worksheet_summary_name, startrow = 64, startcol = 0, index = False)
        
        brand_three_years = brand_for_16.append([brand_for_17])
        brand_three_years.to_excel(writer, sheet_name = worksheet_summary_name, startrow = 68, startcol = 0)
        
        brand_location_three_years.append("='" + worksheet_summary_name + "'!$D$70" + ":$" + str(chr(ord('D') + num_of_product_type)) + "$" + str(numbrand_16 + 70))
        start_17 = numbrand_16 + 70 + 1
        brand_location_three_years.append("='" + worksheet_summary_name + "'!$D$" + str(start_17) + ":$" + str(chr(ord('D') + num_of_product_type)) + "$" + str(start_17 + numbrand_17))
        
        num_brand_location_three_years.append(numbrand_16)
        num_brand_location_three_years.append(numbrand_17)
        
        brand_name.append(brand_name_16)
        brand_name.append(brand_name_17)
        flag = 7
#         print(total_spend_three_years)
    else:
        pass
    
#     total_spend_three_years.to_excel(writer, sheet_name = worksheet_summary_name, startrow = 34, startcol = 14, index = False)
    # 1 -> cdw, 2 -> cdw + company_1, 3 -> cdw + company_1 + company_2, 4 -> cdw + company_1 + company_2 + company_3
    flag_2 = []
    if flag == 1:
        company_table_15, flag_15 = create_company_table(detail_dataframe, 2015)
        company_table_16, flag_16 = create_company_table(detail_dataframe, 2016)
        company_table_17, flag_17 = create_company_table(detail_dataframe, 2017)    
        company_table_for_three_years = company_table_15.append([company_table_16, company_table_17])
        company_table_for_three_years.to_excel(writer, sheet_name = worksheet_summary_name, startrow = 54, startcol = 0)
        flag_2 = [flag_15, flag_16, flag_17]
    elif flag == 2:
        company_table_15, flag_15 = create_company_table(detail_dataframe, 2015)
        company_table_16, flag_16 = create_company_table(detail_dataframe, 2016)
        company_table_for_three_years = company_table_15.append([company_table_16])
        company_table_for_three_years.to_excel(writer, sheet_name = worksheet_summary_name, startrow = 54, startcol = 0)
        flag_2 = [flag_15, flag_16]
    elif flag == 3:
        company_table_15, flag_15 = create_company_table(detail_dataframe, 2015)
#         company_table_for_three_years = company_table_15.append([company_table_16])
        company_table_15.to_excel(writer, sheet_name = worksheet_summary_name, startrow = 54, startcol = 0)
        flag_2 = [flag_15]
    elif flag == 4:
        company_table_15, flag_15 = create_company_table(detail_dataframe, 2015)
        company_table_17, flag_17 = create_company_table(detail_dataframe, 2017)
        company_table_for_three_years = company_table_15.append([company_table_17])
        company_table_for_three_years.to_excel(writer, sheet_name = worksheet_summary_name, startrow = 54, startcol = 0)
        flag_2 = [flag_15, flag_17]
    elif flag == 5:
        company_table_17, flag_17 = create_company_table(detail_dataframe, 2017)
#         company_table_16 = create_company_table(detail_dataframe, 2016)
#         company_table_for_three_years = company_table_15.append([company_table_16])
        company_table_17.to_excel(writer, sheet_name = worksheet_summary_name, startrow = 54, startcol = 0)
        flag_2 = [flag_17]
    elif flag == 6:
#         company_table_15 = create_company_table(detail_dataframe, 2015)
        company_table_16, flag_16 = create_company_table(detail_dataframe, 2016)
#         company_table_for_three_years = company_table_15.append([company_table_16])
        company_table_16.to_excel(writer, sheet_name = worksheet_summary_name, startrow = 54, startcol = 0)
        flag_2 = [flag_16]
    elif flag == 7:
        company_table_16, flag_16 = create_company_table(detail_dataframe, 2016)
        company_table_17, flag_17 = create_company_table(detail_dataframe, 2017)
        company_table_for_three_years = company_table_16.append([company_table_17])
        company_table_for_three_years.to_excel(writer, sheet_name = worksheet_summary_name, startrow = 54, startcol = 0)
        flag_2 = [flag_16, flag_17]
    else:
        pass
    
    # for the titleTotalSpend
#     locale.setlocale(locale.LC_ALL, '')
    format_header_1 = workbook.add_format({'bold': False, 'bg_color' : 'white', 'font_color': 'green', 'font_size' : 25, 'align': 'center', 'valign': 'vcenter', 'right' : 1})
    format_header_2 = workbook.add_format({'bold': False, 'bg_color' : 'white', 'font_color': 'green', 'font_size' : 25, 'align': 'center', 'valign': 'vcenter', 'bottom' : 1, 'right' : 1})
    str_append = 'DisPct: ' + str(detail_dataframe.iloc[0]['Discpct']) + '  ' + 'T12M CDW Spend: $' + str('{:0,.0f}'.format(detail_dataframe['T12OrderDollarAmount'].sum() / detail_dataframe['T12OrderDollarAmount'].count())) + '  ' + 'T12M #Orders: ' + str(int(detail_dataframe['T12OrderCount'].sum() / detail_dataframe['T12OrderCount'].count())) + '  ' + 'T12M #Quotes: ' + str(int(detail_dataframe['T12QuoteCount'].sum() / detail_dataframe['T12QuoteCount'].count()))
    enrollment_budget_remain = int(detail_dataframe['Enrollment'].sum() / detail_dataframe['Enrollment'].count()) * 153.47 - int(detail_dataframe['TotalSpend'].sum())
    if enrollment_budget_remain < 0:
        enrollment_budget_remain = 0
    str_append_for_erate_left = 'Remaining Erate Budget: $' + str('{:0,.0f}'.format(enrollment_budget_remain))
    worksheet_summary = workbook.get_worksheet_by_name(worksheet_summary_name)
#     print(type(worksheet_summary))
    worksheet_summary.merge_range('A5:T6', str_append_for_erate_left, format_header_2)
    worksheet_summary.merge_range('A3:T4', str_append, format_header_1)
    worksheet_summary.merge_range('A1:T2', str(detail_dataframe.iloc[0]['Applicant']) + '(' + str(detail_dataframe.iloc[0]['BEN']) + ')' + ' ' + str(detail_dataframe.iloc[0]['State']), format_header_1)
#     print(flag)
    return flag, flag_2, brand_location_three_years, brand_name, num_brand_location_three_years

def read_data_35_38(workbook_read, num_of_product_type, worksheet_summary_name, flag_2):
    # flag: 1 -> 15, 16, 17; 2 -> 15, 16; 3 -> 15; 4 -> 15, 17; 5 -> 17; 6 -> 16; 7 -> 16, 17; 0 -> null
    # flag_2: 1 -> cdw, 2 -> cdw + company_1, 3 -> cdw + company_1 + company_2, 4 -> cdw + company_1 + company_2 + company_3
#     workbook_read = xlrd.open_workbook(filename)
    worksheet_read = workbook_read.sheet_by_name(worksheet_summary_name)
    location_for_label_15 = ''
    location_for_value_15 = ''
    location_for_bar_chart_15 = ''
    company_name_for_15 = []
    
    loc_indx = 0
    
    if flag_2 == 1:
#         if cdw_15_value != '':
        location_for_label_15 = "='" + worksheet_summary_name + "'!$B$56:$B$56"
        location_for_value_15 = "='" + worksheet_summary_name + "'!$C$56:$C$56"
        location_for_bar_chart_15 = "='" + worksheet_summary_name + "'!$D$56:$" + str(chr(ord('D') + num_of_product_type)) + "$56"
        company_name_for_15.append("='" + worksheet_summary_name + "'!$B$56:$B$56")
        loc_indx = 57
    elif flag_2 == 2:
#         if cdw_15_value != '':
        location_for_label_15 = "='" + worksheet_summary_name + "'!$B$56:$B$57"
        location_for_value_15 = "='" + worksheet_summary_name + "'!$C$56:$C$57"
        location_for_bar_chart_15 = "='" + worksheet_summary_name + "'!$D$56:$" + str(chr(ord('D') + num_of_product_type)) + "$57"
        company_name_for_15.append("='" + worksheet_summary_name + "'!$B$56:$B$56")
        company_name_for_15.append("='" + worksheet_summary_name + "'!$B$57:$B$57")

        loc_indx = 58
    elif flag_2 == 3:
#         if cdw_15_value != '':
        location_for_label_15 = "='" + worksheet_summary_name + "'!$B$56:$B$58"
        location_for_value_15 = "='" + worksheet_summary_name + "'!$C$56:$C$58"
        location_for_bar_chart_15 = "='" + worksheet_summary_name + "'!$D$56:$" + str(chr(ord('D') + num_of_product_type)) + "$58"
        company_name_for_15.append("='" + worksheet_summary_name + "'!$B$56:$B$56")
        company_name_for_15.append("='" + worksheet_summary_name + "'!$B$57:$B$57")
        company_name_for_15.append("='" + worksheet_summary_name + "'!$B$58:$B$58")

        loc_indx = 59
    elif flag_2 == 4:
#         if cdw_15_value != '':
        location_for_label_15 = "='" + worksheet_summary_name + "'!$B$56:$B$59"
        location_for_value_15 = "='" + worksheet_summary_name + "'!$C$56:$C$59"
        location_for_bar_chart_15 = "='" + worksheet_summary_name + "'!$D$56:$" + str(chr(ord('D') + num_of_product_type)) + "$59"
        company_name_for_15.append("='" + worksheet_summary_name + "'!$B$56:$B$56")
        company_name_for_15.append("='" + worksheet_summary_name + "'!$B$57:$B$57")
        company_name_for_15.append("='" + worksheet_summary_name + "'!$B$58:$B$58")
        company_name_for_15.append("='" + worksheet_summary_name + "'!$B$59:$B$59")

        loc_indx = 60
    else:
        pass
    
    return location_for_label_15, location_for_value_15, location_for_bar_chart_15, company_name_for_15, loc_indx

def read_data_39_42(workbook_read, num_of_product_type, worksheet_summary_name, flag_2, loc_indx):
    # flag: 1 -> 15, 16, 17; 2 -> 15, 16; 3 -> 15; 4 -> 15, 17; 5 -> 17; 6 -> 16; 7 -> 16, 17; 0 -> null
    # flag-2: 1 -> cdw, 2 -> cdw + company_1, 3 -> cdw + company_1 + company_2, 4 -> cdw + company_1 + company_2 + company_3

    worksheet_read = workbook_read.sheet_by_name(worksheet_summary_name)
    location_for_label_16 = ''
    location_for_value_16 = ''
    location_for_bar_chart_16 = ''
    company_name_for_16 = []
    loc_indx_16 = 0
    
    cdw_16 = ''
    cdw_16_value = 0
    
    if flag_2 == 1:
        location_for_label_16 = "='" + worksheet_summary_name + "'!$B$" + str(loc_indx) + ":$B$" + str(loc_indx)
        location_for_value_16 = "='" + worksheet_summary_name + "'!$C$" + str(loc_indx) + ":$C$" + str(loc_indx)
        location_for_bar_chart_16 = "='" + worksheet_summary_name + "'!$D$" + str(loc_indx) + ":$" + str(chr(ord('D') + num_of_product_type)) + "$" + str(loc_indx)
        company_name_for_16.append("='" + worksheet_summary_name + "'!$B$" + str(loc_indx) + ":$B$" + str(loc_indx))
        loc_indx_16 = loc_indx + 1
    elif flag_2 == 2:
        location_for_label_16 = "='" + worksheet_summary_name + "'!$B$" + str(loc_indx) + ":$B$" + str(loc_indx + 1)
        location_for_value_16 = "='" + worksheet_summary_name + "'!$C$" + str(loc_indx) + ":$C$" + str(loc_indx + 1)
        location_for_bar_chart_16 = "='" + worksheet_summary_name + "'!$D$" + str(loc_indx) + ":$" + str(chr(ord('D') + num_of_product_type)) + "$" + str(loc_indx + 1)
        company_name_for_16.append("='" + worksheet_summary_name + "'!$B$" + str(loc_indx) + ":$B$" + str(loc_indx))
        company_name_for_16.append("='" + worksheet_summary_name + "'!$B$" + str(loc_indx + 1) + ":$B$" + str(loc_indx + 1))
        
        loc_indx_16 = loc_indx + 2
    elif flag_2 == 3:
        location_for_label_16 = "='" + worksheet_summary_name + "'!$B$" + str(loc_indx) + ":$B$" + str(loc_indx + 2)
        location_for_value_16 = "='" + worksheet_summary_name + "'!$C$" + str(loc_indx) + ":$C$" + str(loc_indx + 2)
        location_for_bar_chart_16 = "='" + worksheet_summary_name + "'!$D$" + str(loc_indx) + ":$" + str(chr(ord('D') + num_of_product_type)) + "$" + str(loc_indx + 2)
        company_name_for_16.append("='" + worksheet_summary_name + "'!$B$" + str(loc_indx) + ":$B$" + str(loc_indx))
        company_name_for_16.append("='" + worksheet_summary_name + "'!$B$" + str(loc_indx + 1) + ":$B$" + str(loc_indx + 1))
        company_name_for_16.append("='" + worksheet_summary_name + "'!$B$" + str(loc_indx + 2) + ":$B$" + str(loc_indx + 2))

        loc_indx_16 = loc_indx + 3
    elif flag_2 == 4:
        location_for_label_16 = "='" + worksheet_summary_name + "'!$B$" + str(loc_indx) + ":$B$" + str(loc_indx + 3)
        location_for_value_16 = "='" + worksheet_summary_name + "'!$C$" + str(loc_indx) + ":$C$" + str(loc_indx + 3)
        location_for_bar_chart_16 = "='" + worksheet_summary_name + "'!$D$" + str(loc_indx) + ":$" + str(chr(ord('D') + num_of_product_type)) + "$" + str(loc_indx + 3)
        company_name_for_16.append("='" + worksheet_summary_name + "'!$B$" + str(loc_indx) + ":$B$" + str(loc_indx))
        company_name_for_16.append("='" + worksheet_summary_name + "'!$B$" + str(loc_indx + 1) + ":$B$" + str(loc_indx + 1))
        company_name_for_16.append("='" + worksheet_summary_name + "'!$B$" + str(loc_indx + 2) + ":$B$" + str(loc_indx + 2))
        company_name_for_16.append("='" + worksheet_summary_name + "'!$B$" + str(loc_indx + 3) + ":$B$" + str(loc_indx + 3))

        loc_indx_16 = loc_indx + 4
    else:
        pass
            
    return location_for_label_16, location_for_value_16, location_for_bar_chart_16, company_name_for_16, loc_indx_16

def read_data_43_46(workbook_read, num_of_product_type, worksheet_summary_name, flag_2, loc_indx):
    # flag: 1 -> 15, 16, 17; 2 -> 15, 16; 3 -> 15; 4 -> 15, 17; 5 -> 17; 6 -> 16; 7 -> 16, 17; 0 -> null
    # flag-2: 1 -> cdw, 2 -> cdw + company_1, 3 -> cdw + company_1 + company_2, 4 -> cdw + company_1 + company_2 + company_3
#     workbook_read = xlrd.open_workbook(filename)
    worksheet_read = workbook_read.sheet_by_name(worksheet_summary_name)
    location_for_label_17 = ''
    location_for_value_17 = ''
    location_for_bar_chart_17 = ''
    company_name_for_17 = []
    loc_indx_17 = 0
    
    cdw_17 = worksheet_read.cell(loc_indx - 1, 1).value
    cdw_17_value = worksheet_read.cell(loc_indx - 1, 2).value
    
    if flag_2 == 1:
        location_for_label_17 = "='" + worksheet_summary_name + "'!$B$" + str(loc_indx) + ":$B$" + str(loc_indx)
        location_for_value_17 = "='" + worksheet_summary_name + "'!$C$" + str(loc_indx) + ":$C$" + str(loc_indx)
        location_for_bar_chart_17 = "='" + worksheet_summary_name + "'!$D$" + str(loc_indx) + ":$" + str(chr(ord('D') + num_of_product_type)) + "$" + str(loc_indx)
        company_name_for_17.append("='" + worksheet_summary_name + "'!$B$" + str(loc_indx) + ":$B$" + str(loc_indx))
        loc_indx_17 = loc_indx + 1
    elif flag_2 == 2:
        location_for_label_17 = "='" + worksheet_summary_name + "'!$B$" + str(loc_indx) + ":$B$" + str(loc_indx + 1)
        location_for_value_17 = "='" + worksheet_summary_name + "'!$C$" + str(loc_indx) + ":$C$" + str(loc_indx + 1)
        location_for_bar_chart_17 = "='" + worksheet_summary_name + "'!$D$" + str(loc_indx) + ":$" + str(chr(ord('D') + num_of_product_type)) + "$" + str(loc_indx + 1)
        company_name_for_17.append("='" + worksheet_summary_name + "'!$B$" + str(loc_indx) + ":$B$" + str(loc_indx))
        company_name_for_17.append("='" + worksheet_summary_name + "'!$B$" + str(loc_indx + 1) + ":$B$" + str(loc_indx + 1))

        loc_indx_17 = loc_indx + 2
    elif flag_2 == 3:
        location_for_label_17 = "='" + worksheet_summary_name + "'!$B$" + str(loc_indx) + ":$B$" + str(loc_indx + 2)
        location_for_value_17 = "='" + worksheet_summary_name + "'!$C$" + str(loc_indx) + ":$C$" + str(loc_indx + 2)
        location_for_bar_chart_17 = "='" + worksheet_summary_name + "'!$D$" + str(loc_indx) + ":$" + str(chr(ord('D') + num_of_product_type)) + "$" + str(loc_indx + 2)
        company_name_for_17.append("='" + worksheet_summary_name + "'!$B$" + str(loc_indx) + ":$B$" + str(loc_indx))
        company_name_for_17.append("='" + worksheet_summary_name + "'!$B$" + str(loc_indx + 1) + ":$B$" + str(loc_indx + 1))
        company_name_for_17.append("='" + worksheet_summary_name + "'!$B$" + str(loc_indx + 2) + ":$B$" + str(loc_indx + 2))

        loc_indx_17 = loc_indx + 3
    elif flag_2 == 4:
        location_for_label_17 = "='" + worksheet_summary_name + "'!$B$" + str(loc_indx) + ":$B$" + str(loc_indx + 3)
        location_for_value_17 = "='" + worksheet_summary_name + "'!$C$" + str(loc_indx) + ":$C$" + str(loc_indx + 3)
        location_for_bar_chart_17 = "='" + worksheet_summary_name + "'!$D$" + str(loc_indx) + ":$" + str(chr(ord('D') + num_of_product_type)) + "$" + str(loc_indx + 3)
        company_name_for_17.append("='" + worksheet_summary_name + "'!$B$" + str(loc_indx) + ":$B$" + str(loc_indx))
        company_name_for_17.append("='" + worksheet_summary_name + "'!$B$" + str(loc_indx + 1) + ":$B$" + str(loc_indx + 1))
        company_name_for_17.append("='" + worksheet_summary_name + "'!$B$" + str(loc_indx + 2) + ":$B$" + str(loc_indx + 2))
        company_name_for_17.append("='" + worksheet_summary_name + "'!$B$" + str(loc_indx + 3) + ":$B$" + str(loc_indx + 3))

        loc_indx_17 = loc_indx + 4
    else:
        pass
          
    return location_for_label_17, location_for_value_17, location_for_bar_chart_17, company_name_for_17

def read_data_location(workbook_read, num_of_product_type, worksheet_summary_name, flag, flag_2):
    # 1 -> 15, 16, 17; 2 -> 15, 16; 3 -> 15; 4 -> 15, 17; 5 -> 17; 6 -> 16; 7 -> 16, 17; 0 -> null
#     workbook_read = xlrd.open_workbook(filename)
    location_for_label_three_years = []
    location_for_value_three_years = []
    location_for_bar_chart_three_years = []
    company_name_for_three_years = []
    location = []
#     print(worksheet_summary_name)
    if flag == 1:
        location_for_label_35_38, location_for_value_35_38, location_for_bar_chart_35_38, company_name_for_35_38, loc_indx_15 = read_data_35_38(workbook_read, num_of_product_type, worksheet_summary_name, flag_2[0])
        location_for_label_39_42, location_for_value_39_42, location_for_bar_chart_39_42, company_name_for_39_42, loc_indx_16 = read_data_39_42(workbook_read, num_of_product_type, worksheet_summary_name, flag_2[1], loc_indx_15)
        location_for_label_43_46, location_for_value_43_46, location_for_bar_chart_43_46, company_name_for_43_46 = read_data_43_46(workbook_read, num_of_product_type, worksheet_summary_name, flag_2[2], loc_indx_16)
        location_for_label_three_years = [location_for_label_35_38, location_for_label_39_42, location_for_label_43_46]
        location_for_value_three_years = [location_for_value_35_38, location_for_value_39_42, location_for_value_43_46]
        location_for_bar_chart_three_years = [location_for_bar_chart_35_38, location_for_bar_chart_39_42, location_for_bar_chart_43_46]
        company_name_for_three_years = [company_name_for_35_38, company_name_for_39_42, company_name_for_43_46]
        location = [15, 16, 17]
    elif flag == 2 or flag == 4 or flag == 7:
        location_for_label_35_38, location_for_value_35_38, location_for_bar_chart_35_38, company_name_for_35_38, loc_indx_15 = read_data_35_38(workbook_read, num_of_product_type, worksheet_summary_name, flag_2[0])
        location_for_label_39_42, location_for_value_39_42, location_for_bar_chart_39_42, company_name_for_39_42, loc_indx_16 = read_data_39_42(workbook_read, num_of_product_type, worksheet_summary_name, flag_2[1], loc_indx_15)
        location_for_label_three_years = [location_for_label_35_38, location_for_label_39_42]
        location_for_value_three_years = [location_for_value_35_38, location_for_value_39_42]
        location_for_bar_chart_three_years = [location_for_bar_chart_35_38, location_for_bar_chart_39_42]
        company_name_for_three_years = [company_name_for_35_38, company_name_for_39_42]
        if flag == 2:
            location = [15, 16]
        elif flag == 4:
            location = [15, 17]
        else:
            location = [16, 17]
    elif flag == 3 or flag == 5 or flag == 6:
        location_for_label_35_38, location_for_value_35_38, location_for_bar_chart_35_38, company_name_for_35_38, loc_indx_15 = read_data_35_38(workbook_read, num_of_product_type, worksheet_summary_name, flag_2[0])
        location_for_label_three_years = [location_for_label_35_38]
        location_for_value_three_years = [location_for_value_35_38]
        location_for_bar_chart_three_years = [location_for_bar_chart_35_38]
        company_name_for_three_years = [company_name_for_35_38]
        if flag == 3:
            location = [15]
        elif flag == 5:
            location = [17]
        else:
            location = [16]
    else:
        pass
    
#     print(flag, location_for_label_three_years, location_for_value_three_years, location_for_bar_chart_three_years, company_name_for_three_years, location)
    return location_for_label_three_years, location_for_value_three_years, location_for_bar_chart_three_years, company_name_for_three_years, location

# create the location of title, series and others
def set_the_format(workbook_read, num_of_product_type, worksheet_summary_name, flag):
#     workbook_read = xlrd.open_workbook(filename)
    # 1 -> 15, 16, 17; 2 -> 15, 16; 3 -> 15; 4 -> 15, 17; 5 -> 17; 6 -> 16; 7 -> 16, 17; 0 -> null
#     print(1)
    worksheet_read = workbook_read.sheet_by_name(worksheet_summary_name)
    product_type_name = "='" + worksheet_summary_name + "'!$D$55:$" + str(chr(ord('D') + num_of_product_type)) + "$55"

    title_pie_chart_three_years = []
    # title for pie chart
    if flag == 1:        
        title_pie_chart_2015 = '2015 eRate($' + str('{:0,.0f}'.format(worksheet_read.cell(69, 1).value)) + ')'
        title_pie_chart_2016 = '2016 eRate($' + str('{:0,.0f}'.format(worksheet_read.cell(70, 1).value))+ ')'
        title_pie_chart_2017 = '2017 eRate($' + str('{:0,.0f}'.format(worksheet_read.cell(71, 1).value))+ ')'
        title_pie_chart_three_years = [title_pie_chart_2015, title_pie_chart_2016, title_pie_chart_2017]
    elif flag == 2:
        title_pie_chart_2015 = '2015 eRate($' + str('{:0,.0f}'.format(worksheet_read.cell(65, 1).value)) + ')'
        title_pie_chart_2016 = '2016 eRate($' + str('{:0,.0f}'.format(worksheet_read.cell(66, 1).value)) + ')'
        title_pie_chart_three_years = [title_pie_chart_2015, title_pie_chart_2016]
    elif flag == 3:
        title_pie_chart_2015 = '2015 eRate($' + str('{:0,.0f}'.format(worksheet_read.cell(61, 1).value)) + ')'
        title_pie_chart_three_years = [title_pie_chart_2015]
    elif flag == 4:
        title_pie_chart_2015 = '2015 eRate($' + str('{:0,.0f}'.format(worksheet_read.cell(65, 1).value)) + ')'
        title_pie_chart_2017 = '2017 eRate($' + str('{:0,.0f}'.format(worksheet_read.cell(66, 1).value)) + ')'
        title_pie_chart_three_years = [title_pie_chart_2015, title_pie_chart_2017]
    elif flag == 5:
        title_pie_chart_2017 = '2017 eRate($' + str('{:0,.0f}'.format(worksheet_read.cell(61, 1).value)) + ')'
        title_pie_chart_three_years = [title_pie_chart_2017]
    elif flag == 6:
        title_pie_chart_2016 = '2016 eRate($' + str('{:0,.0f}'.format(worksheet_read.cell(61, 1).value)) + ')'
        title_pie_chart_three_years = [title_pie_chart_2016]
    elif flag == 7:
        title_pie_chart_2016 = '2016 eRate($' + str('{:0,.0f}'.format(worksheet_read.cell(65, 1).value)) + ')'
        title_pie_chart_2017 = '2017 eRate($' + str('{:0,.0f}'.format(worksheet_read.cell(66, 1).value)) + ')'
        title_pie_chart_three_years = [title_pie_chart_2016, title_pie_chart_2017]
    else:
        pass
#     print(flag)
    return title_pie_chart_three_years, product_type_name

# draw pie chart
def draw_pie_chart(label_three_years, value_three_years, bar_three_years, title_pie_chart, product_type_name, company_name, location, workbook, worksheet_summary_name, worksheet_detail_name, brand_location_three_years, brand_name, num_brand_location_three_years, flag, flag_2, filename):
#     workbook = xlsxwriter.Workbook(filename)
    worksheet_summary = workbook.get_worksheet_by_name(worksheet_summary_name)
    worksheet_detail = workbook.get_worksheet_by_name(worksheet_detail_name)
        # for the pie chart
#     print(type(worksheet_summary))
    format1 = workbook.add_format({'num_format': '$#,##0'})
    format2 = workbook.add_format({'num_format': '%0.0'})
#     format3 = workbook.add_format()
#     format3.set_text_wrap()
#     format3 = workbook.add_format({'border' : 0})
#     format3.set_border(style=0)
#     format3.set_bottom(0)
    
    worksheet_detail.set_column('I:I', None, format1)
    worksheet_detail.set_column('G:G', None, format1)
    worksheet_detail.set_column('A:A', 50)
    worksheet_detail.set_column('B:L', 15)
#     worksheet_detail.set_column('A1:L1', None, format3)
#     if flag == 1:
#         worksheet_summary.set_column('$C$36:$X$47', None, format1)
#         worksheet_summary.set_column('$B$50:$C$52', None, format1)
# #         worksheet_summary.set_column('E50:E52', None, format1)
#         worksheet_summary.set_column('$D$50:$E$52', None, format2)
#     elif flag == 2 or flag == 4 or flag == 7:
#         worksheet_summary.set_column('$C$36:$X$44', None, format1)
#         worksheet_summary.set_column('$B$46:$C$47', None, format1)
# #         worksheet_summary.set_column('E50:E52', None, format1)
#         worksheet_summary.set_column('$D$46:$E$47', None, format2)
#     else:
#         worksheet_summary.set_column('$C$36:$X$40', None, format1)
#         worksheet_summary.set_column('$B$41:$C$41', None, format1)
# #         worksheet_summary.set_column('E50:E52', None, format1)
#         worksheet_summary.set_column('$D$41:$E$41', None, format2)
#     worksheet_summary.set_column('A:Z', 15)
    if flag == 1:
        worksheet_summary.set_column('$C$56:$X$67', None, format1)
        worksheet_summary.set_column('$B$70:$C$72', None, format1)
#         worksheet_summary.set_column('E50:E52', None, format1)
        worksheet_summary.set_column('$D$70:$E$72', None, format2)
        for i in range(66 - 55):
            worksheet_summary.set_row(55 + i, None, format1)
        for i in range(20):
            worksheet_summary.set_row(74 + i, None, format1)
    elif flag == 2 or flag == 4 or flag == 7:
        worksheet_summary.set_column('$C$56:$X$64', None, format1)
        worksheet_summary.set_column('$B$66:$C$67', None, format1)
#         worksheet_summary.set_column('E50:E52', None, format1)
        worksheet_summary.set_column('$D$66:$E$67', None, format2)
        for i in range(63 - 55):
            worksheet_summary.set_row(55 + i, None, format1)
        for i in range(20):
            worksheet_summary.set_row(69 + i, None, format1)
    else:
        worksheet_summary.set_column('$C$56:$X$60', None, format1)
        worksheet_summary.set_column('$B$61:$C$61', None, format1)
#         worksheet_summary.set_column('E50:E52', None, format1)
        worksheet_summary.set_column('$D$61:$E$61', None, format2)
        for i in range(59 - 55):
            worksheet_summary.set_row(55 + i, None, format1)
        for i in range(20):
            worksheet_summary.set_row(65 + i, None, format1)
    worksheet_summary.set_column('A:Z', 15)
    
    for indx, each_year in enumerate(label_three_years):
#     for label, value, title, loc in zip(label_three_years, value_three_years, title_pie_chart, location):
#         if each_year != '':
        chart = workbook.add_chart({'type' : 'pie'})
        chart.set_chartarea({
            'border': {'none': True},
            'fill':   {'none': True},
        })
        chart.add_series({
                          'categories' : each_year,
                          'values' : value_three_years[indx],
                          'data_labels' : {'value' : True, 'percentage' : True, 'separator' : ' '},
                          'bg_color' : None,
                          'points' : [ 
                              {'fill' : {'color': '#FF5500'}}, # red
                              {'fill' : {'color': '#0099FF'}}, 
                              {'fill' : {'color': '#00FF99'}},
                              {'fill' : {'color': '#FFFF00'}}, 
                           ],
                         })            
        chart.set_title({'name': title_pie_chart[indx]})
        chart.set_size({'x_scale' : 1.8, 'y_scale' : 1})
#         chart.set_legend({'position': 'bottom'})
        
        if len(label_three_years) == 1:
            worksheet_summary.insert_chart('F7', chart)
        elif len(label_three_years) == 2:
            if indx == 0:
                worksheet_summary.insert_chart('C7', chart)
            else:
                worksheet_summary.insert_chart('K7', chart)
        else:
            if indx == 0:
                worksheet_summary.insert_chart('A7', chart)
            elif indx == 1:
                worksheet_summary.insert_chart('I7', chart)
            else:
                worksheet_summary.insert_chart('Q7', chart)

    for indx, each_year in enumerate(label_three_years): 
#         if each_year != '':
        chart_bar = workbook.add_chart({'type' : 'bar', 'subtype': 'stacked'})
#             '=' + worksheet_summary_name + '!$B$42:$G$45'
#             ->  '=' + worksheet_summary_name + '!$B$42:$G$42'
#                  '=' + worksheet_summary_name + '!$B$43:$G$43'
# #                   '=' + worksheet_summary_name + '!$B$44:$G$44'  ....
        tmp = []
        for s in bar_three_years[indx]:
            tmp.append(s)
        for i in range(flag_2[indx]): # cdw, competitor_1, competitor_2, competitor_3
            # judge the location, if only the digit position change, just change one char is ok
            # when two position change, we need to judge when to change the both num
            if tmp[-2] != tmp[-8]:
                tmp[-2] = '5'
                tmp[-8] = '5'
            if ord(bar_three_years[indx][-7]) + i > ord('9'):
                tmp[-2] = '6'
                tmp[-8] = '6' 
                tmp[-1] = chr(ord(bar_three_years[indx][-7]) + i - 10)
#                 print(tmp[-1])
                tmp[-7] = chr(ord(bar_three_years[indx][-7]) + i - 10)
            else:
                tmp[-1] = chr(ord(bar_three_years[indx][-7]) + i)
                tmp[-7] = chr(ord(bar_three_years[indx][-7]) + i)
            
            colors = ['#FF5500', '#0099FF', '#00FF99', '#FFFF00']
            chart_bar.add_series({ 
                                  'categories' : product_type_name,
                                  'name' : company_name[indx][i],
                                  'values' : ''.join(tmp),
                                  'fill' : {'color' : colors[i]},
#                                   'data_labels' : {'value' : True, 'position' : 'outside_end', 'num_format' : '$#,##0'},
                                })
            chart_bar.set_plotarea({
                'border': {'none': True},
                'fill':   {'none': True},
            })
            chart_bar.set_chartarea({
                'border': {'none': True},
                'fill':   {'none': True},
            })
            chart_bar.set_x_axis({
                'major_gridlines': {
                    'visible': False,
                },
                'display_units' : 'thousands'
            })
            chart_bar.set_size({'x_scale' : 1.8, 'y_scale' : 1.2})
#             chart_bar.set_y_axis({
#                 'major_gridlines': {
#                     'visible': False,
#                 },
#             })
#             chart_bar.set_legend({'position': 'bottom'})
            chart_bar.set_title({'none': True})
            if len(label_three_years) == 1:
                worksheet_summary.insert_chart('F21', chart_bar)
            elif len(label_three_years) == 2:
                if indx == 0:
                    worksheet_summary.insert_chart('C21', chart_bar)
                else:
                    worksheet_summary.insert_chart('K21', chart_bar)
            else:
                if indx == 0:
                    worksheet_summary.insert_chart('A21', chart_bar)
                elif indx == 1:
                    worksheet_summary.insert_chart('I21', chart_bar)
                else:
                    worksheet_summary.insert_chart('Q21', chart_bar)
            
    for indx, each_year in enumerate(label_three_years): 
#         if each_year != '':
        chart_brand = workbook.add_chart({'type' : 'bar', 'subtype': 'stacked'})

        tmp = []
        tmp_i = 0
        for s in brand_location_three_years[indx]:
            tmp.append(s)
        for i in range(num_brand_location_three_years[indx] + 1):
#             judge the location, if only the digit position change, just change one char is ok
#             when two position change, we need to judge when to change the both num
#             tmp_i = i
            if tmp[-2] == '7' and tmp[-8] == '6':
                tmp[-2] = '6'
                tmp[-8] = '6'
            elif tmp[-2] == '8' and tmp[-8] == '7':
                tmp[-2] = '7'
                tmp[-8] = '7'
            elif tmp[-2] == '9' and tmp[-8] == '8':
                tmp[-2] = '8'
                tmp[-8] = '8'
            if ord(brand_location_three_years[indx][-7]) + i > ord('9') and tmp_i == 0:
                if tmp[-2] == '6' and tmp[-8] == '6':
                    tmp[-2] = '7'
                    tmp[-8] = '7' 
                    tmp[-1] = chr(ord(brand_location_three_years[indx][-7]) + i - 10)
                    tmp[-7] = chr(ord(brand_location_three_years[indx][-7]) + i - 10)
                    tmp_i = 1
                elif tmp[-2] == '7' and tmp[-8] == '7':
                    tmp[-2] = '8'
                    tmp[-8] = '8' 
                    tmp[-1] = chr(ord(brand_location_three_years[indx][-7]) + i - 10)
                    tmp[-7] = chr(ord(brand_location_three_years[indx][-7]) + i - 10)
                    tmp_i = 1
                elif tmp[-2] == '8' and tmp[-8] == '8':
                    tmp[-2] = '9'
                    tmp[-8] = '9'
                    tmp[-1] = chr(ord(brand_location_three_years[indx][-7]) + i - 10)
                    tmp[-7] = chr(ord(brand_location_three_years[indx][-7]) + i - 10)
                    tmp_i = 1
            else:
                if tmp_i == 1:
                    tmp[-1] = chr(ord(brand_location_three_years[indx][-7]) + i - 10)
                    tmp[-7] = chr(ord(brand_location_three_years[indx][-7]) + i - 10)
                else:
                    tmp[-1] = chr(ord(brand_location_three_years[indx][-7]) + i)
                    tmp[-7] = chr(ord(brand_location_three_years[indx][-7]) + i)
#             print(''.join(tmp))
#             colors = ['red', 'blue', 'green', 'yellow']
            try:
                chart_brand.add_series({ 
                                      'categories' : product_type_name,
                                      'name' : brand_name[indx][i],
                                      'values' : ''.join(tmp),
    #                                   'fill' : {'color' : colors[i]},
    #                                   'data_labels' : {'value' : True, 'position' : 'outside_end', 'num_format' : '$#,##0'},
                                    })
                print(''.join(tmp))
            except:
                print(tmp, type(tmp), type(tmp[0]))
                print(worksheet_summary_name, filename)
            chart_brand.set_plotarea({
                'border': {'none': True},
                'fill':   {'none': True},
            })
            chart_brand.set_chartarea({
                'border': {'none': True},
                'fill':   {'none': True},
            })
            chart_brand.set_x_axis({
                'display_units' : 'thousands',
                'major_gridlines': {
                    'visible': False,
                },
            })
            chart_brand.set_size({'x_scale' : 1.8, 'y_scale' : 1.2})
            chart_brand.set_title({'none': True})
            if len(label_three_years) == 1:
                worksheet_summary.insert_chart('F38', chart_brand)
            elif len(label_three_years) == 2:
                if indx == 0:
                    worksheet_summary.insert_chart('C38', chart_brand)
                else:
                    worksheet_summary.insert_chart('K38', chart_brand)
            else:
                if indx == 0:
                    worksheet_summary.insert_chart('A38', chart_brand)
                elif indx == 1:
                    worksheet_summary.insert_chart('I38', chart_brand)
                else:
                    worksheet_summary.insert_chart('Q38', chart_brand)

# send corresponding file to AM's Email
def send_mail(send_from, send_to, subject, text, files=None):
    assert isinstance(send_to, list)

    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    msg.attach(MIMEText(text))

#     for f in files or []:
    with open(files, "rb") as fil:
        part = MIMEApplication(
            fil.read(),
            Name=basename(files)
        )
        part['Content-Disposition'] = 'attachment; filename="%s"' % basename(files)
        msg.attach(part)

    smtp = smtplib.SMTP("messaging.cdw.com", 25)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.close()
    
# BEN, Account Manager, Enrollment
k12_data = k12_data_filter()
# Applicant, BEN, State, Disc, ServiceProvider, ProductType, Brand, TotalSpend, RequestAmt
raw_data_15_with_k12, raw_data_16_with_k12, raw_data_17_with_k12 = read_raw_data(k12_data)
# add EmailAddress, #order, #quote, TotalDollar
raw_data_three_years = info_appending(raw_data_15_with_k12, raw_data_16_with_k12, raw_data_17_with_k12)
# for each AM generate a csv attachment, for each customer generate two sheets
# raw_data_three_years.groupby('PAM')
# for each AM
UniquePAM = raw_data_three_years.PAM.unique()
# UniqueState = raw_data_three_years.State.unique()
DataFrameDict = {elem : pd.DataFrame for elem in UniquePAM}

for key in DataFrameDict.keys():
    DataFrameDict[key] = raw_data_three_years[:][raw_data_three_years.PAM == key]
#     DataFrameDict_State[key] = raw_data_three_years['State'][raw_data_three_years.PAM == key]
    filename = key + '.xlsx'
    # create a file for each AM
    writer = pd.ExcelWriter(filename, engine='xlsxwriter')
    workbook = writer.book
#     print(filename)
#     workbook_read = xlrd.open_workbook(filename)
    # for each applicant form table and title
    UniqueApplicant = DataFrameDict[key].CustomerCode.unique()
    DataFrameDict_2 = {elem : pd.DataFrame for elem in UniqueApplicant}
    flage = 0
    sheetname_indx = 1
    for key2 in DataFrameDict_2.keys():
        detail_sheet_name = str(key2) + '_Detail'
        summary_sheet_name = str(key2) + '_Summary'
        
        DataFrameDict_2[key2] = DataFrameDict[key][:][DataFrameDict[key].CustomerCode == key2]
        # create a sheet for each applicant(detail)
        usecols = ['Applicant', 'CustomerCode', 'State', 'ServiceProvider', 'Brand', 'ProductType', 'TotalSpend', 'Discpct', 'RequestAmt', 'OrderYear']
#         try:
        DataFrameDict_2[key2].to_excel(writer, detail_sheet_name, columns = usecols, index = False)
        num_of_product_type = len((raw_data_three_years[:][raw_data_three_years.PAM == key][raw_data_three_years.CustomerCode == key2]).ProductType.unique()) - 1
        flag, flag_2, brand_location_three_years, brand_name, num_brand_location_three_years = make_summary_sheet_table(writer, workbook, DataFrameDict_2[key2], summary_sheet_name, num_of_product_type)
        sheetname_indx += 1
    writer.close()

for key in DataFrameDict.keys():
    DataFrameDict[key] = raw_data_three_years[:][raw_data_three_years.PAM == key]
#     DataFrameDict_State[key] = raw_data_three_years['State'][raw_data_three_years.PAM == key]
    filename = key + '.xlsx'
    # create a file for each AM
    writer = pd.ExcelWriter(filename, engine='xlsxwriter')
    workbook = writer.book
#     print(filename)
    workbook_read = xlrd.open_workbook(filename)
    # for each applicant form table and title
    UniqueApplicant = DataFrameDict[key].CustomerCode.unique()
    DataFrameDict_2 = {elem : pd.DataFrame for elem in UniqueApplicant}
    # flage = 0
    # sheetname_indx = 1
    for key2 in DataFrameDict_2.keys():
        detail_sheet_name = str(key2) + '_Detail'
        summary_sheet_name = str(key2) + '_Summary'
        
        DataFrameDict_2[key2] = DataFrameDict[key][:][DataFrameDict[key].CustomerCode == key2]
        # create a sheet for each applicant(detail)
        usecols = ['Applicant', 'CustomerCode', 'State', 'ServiceProvider', 'Brand', 'ProductType', 'TotalSpend', 'Discpct', 'RequestAmt', 'OrderYear']
#         try:
        num_of_product_type = len((raw_data_three_years[:][raw_data_three_years.PAM == key][raw_data_three_years.CustomerCode == key2]).ProductType.unique()) - 1
        flag, flag_2, brand_location_three_years, brand_name, num_brand_location_three_years = make_summary_sheet_table(writer, workbook, DataFrameDict_2[key2], summary_sheet_name, num_of_product_type)
        DataFrameDict_2[key2].to_excel(writer, detail_sheet_name, columns = usecols, index = False)
        label_three_years, value_three_years, bar_three_years, company_name, location = read_data_location(workbook_read, num_of_product_type, summary_sheet_name, flag, flag_2)
        title_pie_chart, product_type_name = set_the_format(workbook_read, num_of_product_type, summary_sheet_name, flag)
        draw_pie_chart(label_three_years, value_three_years, bar_three_years, title_pie_chart, product_type_name, company_name, location, workbook, summary_sheet_name, detail_sheet_name, brand_location_three_years, brand_name, num_brand_location_three_years, flag, flag_2, filename)
        # sheetname_indx += 1
    writer.close()

# Email Sending
# obtain the emailAddress corresponding to the name of account manager
data_for_email = raw_data_three_years[['PAM', 'EMailAddress']]
data_for_email = data_for_email[data_for_email['EMailAddress'] != 0]
no_duplicate_name = data_for_email['PAM'].drop_duplicates()
no_duplicate_name = pd.Series.tolist(no_duplicate_name)
no_duplicate_email = data_for_email['EMailAddress'].drop_duplicates()
no_duplicate_email = pd.Series.tolist(no_duplicate_email)
for name, email in zip(no_duplicate_name, no_duplicate_email):
    file_name = name + '.xlsx'
    full_name = name.split(' ')
    email_intro = ""
    email_intro += "Good morning " + full_name[0].lower().capitalize() + ", \n"
    email_intro += "\n"
    email_intro += email
    email_intro += "\n"
    email_intro += "This is the eRate program conclusion for 2015-2017. \n"
    # email_intro += " history is based on 3 year. \n"
    email_intro += "Please use this data responsibly. \n"
    email_intro += "\n"
    email_intro += "Best Regards, \n"
    email_intro += "Marketing"
    email_intro += "\n"
    email_intro += "\n"
    email_intro += "\n"
    email_intro += "Please contact greg tomezak (gregtom@cdw.com) with any questions or feedback."
    send_mail('yuhangpeng2018@u.northwestern.edu', ['yuhapen@cdw.com'], 'eRate from 15-17', email_intro, file_name)
