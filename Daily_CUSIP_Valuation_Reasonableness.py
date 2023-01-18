#https://bloomberg.github.io/blpapi-docs/python/3.13/
from xbbg import blp
from datetime import datetime
import pandas as pd
from RiskTools import useful_functions as func
import PriceExpectationMethodologies as expectation
import Market_Data as md

from GenerateMetrics import generatemetrics

reportdate_str = '20230113'
prevdate_str = '20230112'

reportfldr = r'M:\Risk Management\Valuation\Daily Valuation Source Reports'
datafile = f'{reportdate_str}_Angel_Oak_Custom_Pricing_Report2_Final.csv'
outfldr = r'\\aoc-files01\funding_users\users\ksluss\Projects\2023\CUSIP Pricing Analytics\Daily Report Output'

reportdate = datetime.strptime(reportdate_str,'%Y%m%d')
prevdate = datetime.strptime(prevdate_str,'%Y%m%d')

reportfldr = func.scrubfldrname(reportfldr, "\\")
outfldr = func.scrubfldrname(outfldr,"\\")
(fund, public, publicIDs, publicIDstr, private, privateIDs, privateIDstr) = func.importFunds(exclude=['AOU'])

wgtCol_prefix = 'wgt_'
tenors = [0, .25, 1, 2, 3, 5, 7, 10, 20, 30]

methodologies = pd.read_json('methodologies.json')
method_map = pd.Series(methodologies['model_methodology'].values, index=methodologies['class_sector']).to_dict()

#%% generate data metrics from the previous day
metrics = generatemetrics(prevdate_str, publicIDstr,tenors, wgtCol_prefix)
metrics.columns = metrics.columns.str.lower()
metrics['cusip'] = metrics['cusip'].str.upper()
#metrics.rename(columns={'price':'price_prev'}, inplace=True)

#%% get_holidays()
calendar = func.get_calendar()

#%%read in position data from US Bank File
#read in pricing source data from EagleSTAR output
source_data = pd.read_csv(reportfldr + datafile)[['Master_ID','Primary_Asset_ID','Issue_Name','Current_Price_Type','Current_Source','Prior_Source']]
source_data.columns= source_data.columns.str.lower()
revrepo = source_data.loc[source_data['issue_name'].str[:7]=='REVREPO']

#read in current day's price info
qry = f"Select a.Date, a.PortfolioID, a.Cusip, a.price, a.MarketValue, a.quantity from dbo.position a where date = '{reportdate_str}' and portfolioid in ({publicIDstr})"
print(qry)
pos = func.read_data('AOCA',qry)
pos.columns = pos.columns.str.lower()

#merge today's prices with US Bank information
positions = pos.merge(source_data, how = 'left', left_on = ['cusip', 'portfolioid'], right_on=['primary_asset_id','master_id'])
positions['cusip'] = positions['cusip'].str.upper()
positions['expected_source'] = None


#%%merge today's positions with prior day's position metrics
positions = positions.merge(metrics, on=['cusip','portfolioid'],how = 'left', suffixes=("","_prev"))
positions['class_sector']= positions['class'] + "_" + positions['sector']
positions = positions.merge(methodologies, on='class_sector',how='left')
positions['price_change'] = positions['price']-positions['price_prev']

#identify postiions with no source information in the US Bank file.
nosource = pos.loc[~pos['cusip'].isin(source_data['primary_asset_id'])]
nopos = source_data.loc[~source_data['primary_asset_id'].isin(pos['cusip'])]

#%%Read in expected Pricing Source
qry = f"select * from pricing.instruments"
pricing_sources = func.read_data('AOCA',qry)
pricing_sources.columns

#%%Identify the stuff where the price is not subject to question
positions['source_change'] = positions['current_source']!=positions['prior_source']
source_change = positions.loc[positions['source_change'] == True]

positions.loc[positions['class'].isin(['Financing','Fund']), 'expected_source'] = 'Financing'
positions.loc[(positions['current_source']=='Price at Cost') & (pd.isnull(positions['expected_source'])),'expected_source'] = 'Cost'
positions.loc[((positions['current_price_type']=='Px Official Close') |(positions['current_source']=='IDC NOCP') | (positions['current_source']=='IDC CANDADIAN')) & (pd.isnull(positions['expected_source'])),'expected_source'] = 'Official Close'
positions.loc[positions['class'].isin(['ETF']),'expected_source'] = 'Official Close'
positions.loc[(positions['current_source']=='Angel Oak Overrides') & (pd.isnull(positions['expected_source'])),'expected_source'] = 'Internal'
positions.loc[(positions['current_source']=='Broker Mark') & (pd.isnull(positions['expected_source'])),'expected_source'] = 'Broker Mark'
positions.loc[(positions['current_source']=='Eagle PACE') & (pd.isnull(positions['expected_source'])),'expected_source'] = 'Eagle PACE'
positions.loc[(positions['current_source']=='Bloomberg') & (pd.isnull(positions['expected_source'])),'expected_source'] = 'Bloomberg'

#%%For the stuff where the price comes from a vendor, identify the expected vendor
positions.loc[pd.isnull(positions['expected_source']), 'expected_source'] = positions.loc[pd.isnull(positions['expected_source'])].reset_index().merge(pricing_sources, how='left', on=['class','sector']).set_index('index')['PrimaryProvider']

#todo: check to ensure all positions are being priced by the primary vendor.

#%%identify expected price
#positions['expectation_source'] = ''
#cost = positions.loc[positions['expected_source']=='Cost']
#if cost.shape[0]!=0:
#    cost = expectation.cost(cost, reportdate_str)

#%% Generate market data
mktdata = md.PullHistoricalMarketData(prevdate,reportdate)
diff = md.calcmktdatachngs(mktdata, positions['date'].unique())
diff.index = pd.to_datetime(diff.index)
diff = diff.loc[diff.index==reportdate]

#%%Split corporates into HY and IG, then isolate all of the positions that were vendor priced.
positions.loc[(positions['model_methodology']=='corp') & (positions['is_ig']=='TRUE'), 'model_methodology'] = 'corp_ig'
positions.loc[(positions['model_methodology']=='corp') & (positions['is_ig']=='FALSE'), 'model_methodology'] = 'corp_hy'
vendor_priced = positions.loc[positions['expected_source'].isin(['BAML PriceServe','IDC','Eagle PACE',''])].copy()


#%%Identify positions where the price comes from Bloomberg and those that come from miscellaneous sources.
other = positions.loc[positions['expected_source'].isin(['Cost','Internal','Broker Mark','Eagle PACE'])].copy()
bloomberg = positions.loc[positions['expected_source'].isin(['Official Close','Bloomberg'])].copy()

index = bloomberg.index
bloomberg = expectation.Get_Bloomberg_Price(bloomberg, reportdate)
bloomberg.index = index

#%%Calculate prices for modeled securities
agency_rmbs = vendor_priced.loc[vendor_priced['model_methodology']=='RMBS_Agency'].copy()
corp_ig = vendor_priced.loc[vendor_priced['model_methodology']=='corp_ig'].copy()
corp_hy = vendor_priced.loc[vendor_priced['model_methodology']=='corp_hy'].copy()
abs_auto = vendor_priced.loc[vendor_priced['model_methodology']=='abs_auto'].copy()
abs_card = vendor_priced.loc[vendor_priced['model_methodology']=='abs_card'].copy()
abs = vendor_priced.loc[vendor_priced['model_methodology']=='abs'].copy()

index = agency_rmbs.index
agency_rmbs = expectation.RMBS_Agency(agency_rmbs, diff, reportdate, calendar)
agency_rmbs.index = index

index = corp_ig.index
corp_ig = expectation.corp_ig(corp_ig, diff, reportdate, calendar)
corp_ig.index = index

index = corp_hy.index
corp_hy = expectation.corp_hy(corp_hy, diff, reportdate, calendar)
corp_hy.index = index

index = abs_auto.index
abs_auto = expectation.corp_hy(abs_auto, diff, reportdate, calendar)
abs_auto.index = index

index = abs_card.index
abs_card = expectation.corp_hy(abs_card, diff, reportdate, calendar)
abs_card.index = index

index = abs.index
abs = expectation.corp_hy(abs, diff, reportdate, calendar)
abs.index = index

output = pd.concat([bloomberg, agency_rmbs, corp_hy, corp_ig, abs, abs_auto, abs_card])


#%%
corp = pd.concat([corp_hy,corp_ig])

#%%
output['expected_chg'] = output['expected_price'] - output['price_prev']
output['expected_mkt_val'] = output['expected_price']/100 * output['quantity']
relevantdata = output[['date','portfolioid','cusip','description','class','sector','current_source','prior_source','model_methodology','expectation_method','rate_move','conv_move','spread_move','model_price_chg','min_piece','trace_last_trade_size','trace_time_of_trade','trace_last_trade_price','expectation_method','expected_chg','price_change','price_prev','model_price','expected_price','price','quantity','expected_mkt_val','marketvalue']]



#%%output a report
with pd.ExcelWriter(f'{outfldr}{reportdate_str}_Pricing Reasonableness Report.xlsx') as writer:
    positions.to_excel(writer, sheet_name='Positions', engine='xlsxwriter')
    output.to_excel(writer, sheet_name='Output', engine='xlsxwriter')
    nosource.to_excel(writer, sheet_name='No Pricing Source Info', engine='xlsxwriter')
    source_change.to_excel(writer, sheet_name='Source Change', engine='xlsxwriter')
    nopos.to_excel(writer, sheet_name='No Position Info', engine='xlsxwriter')
    revrepo.to_excel(writer, sheet_name='Reverse Repos', engine='xlsxwriter')
    pricing_sources.to_excel(writer, sheet_name='Pricing Sources', engine='xlsxwriter')

    metrics.to_excel(writer, sheet_name='Position Metrics', engine='xlsxwriter')
    agency_rmbs.to_excel(writer, sheet_name='Agency RMBS', engine='xlsxwriter')
    corp.to_excel(writer, sheet_name='Agency RMBS', engine='xlsxwriter')
    relevantdata.to_excel(writer, sheet_name='Distilled Data', engine='xlsxwriter')
    #test.to_excel(writer, sheet_name='Test', engine='xlsxwriter')
    #test2.to_excel(writer, sheet_name='Test2', engine='xlsxwriter')

print('C.R.E.A.M. GET THE MONEY!!!')
print(f'done running {reportdate}')