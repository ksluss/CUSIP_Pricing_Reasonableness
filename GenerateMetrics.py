#%% Setting everything up
from RiskTools import useful_functions as func
import pandas as pd
from datetime import datetime
import numpy as np
import blpapi


#%%
def generatemetrics(datestr:str, portstr:str, tenors, wgtCol_prefix:str):
    # maps AOCA ratings to IG vs non-IG
    ratings = func.getRatingsMap()
    # datetime type variable for the report_date
    enddate = datetime.strptime(datestr, '%Y%m%d')

    #%% Import instrument metrics

    qry = f"Select a.Date, a.PortfolioID, a.Cusip, b.Class,  a.Description, b.Sector, a.price, a.MarketValue, b.ParPrice, c.EffectiveDuration, c.ModifiedDuration, c.EffectiveConvexity, c.SpreadDuration, c.WAL, b.MaturityDate, d.AOCA_Rating " \
          f"from dbo.position a " \
          f"left join dbo.instrument b on a.cusip = b.cusip " \
          f"left join dbo.InstrumentMetrics c on a.cusip = c.cusip and a.date = c.date " \
          f"OUTER APPLY dbo.getInstrumentRating(a.Cusip, a.Date) d " \
          f"where a.date = '{datestr}' and a.portfolioid in ({portstr}) "
    print(qry)
    positions = func.read_data('AOCA', qry)
    positions['MarketValue'].fillna(0)
    positions = positions.loc[positions['MarketValue']!=0]
    print(positions.head())

    #%%calculate time to maturity
    #pos = positions.merge(metrics, left_on='Primary_Asset_ID', right_on= 'Cusip', how = 'left')
    positions['TimeToMaturity'] = (pd.to_datetime(positions['MaturityDate'], '%Y%m%d') - enddate) / np.timedelta64(1, 'Y')
    # fill missing WAL with the average.  DV01 for all missing WAL will get assigned to the weighted average WAL from the points for which we do have WAL
    # fillWithAvg(df: pd.DataFrame, walcol: str, groupbycol: str)
    positions = func.fillWithAvg(positions, 'WAL', 'PortfolioID')

    # Determine IG vs non-IG
    positions['is_IG'] = positions['AOCA_Rating'].map(ratings)

    #%%Allocate EffectiveDuration and SpreadDuration to different tenors through linear interpolation of WAL and tenor buckets defined above.

    # CalcTenorWeights(df: pd.DataFrame, tenors: float, tenor_col: str, wgtCol_prefix)
    positions = func.CalcTenorWeights(positions, tenors, 'WAL', wgtCol_prefix)

    # CalcTenorRisk(df: pd.DataFrame, tenors:float, riskCol: str, ColPrefix:str, wgtColPrefix)
    positions = func.CalcTenorRisk(positions, tenors, "EffectiveDuration", "EffDur_", wgtCol_prefix)
    positions = func.CalcTenorRisk(positions, tenors, "EffectiveConvexity", "EffConv_", wgtCol_prefix)
    positions = func.CalcTenorRisk(positions, tenors, "SpreadDuration", "SpreadDur_", wgtCol_prefix)

    # CombineLowerBound(pos: pd.DataFrame, tenors: float, ColPrefix: str)
    positions = func.CombineLowerBound(positions, tenors, 'EffDur_')
    positions = func.CombineLowerBound(positions, tenors, 'EffConv_')
    positions = func.CombineLowerBound(positions, tenors, 'SpreadDur_')

    tenors.pop(0)

#Calculate the dollar risk for each tenor
#  CalcDollarRisk(df: pd.DataFrame, tenors:float, riskColPrefix: str, colname:str, mktvalcol:str)
#pos = rt.CalcDollarRisk(pos, tenors, 'EffDur_', 'DV01', 'MarketValue')
#pos = rt.CalcDollarRisk(pos, tenors, 'SpreadDur_', 'CS01', 'MarketValue')


#%% define data sets for swaps, treasuries,
#mktData = pd.read_csv(pos_fldr + mktDatafile, index_col='Date')

#treas_list = ['Date','H15T1Y Index','H15T2Y Index','H15T3Y Index','H15T5Y Index','H15T7Y Index','H15T10Y Index','H15T20Y Index','H15T30Y Index']
#swap_list = ['Date','USSW1 Curncy','USSWAP2 Curncy','USSWAP3 Curncy','USSWAP5 Curncy','USSWAP7 Curncy','USSWAP10Y Curncy','USSWAP20Y Curncy','USSWAP30Y Curncy']

#treasuries = mktData[treas_list]
    print('done generating metrics!')
    return positions

def writemetrics(outfldr:str, filename:str, positions:pd.DataFrame, publicIDs):
    outfldr = func.scrubfldrname(outfldr)

    #%% Write output to Excel
    with pd.ExcelWriter(f'{outfldr}{filename}') as writer:
        positions.to_excel(writer, sheet_name='Data', engine='xlsxwriter')
        #mktDatafile.to_excel(writer, sheet_name='Market_Data', engine='xlsxwriter')
        positions.loc[positions['PortfolioID'].isin(publicIDs)].to_excel(writer, sheet_name='Public_Funds', engine='xlsxwriter')
        positions.to_excel(writer, sheet_name='metrics', engine='xlsxwriter')

    print('done!')

    return None

if __name__ == "__main__":
    # Primary inputs are dates, paths, files
    report_date = '20230103'
    prev_date = '20221230'

    outfldr = r'H:\Projects\2023\CUSIP Pricing Analytics\EOD Metrics'
    pos_fldr = 'H:\Projects\CUSIP Pricing Analytics'
    pos_file = 'Angel_Oak_Custom_Pricing_Report.csv'
    mktDatafile = 'BloombergData.csv'

    # You can change this stuff, but you don't need to.
    wgtCol_prefix = 'wgt_'
    tenors = [0, .25, 1, 2, 3, 5, 7, 10, 20, 30]

    # Don't touch anything below this line.
    # scrub to folder name to ensure there's a slash at the end.
    outfldr = func.scrubfldrname(outfldr)
    pos_fldr = func.scrubfldrname(pos_fldr)

    # datetime type variable for the report_date
    enddate = datetime.strptime(report_date, '%Y%m%d')

    # %% Read in position data
    # positions = pd.read_csv(pos_fldr + pos_file)
    # mktDatafile = pd.read_csv(pos_fldr + mktDatafile)
    (fund, public, publicIDs, publicIDstr, private, privateIDs, privateIDstr) = func.importFunds(exclude=['AOU'])

#%%
    metrics = generatemetrics(prev_date, publicIDstr, tenors, wgtCol_prefix)

#%%
    writemetrics(outfldr, f'{prev_date}_ExpectedPrice_Output.xlsx', metrics, publicIDs)