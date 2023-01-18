import pandas as pd
from datetime import datetime
from xbbg import blp

#%%Read in market data
def PullHistoricalMarketData(startdate:datetime, enddate:datetime):
    mktdatainfo = pd.read_json('market_data_mappings.json')
    name_map = pd.Series(mktdatainfo['Name'].values, index=mktdatainfo['Bloomberg Name']).to_dict()
    rates = mktdatainfo.loc[mktdatainfo['Type']=='Rates']
    #convert rates into bps
    rates = rates
    rates_tickers = list(rates['Bloomberg Name'].map('{}'.format))


    spreads = mktdatainfo.loc[mktdatainfo['Type'] == 'Spreads']
    spread_tickers = list(spreads['Bloomberg Name'].map('{}'.format))

    rates = blp.bdh(rates_tickers, 'PX_LAST', startdate, enddate)
    zspreads = blp.bdh(spread_tickers, ['INDEX_OAS_TSY_BP','INDEX_OAS_SWAP_BP','INDEX_Z_SPREAD_BP','INDEX_BID_SPREAD_BP'], startdate, enddate)

    rates.index.name='date'
    zspreads.index.name='date'
    #rates.reset_index(inplace=True)
    #zspreads.reset_index(inplace=True)

    mktdata = rates.merge(zspreads, on='date',how='outer')
    mktdata.columns = mktdata.columns.get_level_values(0).map(name_map)+"_"+mktdata.columns.get_level_values(1)
    mktdata.columns = mktdata.columns.str.lower()

    return mktdata

def calcmktdatachngs(df:pd.DataFrame, dates):
    data = df.loc[df.index.isin(dates)]
    diff = df.diff()
    return diff