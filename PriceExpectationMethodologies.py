# https://bloomberg.github.io/blpapi-docs/python/3.13/
import datetime

from xbbg import blp
import pandas as pd
import RiskTools as rt




def Rate_move(df: pd.DataFrame, mktdata: pd.DataFrame):
    df['rate_move'] = -1 / 100 * (df['effdur_0.25'] * mktdata['3m_cmt_px_last'].values
                                  + df['effdur_1'] * mktdata['1y_cmt_px_last'].values
                                  + df['effdur_2'] * mktdata['2y_cmt_px_last'].values
                                  + df['effdur_3'] * mktdata['3y_cmt_px_last'].values
                                  + df['effdur_5'] * mktdata['5y_cmt_px_last'].values
                                  + df['effdur_7'] * mktdata['7y_cmt_px_last'].values
                                  + df['effdur_10'] * mktdata['10y_cmt_px_last'].values
                                  + df['effdur_20'] * mktdata['20y_cmt_px_last'].values
                                  + df['effdur_30'] * mktdata['30y_cmt_px_last'].values
                                  )
    return df


def Convexity_move(df: pd.DataFrame, mktdata: pd.DataFrame):
    df['conv_move'] = .5 / (100 ** 2) * (df['effconv_0.25'] * mktdata['3m_cmt_px_last'].values ** 2
                                         + df['effconv_1'] * (mktdata['1y_cmt_px_last'].values) ** 2
                                         + df['effconv_2'] * (mktdata['2y_cmt_px_last'].values) ** 2
                                         + df['effconv_3'] * (mktdata['3y_cmt_px_last'].values) ** 2
                                         + df['effconv_5'] * (mktdata['5y_cmt_px_last'].values) ** 2
                                         + df['effconv_7'] * (mktdata['7y_cmt_px_last'].values) ** 2
                                         + df['effconv_10'] * (mktdata['10y_cmt_px_last'].values) ** 2
                                         + df['effconv_20'] * (mktdata['20y_cmt_px_last'].values) ** 2
                                         + df['effconv_30'] * (mktdata['30y_cmt_px_last'].values) ** 2)
    return df


def Spread_move(df: pd.DataFrame, mktdata: pd.DataFrame, spread: str):
    df['spread_move'] = -1 / 10000 * df['spreadduration'] * mktdata[spread].values

    return df

def Get_TRACE(df:pd.DataFrame):
    # pull trade data from BBG
    tickers = list("/CUSIP/" + df['cusip'].unique())
    fields = ['MIN_PIECE', 'TRACE_LAST_TRADE_SIZE', 'TRACE_TIME_OF_TRADE', 'TRACE_LAST_TRADE_PRICE']

    trade_data = blp.bdp(tickers, fields)
    trade_data['cusip'] = trade_data.index.str[7:]
    df = df.merge(trade_data, on='cusip', how='left')
    df['trace_time_of_trade']= pd.to_datetime(df['trace_time_of_trade'].astype(str))

    return df

def Get_Model_Price(df:pd.DataFrame, mktdata_diff: pd.DataFrame, spread:str):

    # calculate expected movement due to price changes
    df = Rate_move(df, mktdata_diff)
    df = Convexity_move(df, mktdata_diff)
    df = Spread_move(df, mktdata_diff, spread)
    df['model_price_chg'] = (df['rate_move'] + df['conv_move'] + df['spread_move']) * df['price_prev']

    # set the expected price equal to the previous price + the modeled price change
    df['expectation_method'] = 'Model'
    df['model_price'] = df['price_prev'] + df['model_price_chg']
    df['expected_price'] = df['model_price']

    return df

def Get_Bloomberg_Price(df:pd.DataFrame, date: datetime.datetime):
    # pull trade data from BBG
    tickers = list("/CUSIP/" + df['cusip'].unique())
    fields = ['PX_LAST']

    trade_data = blp.bdh(tickers, fields, date, date)
    trade_data = trade_data.transpose().droplevel(1)
    trade_data['cusip'] = trade_data.index.str[7:]
    trade_data.rename(columns={trade_data.columns[0]: 'px_last'}, inplace=True)
    df = df.merge(trade_data, on='cusip', how='left')

    #set the expected price equal to the previous price + the modeled price change
    df['expectation_method'] = 'Bloomberg'
    df['expected_price'] = df['px_last']

    return df

def RMBS_Agency(df: pd.DataFrame(), mktdata_diff: pd.DataFrame(), reportdate: datetime.datetime, calendar):
    trade_threshold = -2
    spread = 'mbs_index_index_z_spread_bp'

    # calculate expected movement due to price changes
    df = Get_Model_Price(df, mktdata_diff, spread)

    # pull trade data from BBG
    df = Get_TRACE(df)

    # if the last trade was within some threshold then overwrite the expected price with the last trade price
    df.loc[df['trace_time_of_trade'] >= reportdate - 2 * calendar, 'expectation_method'] = 'TRACE'
    df.loc[df['expectation_method'] == 'TRACE', 'expected_price'] = df['trace_last_trade_price']
    # todo:  Alter this methodology so the expected price is the last trade price +/- the modeled price movements since the last trade.

    return df

def RMBS(df: pd.DataFrame(), mktdata_diff: pd.DataFrame(), reportdate: datetime.datetime, calendar):
    trade_threshold = -2
    spread = 'mbs_index_index_z_spread_bp'

    # calculate expected movement due to price changes
    df = Get_Model_Price(df, mktdata_diff, spread)

    # pull trade data from BBG
    df = Get_TRACE(df)

    # if the last trade was within some threshold then overwrite the expected price with the last trade price
    df.loc[df['trace_time_of_trade'] >= reportdate - 2 * calendar, 'expectation_method'] = 'TRACE'
    df.loc[(df['expectation_method'] == 'TRACE') & (~pd.isna(df['trace_last_trade_price'])) , 'expected_price'] = df['trace_last_trade_price']
    # todo:  Alter this methodology so the expected price is the last trade price +/- the modeled price movements since the last trade.

    return df

def CMBS(df: pd.DataFrame(), mktdata_diff: pd.DataFrame(), reportdate: datetime.datetime, calendar):
    trade_threshold = -2
    spread = 'bbg_agg_cmbs_index_z_spread_bp'

    # calculate expected movement due to price changes
    df = Get_Model_Price(df, mktdata_diff, spread)

    # pull trade data from BBG
    #df = Get_TRACE(df)

    # if the last trade was within some threshold then overwrite the expected price with the last trade price
    #df.loc[df['trace_time_of_trade'] >= reportdate - 2 * calendar, 'expectation_method'] = 'TRACE'
    #df.loc[(df['expectation_method'] == 'TRACE') & (~pd.isna(df['trace_last_trade_price'])) , 'expected_price'] = df['trace_last_trade_price']
    # todo:  Alter this methodology so the expected price is the last trade price +/- the modeled price movements since the last trade.

    return df

def CMBS_Agency(df: pd.DataFrame(), mktdata_diff: pd.DataFrame(), reportdate: datetime.datetime, calendar):
    trade_threshold = -2
    spread = 'bbg_agg_agency_cmbs_index_z_spread_bp'

    # calculate expected movement due to price changes
    df = Get_Model_Price(df, mktdata_diff, spread)

    # pull trade data from BBG
    #df = Get_TRACE(df)

    # if the last trade was within some threshold then overwrite the expected price with the last trade price
    #df.loc[df['trace_time_of_trade'] >= reportdate - 2 * calendar, 'expectation_method'] = 'TRACE'
    #df.loc[df['expectation_method'] == 'TRACE', 'expected_price'] = df['trace_last_trade_price']
    # todo:  Alter this methodology so the expected price is the last trade price +/- the modeled price movements since the last trade.

    return df

def corp_ig(df: pd.DataFrame(), mktdata_diff: pd.DataFrame(), reportdate: datetime.datetime, calendar):

    trade_threshold = -2
    spread = 'corp_ig_index_z_spread_bp'
    # calculate expected movement due to price changes
    df = Get_Model_Price(df, mktdata_diff, spread)

    # pull trade data from BBG
    df = Get_TRACE(df)

    # if the last trade was within some threshold then overwrite the expected price with the last trade price
    df.loc[df['trace_time_of_trade'] >= reportdate - 2 * calendar, 'expectation_method'] = 'TRACE'
    df.loc[df['expectation_method'] == 'TRACE', 'expected_price'] = df['trace_last_trade_price']
    # todo:  Alter this methodology so the expected price is the last trade price +/- the modeled price movements since the last trade.

    return df

def corp_hy(df: pd.DataFrame(), mktdata_diff: pd.DataFrame(), reportdate: datetime.datetime, calendar):

    trade_threshold = -2
    spread = 'corp_hy_index_z_spread_bp'
    # calculate expected movement due to price changes
    df = Get_Model_Price(df, mktdata_diff, spread)

    # pull trade data from BBG
    df = Get_TRACE(df)

    # if the last trade was within some threshold then overwrite the expected price with the last trade price
    df.loc[df['trace_time_of_trade'] >= reportdate - 2 * calendar, 'expectation_method'] = 'TRACE'
    df.loc[df['expectation_method'] == 'TRACE', 'expected_price'] = df['trace_last_trade_price']
    # todo:  Alter this methodology so the expected price is the last trade price +/- the modeled price movements since the last trade.

    return df

def abs(df: pd.DataFrame(), mktdata_diff: pd.DataFrame(), reportdate: datetime.datetime, calendar):

    trade_threshold = -2
    spread = 'abs_index_z_spread_bp'
    # calculate expected movement due to price changes
    df = Get_Model_Price(df, mktdata_diff, spread)

    # pull trade data from BBG
    df = Get_TRACE(df)

    # if the last trade was within some threshold then overwrite the expected price with the last trade price
    df.loc[df['trace_time_of_trade'] >= reportdate - 2 * calendar, 'expectation_method'] = 'TRACE'
    df.loc[df['expectation_method'] == 'TRACE', 'expected_price'] = df['trace_last_trade_price']
    # todo:  Alter this methodology so the expected price is the last trade price +/- the modeled price movements since the last trade.

    return df

def abs_auto(df: pd.DataFrame(), mktdata_diff: pd.DataFrame(), reportdate: datetime.datetime, calendar):

    trade_threshold = -2
    spread = 'abs_auto_index_z_spread_bp'
    # calculate expected movement due to price changes
    df = Get_Model_Price(df, mktdata_diff, spread)

    # pull trade data from BBG
    df = Get_TRACE(df)

    # if the last trade was within some threshold then overwrite the expected price with the last trade price
    df.loc[df['trace_time_of_trade'] >= reportdate - 2 * calendar, 'expectation_method'] = 'TRACE'
    df.loc[df['expectation_method'] == 'TRACE', 'expected_price'] = df['trace_last_trade_price']
    # todo:  Alter this methodology so the expected price is the last trade price +/- the modeled price movements since the last trade.

    return df

def abs_card(df: pd.DataFrame(), mktdata_diff: pd.DataFrame(), reportdate: datetime.datetime, calendar):

    trade_threshold = -2
    spread = 'abs_card_index_z_spread_bp'
    # calculate expected movement due to price changes
    df = Get_Model_Price(df, mktdata_diff, spread)

    # pull trade data from BBG
    df = Get_TRACE(df)

    # if the last trade was within some threshold then overwrite the expected price with the last trade price
    df.loc[df['trace_time_of_trade'] >= reportdate - 2 * calendar, 'expectation_method'] = 'TRACE'
    df.loc[df['expectation_method'] == 'TRACE', 'expected_price'] = df['trace_last_trade_price']
    # todo:  Alter this methodology so the expected price is the last trade price +/- the modeled price movements since the last trade.

    return df

def cost(df: pd.DataFrame(), reportdate):
    # find the last trade for each cusip.
    cusips = rt.createStringOfIDs(df['cusip'].tolist())
    qry = f"select TradeDate, PortfolioID, cusip, TradePrice from dbo.trade where cusip in({cusips}) and date <= '{reportdate}'"
    print(qry)
    trades = rt.read_data('AOCA', qry)

    return df

def calc_mkt_value(df:pd.DataFrame, method:str, expected_mkt_val:str, expected_price:str, quantity:str):
    df.loc[df[method]=='per_100', expected_mkt_val] = df[expected_price] / 100 * df[quantity]
    df.loc[df[method] == 'absolute', expected_mkt_val] = df[expected_price] * df[quantity]

    return df