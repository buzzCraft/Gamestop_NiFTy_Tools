import nifty_database as nifty
import loopring_api as loopring
from nft_ids import *
from gamestop_api import Nft, User
import pandas as pd
from datetime import datetime
import plotly.graph_objects as go
import plotly.subplots as sp
import xlsxwriter


def get_all_transactions():
    nf = nifty.NiftyDB()
    transactions = nf.get_all_nft_trades()
    columns = ['blockId', 'createdAt', 'txType', 'nftData', 'sellerAccount', 'buyerAccount', 'amount', 'price', 'priceUsd']
    df = pd.DataFrame(transactions, columns=columns)
    # Convert to datetime form unix timestamp
    df['createdAt'] = pd.to_datetime(df['createdAt'], unit='s')
    df['month'] = df['createdAt'].dt.month
    df['year'] = df['createdAt'].dt.year
    df['week'] = df['createdAt'].dt.week
    df['day'] = df['createdAt'].dt.day

    # df['createdAt'] = pd.to_datetime(df['createdAt'], unit='ms')
    return df


def plot_sum(df):
    vol = get_volume(df)
    uniq = get_unique_users(df)
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=vol['date'], y=vol['price'], mode='lines', name='Sum ETH', yaxis='y2'))
    fig.add_trace(go.Scatter(x=vol['date'], y=vol['priceUsd'], mode='lines', name='Sum USD'))
    fig.add_trace(go.Bar(x=vol['date'], y=vol['txType'], name='Number of transactions', yaxis='y3', opacity=0.5))
    fig.add_trace(go.Scatter(x=uniq['date'], y=uniq['uniqueUsers'], mode='lines', name='Unique Users', yaxis='y4'))
    # Name y1 axis
    fig.update_yaxes(title_text="Sum USD", tickprefix="$")
    fig.update_layout(yaxis2=dict(title='Sum ETH', side='left', overlaying='y', position=0.15, showgrid=False, tickprefix="Ξ"))
    fig.update_layout(yaxis4=dict(title='Unique Users', side='right', overlaying='y', position=0.95, showgrid=False))
    fig.update_layout(yaxis3=dict(title='Sum transactions', side='right', overlaying='y', showgrid=False), template="plotly_dark")
    # Give the fig a title
    fig.update_layout(title_text='Day by day sum of ETH, USD and number of transactions on GSMP')
    fig.show()
    fig.write_html("file.html")


def get_unique_users(df):
    # From the df, for each day in the month, get the unique users based on the seller and buyer account
    df = df.groupby(['day', 'month', 'year']).agg({'sellerAccount': 'unique', 'buyerAccount': 'unique'})
    # Convert index to datetime
    df = df.sort_values(by=['year', 'month', 'day'])
    # Split the index into 3 columns
    df['day'] = df.index.get_level_values(0)
    df['month'] = df.index.get_level_values(1)
    df['year'] = df.index.get_level_values(2)
    # Create a datetime column
    df['date'] = pd.to_datetime(df[['year', 'month', 'day']])
    # Get the number of unique users
    df['uniqueUsers'] = df['sellerAccount'].apply(lambda x: len(x)) + df['buyerAccount'].apply(lambda x: len(x))
    return df

def get_volume(df):
    # Sort df by, year, month, day

    df = df.groupby(['day', 'month', 'year']).agg({'txType': 'count', 'price': 'sum', 'priceUsd': 'sum'})
    df = df.sort_values(by=['year', 'month', 'day'])
    # Split the index into 3 columns
    df['day'] = df.index.get_level_values(0)
    df['month'] = df.index.get_level_values(1)
    df['year'] = df.index.get_level_values(2)
    # Create a datetime column
    df['date'] = pd.to_datetime(df[['year', 'month', 'day']])
    return df



def run():
    df = get_all_transactions()
    plot_sum(df)
    # print(df)