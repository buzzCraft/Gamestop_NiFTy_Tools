from nifty_tools import *
import nifty_database as nifty
import pandas as pd
from datetime import datetime
from tqdm import tqdm


def get_user_transaction_history(username=None, address=None):
    """
    Returns a pandas dataframe of the user's transaction history
    """
    if address is not None:
        user = User(address=address)
    else:
        user = User(username=username)
    nf = nifty.NiftyDB()
    print("Getting transaction history")
    trade_history = nf.get_user_trade_history(user.accountId)
    history = []
    last_timestamp = 0
    seen_nfts = []
    seen_data = []
    for row in tqdm(trade_history):
        timestamp = row['createdAt']
        time = datetime.fromtimestamp(timestamp)

        # Get the last sale price of the NFT
        # l_s_eth, l_s_usd = get_last_sale_price(row['nftId'])
        # Find the price of ETH at the time of the trade
        if timestamp != last_timestamp:
            eth_price = get_eth_price(timestamp)
        last_timestamp = timestamp
        id = row['nftId']
        # Check for nft collection name, minter name and floor price
        if id not in seen_nfts:
            collection, minter, floor = get_collection_name(id)
            seen_nfts.append(id)
            l_s_eth, l_s_usd = get_last_sale_price(id)
            seen_data.append([collection, minter, floor, l_s_eth, l_s_usd])
        # Only check once, and then use the cached data
        else:
            collection = seen_data[seen_nfts.index(id)][0]
            minter = seen_data[seen_nfts.index(id)][1]
            floor = seen_data[seen_nfts.index(id)][2]
            l_s_eth = seen_data[seen_nfts.index(id)][3]
            l_s_usd = seen_data[seen_nfts.index(id)][4]

        # Loop through the trade history and get the price of the NFT at the time of the trade
        if row['txType'] == 'Transfer':
            price, priceusd = get_sale_price_at_time(id, timestamp)
            if row['sellerAccount'] == user.accountId:
                # User got nft transferred to them
                t_row = [row['createdAt'], time, "transfer", row['buyer'], row['seller'], -row['amount'], row['name'],
                         row['nftData'], id, collection, minter, price, priceusd,  l_s_eth, l_s_usd, 0,0, floor, eth_price]
                history.append(t_row)
            elif row['buyerAccount'] == user.accountId:
                # User transferred to someone else
                t_row = [row['createdAt'], time, "transfer", row['buyer'], row['seller'], row['amount'], row['name'],
                         row['nftData'], row['nftId'], collection,minter, price, priceusd,  l_s_eth, l_s_usd, 0,0, floor, eth_price]
                history.append(t_row)
        elif row['txType'] == 'SpotTrade':
            if row['buyerAccount'] == user.accountId:
                # User bought NFT
                t_row = [row['createdAt'], time, "spot trade", row['buyer'], row['seller'], row['amount'], row['name'],
                         row['nftData'], id, collection,minter, row['price'], row['priceUsd'], l_s_eth, l_s_usd,
                         0,0,floor,eth_price]
                history.append(t_row)
            if row['sellerAccount'] == user.accountId:
                # User sold NFT
                t_row = [row['createdAt'], time, "spot trade", row['buyer'], row['seller'], -row['amount'], row['name'],
                         row['nftData'], id, collection,minter, row['price'], row['priceUsd'], l_s_eth, l_s_usd,
                         0,0,floor,eth_price]
                history.append(t_row)
    return history


def get_collection_name(nftId):
    """
    Returns the name of the collection an NFT belongs to
    """
    nft = Nft(nftId)
    col = NftCollection(nft.data['collectionId'])
    if col.metadata == None:
        col.get_collection_metadata()
    minter = User(address=nft.data['creatorEthAddress']).username
    floor = nft.get_lowest_price()
    try:
        if col.metadata['name'] == "None":
            return "Unknown", floor
        return col.metadata['name'],minter, floor
    except:
        raise Exception(f"Error getting collection name for collection {nft.data['collectionId']}")


def get_sale_price_at_time(nft_id, timesteamp=None):
    """
    Returns the sale price of an NFT at a given time
    """
    # TODO THIS IS SLOW AF
    nf = nifty.NiftyDB()
    return nf.get_nft_sale_at_time(nft_id,timesteamp)

    # trade_history = nf.get_nft_trade_history(nft_id)
    # for i in range(len(trade_history)):
    #     if trade_history[i]['createdAt'] > timesteamp:
    #         for j in range(i):
    #             if trade_history[i - j - 1]['txType'] == 'SpotTrade':
    #                 price = trade_history[i - j - 1]['price']
    #                 price_usd = trade_history[i - j - 1]['priceUsd']
    #                 # return price
    #                 return price, price_usd
    # # Incase the nft was never sold ( Happens with some airdrops
    # return 0, 0


def get_last_sale_price(nft_id):
    """
    Returns the last sale price of an NFT
    """
    nf = nifty.NiftyDB()
    last_sale = nf.get_last_sales_price(nft_id)
    if last_sale == None:
        return 0, 0
    return last_sale['price'], last_sale['priceUsd']
    # trade_history = nf.get_nft_trade_history(nft_id)
    # # Reverse trade_history
    # trade_history = trade_history[::-1]
    # for i in range(len(trade_history)):
    #     if trade_history[i]['txType'] == 'SpotTrade':
    #         price = trade_history[i]['price']
    #         price_usd = trade_history[i]['priceUsd']
    #         return price, price_usd
    # # Incase the nft was never sold ( Happens with some airdrops )
    # return 0, 0


def get_eth_price(time):
    """
    A function to return ETH/USD at a given time
    """
    nf = nifty.NiftyDB()
    eth_price = nf.get_historical_price("ETH", time, print_str=False)
    return eth_price


def generate_wallet_report(address):
    """
    Generates a report of all the nfts in a wallet
    :param address: The address of the wallet
    :return: A pandas dataframe with the report
    """
    user = User(address=address)
    holdings = get_user_holding(user)
    holdings['collection_name'] = ""
    holdings['nr_sold'] = 0
    holdings.rename(columns={'number_owned': 'nr_owned', 'total_number': 'minted'}, inplace=True)
    holdings = holdings[['name',  'nr_owned', 'nr_sold', 'minted', 'nftId', 'collection_name']]
    print(f"Generating wallet report for {user.username}")
    print(f"Grabbing user transaction history")
    x = get_user_transaction_history(address=address)
    col = ['unixTimeStamp', 'time', 'type', 'buyer', 'seller', 'amount', 'name', 'nftData', 'nftId', 'collection', 'creator',
           'price', 'priceUsd', "lastSalePrice","lastSalePriceUsd","tot_order_value","tot_order_value_usd",'floor', 'ethPrice']

    df = pd.DataFrame(x, columns=col)

    df['tot_order_value'] = df['amount'] * df['price']
    df['tot_order_value_usd'] = df['amount'] * df['priceUsd']
    # Pair nftId in holdings with nftId in df
    holdings['collection_creator'] = None
    holdings["paid"] = 0
    holdings["paidUsd"] = 0
    holdings["AvgPrice"] = 0
    holdings["AvgPriceUsd"] = 0
    holdings["sold"] = 0
    holdings["soldUsd"] = 0
    holdings["profit_from_sale"] = 0
    holdings["profit_from_sale_usd"] = 0
    holdings["last_sale_price"] = 0
    holdings["last_sale_priceUsd"] = 0
    holdings["value"] = 0
    holdings["valueUsd"] = 0
    holdings["floor"] = 0

    # For transactions where the user has sold the NFT and it don't show in the holdings, add it to the holdings
    for idx, row in df.iterrows():
        if row['type'] == 'spot trade' and row['seller'] == user.username:
            nft_id = row['nftId']
            if nft_id not in holdings['nftId'].values:
                print(f"Adding {row['name']} to holdings")
                nft = Nft(nft_id)
                nft.get_nft_data()
                # Add the nft with holding = 0
                holdings = holdings.append(
                    {'nftId': nft_id, 'name': row['name'], 'nr_owned': 0, 'nr_sold': 0, 'minted': nft.data['amount'],
                     'paid': 0, 'paidUsd': 0,
                     'AvgPrice': 0, 'AvgPriceUsd': 0, 'sold': 0, 'soldUsd': 0, 'profit_from_sale': 0,
                     'profit_from_sale_usd': 0,
                     'value': 0, 'valueUsd': 0}, ignore_index=True)

    # Create a pairing between holdings and trade history

    # For each row in holding, find the trades corresponding to that NFT
    for idx, row in holdings.iterrows():
        print(f"Processing nft {idx} of {len(holdings)}")
        # Get nftId
        nft_id = row['nftId']
        # Make a filtered trade list, only containing that nft
        new = df[df['nftId'] == nft_id]
        if len(new['floor'].values) != 0:
            holdings.at[idx, 'floor'] = new['floor'].values[0]

        col_name = None

        # For each row in the filtered trade list, add the price to the paid column
        for id, ro in new.iterrows():

            col_name = ro['collection']
            minter = ro['creator']
            if ro["type"] == 'spot trade' and ro["buyer"] == user.username:
                holdings.at[idx, 'paid'] += abs(ro['price'] * ro['amount'])
                holdings.at[idx, 'paidUsd'] += abs(ro['priceUsd'] * ro['amount'])
            if ro["type"] == 'spot trade' and ro["seller"] == user.username:
                holdings.at[idx, 'sold'] += abs(ro['price'] * ro['amount'])
                holdings.at[idx, 'soldUsd'] += abs(ro['priceUsd'] * ro['amount'])
                holdings.at[idx, 'nr_sold'] -= ro['amount']
            last_sale = ro['lastSalePrice']
            last_sale_usd = ro['lastSalePriceUsd']
        # if col_name is None:
        #     get_collection_name(row['nftId'])
        try:
            holdings.at[idx, 'collection_name'] = col_name

            holdings.at[idx, 'collection_creator'] = minter
            nr_owned = row['nr_owned']
            holdings.at[idx, 'value'] = last_sale * nr_owned
            holdings.at[idx, 'valueUsd'] = last_sale_usd * nr_owned
            # if row['nr_owned'] != 0:
            bought = holdings.at[idx, 'nr_owned'] + holdings.at[idx, 'nr_sold']
            holdings.at[idx, 'AvgPrice'] = holdings.at[idx, 'paid'] / bought
            holdings.at[idx, 'AvgPriceUsd'] = holdings.at[idx, 'paidUsd'] / bought
            holdings.at[idx, 'last_sale_price'] = last_sale
            holdings.at[idx, 'last_sale_priceUsd'] = last_sale_usd
            if holdings.at[idx, 'sold'] != 0:
                holdings.at[idx, 'profit_from_sale'] = holdings.at[idx, 'sold'] - holdings.at[idx, 'AvgPrice']
                holdings.at[idx, 'profit_from_sale_usd'] = holdings.at[idx, 'soldUsd'] - holdings.at[idx, 'AvgPriceUsd']
        except:
            print(f"Error processing a nft")


    # Add a summation at the end of the df
    total = {'nftId': '', 'name': '', 'nr_owned': holdings['nr_owned'].sum(), 'paid': holdings['paid'].sum(),
             'paidUsd': holdings['paidUsd'].sum(), 'sold': holdings['sold'].sum(), 'soldUsd': holdings['soldUsd'].sum(),
             'profit_from_sale': holdings['profit_from_sale'].sum(),
             'profit_from_sale_usd': holdings['profit_from_sale_usd'].sum(),
             'value': holdings['value'].sum(), 'valueUsd': holdings['valueUsd'].sum()}

    # Convert dict to dataframe
    total = pd.DataFrame(total, index=["TOTAL"])

    holdings = pd.concat([holdings, total])

    print(f'Done processing transactions')
    # Write this to sheet 2 of the excel file
    df_history = df.drop(columns=['unixTimeStamp'])

    # Write to excel
    with pd.ExcelWriter(f'{user.username}_wallet_report.xlsx') as writer:

        holdings.to_excel(writer, sheet_name='Holdings')

        df_history.to_excel(writer, sheet_name='Transaction History')
        print(f'Wrote to excel file as {user.username}_wallet_report.xlsx')
    # writer = pd.ExcelWriter(f'{user.username}_wallet_report.xlsx', engine='xlsxwriter')
    # holdings.to_excel(writer, sheet_name='Holdings')
    # df_history.to_excel(writer, sheet_name='Transaction History')
    # writer.save()

    print("Done")


def get_user_holding(user):
    """
    Returns a dataframe with the current holdings of a user
    :param df: A dataframe with the transaction history of a user
    :return: A dataframe with the current holdings of a user
    """

    x = user.get_owned_nfts()
    df = pd.DataFrame(x)

    return df


if __name__ == "__main__":
    # nf = db.NiftyDB()

    RAC = "0xc6cf45a9e87f5732f329882ddae846e1447d995b"
    BUZ = "0x9D44E07C8d1EF2B8d12dBc9A32d908fA3D75D626"
    MRG = "0xE7B3d9797e23B3681109c7C5c608f251005552F9"
    POP = "0x914697ff477a581515dff906bdeeaa7d4212ef09"
    TANK = "0x9fb5b00bdeb34f725795fd83769237916f165c1e"
    b = "0x69fa84a9435446dca8d154dfd3b2fe5bb473ce3e"
    n = "0xe735AD427C1F3b5909D7Cc12aF24486D03eD51cD"
    LOS = "0xc651a1ddbe7f7a4fefdf154d564d9533d60c31b2"
    EME = "0x6128c7d0231b0c3531c25db51611e5e71cc36971"
    T = "0xda4cf25af5551459234cf48044610f23d3e66ca8"
    BeMoreKind = "0xbdc30613b0de0c072b3b35af4eff7240bd7b6ef4"
    EGOULD = "0x77fab5ec0a101c3df8d83028f5380962292dd902"
    ####
    cc ="0x77B9dc71237dcA5fc0F1158a9fe7cEdF24157d50"


    # grab_new_blocks()
    time = datetime.now()

    generate_wallet_report(RAC)



#TODO: Add value transferre+d out/in
#TODO: When transfering out, substract avrage price from bought value
#TODO: Add fees and royalties
