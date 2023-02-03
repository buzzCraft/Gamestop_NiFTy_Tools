from nifty_tools import *
import plotly.express as px
import datetime

def get_holders_at_time_for_nft(nftId, timestamp):

    db = nifty.NiftyDB()

    nft = Nft(nftId)
    tx = db.get_nft_transactions(nft.get_nft_data())
    df = pd.DataFrame(tx, columns=['blockId', 'createdAt', 'txType', 'nftData', 'sellerAccount', 'buyerAccount',
                                   'amount', 'price', 'priceUsd'])
    df = df[df.createdAt < timestamp.timestamp()]
    holders = dict()
    dfs = pd.DataFrame()
    # Go through tx and save holder balances

    i = 0
    holders_purged = {}
    for idx, tx in df.iterrows():
        i += 1

        if tx['buyerAccount'] not in holders:
            holders[tx['buyerAccount']] = tx['amount']
        else:
            holders[tx['buyerAccount']] += tx['amount']
        if tx['sellerAccount'] not in holders:
            holders[tx['sellerAccount']] = -1 * tx['amount']
        else:
            holders[tx['sellerAccount']] -= tx['amount']

        # Remove holders with 0 balance
        holders_purged = {k: v for k, v in holders.items() if v > 0}

        # Get the first value in nft.data['attributes']
        nft.get_nft_data()
        try:
            team1 = nft.data['attributes']
            team = team1.get("COLOR")
            if not team:
                team = team1.get("Color")
        except:
            team = " "
        if not team:
            team = " "

    return [holders_purged, nft.data["name"], team]

def get_holders_for_list_at_time(nft_id_list, time, filename="DomiTotal", export_to_excel=True, get_df=False):

    """
    Take a list of nft_id, and a datetime object and calculate
    holders for that list at that time
    Usage: Example for ThedHoles
    time = datetime.now()-timedelta(days = 1)
    get_holders_for_list_at_time(nft_id_list=TH_LIST, time=time, name="ThedHoles Collection Ownership")
    """
    d_list = []
    name_l = []

    for nftId in nft_id_list:

        dict, name, team = get_holders_at_time_for_nft(nftId, time)

        name_l.append(name + " " + team)
        d_list.append(dict)
    df = pd.DataFrame(d_list)
    df = df.T

    df.columns = name_l
    df.fillna(0, inplace=True)
    df['Sum'] = df.sum(axis=1)
    df.insert(0, 'address', "")
    df.insert(1, 'username', "")


    for idx, row in df.iterrows():
        user = User(accountId=idx)
        df.at[idx, 'address'] = user.address
        df.at[idx, 'username'] = user.username

    df.sort_values(by=['Sum'], ascending=False, inplace=True)
    timestamp = time.strftime("%Y-%m-%d %H-%M")
    date = time.strftime("%Y-%m-%d")

    # Add a final row with the sum, excluding the address and username columns
    df.loc['Total'] = df.iloc[:, 2:].sum()





    if export_to_excel:
        path = f'Snap\\{date}\\'
        if not os.path.exists(path):
            # Create a new directory because it does not exist
            os.makedirs(path)

        df.to_excel(path + f'{filename} {timestamp}.xlsx')
    elif get_df:
        return df

def get_nft_id_list_from_collection(collection_id):
    col = NftCollection(collectionID=collection_id)
    # Scrap the nft_id from the collection
    col.get_collection_nfts(limit=col.get_item_count())
    return col.get_nftId_list()

def run():
    REBORN = "2f4d469c-2128-475c-8c53-4a2020689cc9"
    # DOMI2D = "4ba84ceb-ce77-4256-bf46-44d92c12c2d4"
    # REBORN2 = "57a9b61b-4458-4eab-a856-86859df9c84c"

    nft_id_list = []
    nft_id_list.extend(get_nft_id_list_from_collection(REBORN))
    # nft_id_list.extend(get_nft_id_list_from_collection(DOMI2D))
    # nft_id_list.extend(get_nft_id_list_from_collection(REBORN2))
    get_holders_for_list_at_time(nft_id_list, datetime.datetime.now(), filename="DomiTotal", export_to_excel=True, get_df=False)
