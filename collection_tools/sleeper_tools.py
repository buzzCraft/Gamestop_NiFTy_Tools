from nifty_tools import *
from datetime import datetime, timezone
import pandas as pd



SLEEPERS = "377384d7-3702-4205-a204-0e852931f472"
ZERODAY = "8fbe7753-e88d-4180-b431-783178d256a1"
SLEEPERSYS = "aebdd3fc-7973-40f9-8149-f31eb5364df4"
BREACH = "814f4323-6a6b-4dac-ab01-dc81dd6e8eed"

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


    return [holders_purged, nft.data["name"]]

def get_holders_for_list_at_time(nft_id_list, time, filename="none", export_to_excel=True, get_df=False):

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

        dict, name = get_holders_at_time_for_nft(nftId, time)

        name_l.append(name)
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
    if export_to_excel:
        path = f'Snap\\{date}\\'
        if not os.path.exists(path):
            # Create a new directory because it does not exist
            os.makedirs(path)

        df.to_excel(path + f'{filename} {timestamp}.xlsx')
    elif get_df:
        return df


def snapshot():
    time = datetime(2023, 3, 31, 19, 0, tzinfo=timezone.utc)
    print(time)
    s = NftCollection(collectionID=SLEEPERS)
    # Scrap the nft_id from the collection
    s.get_collection_nfts(limit=s.get_item_count())

    s2 = NftCollection(collectionID=SLEEPERSYS)
    # Scrap the nft_id from the collection
    s2.get_collection_nfts(limit=s2.get_item_count())


    combolist = s.get_nftId_list()
    combolist.extend(s2.get_nftId_list())


    get_holders_for_list_at_time(nft_id_list=combolist, time=time, filename="Sleepers")