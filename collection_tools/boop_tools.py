

from nifty_tools import *
from tqdm import tqdm
from datetime import timezone


BOOP_AND_FRENS = "50630ce6-40f9-4f09-bfa5-b7414496dcd4"


def get_holders_at_time_for_nft(nftId, timestamp):
    db = nifty.NiftyDB()

    nft = Nft(nftId)
    tx = db.get_nft_transactions(nft.get_nft_data())
    df = pd.DataFrame(tx, columns=['blockId', 'createdAt', 'txType', 'nftData', 'sellerAccount', 'buyerAccount',
                                   'amount', 'price', 'priceUsd'])
    df = df[df.createdAt < timestamp.timestamp()]
    holders = dict()
    # dfs = pd.DataFrame()
    # Go through tx and save holder balances

    # i = 0
    holders_purged = {}
    for idx, tx in df.iterrows():
        # i += 1

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

    name = f'{nft.data["name"]}'



    return [holders_purged, name]

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

    for nftId in tqdm(nft_id_list):

        dict, name= get_holders_at_time_for_nft(nftId, time)
        name_l.append(name)
        d_list.append(dict)
    df = pd.DataFrame(d_list)
    df = df.T

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

    # Keep only address, username and the sum
    df = df.iloc[:, [0, 1, -1]]
    # Add colums for tier 1 to 6
    df.insert(2, 'Tier 1', "")
    df.insert(3, 'Tier 2', "")
    df.insert(4, 'Tier 3', "")
    df.insert(5, 'Tier 4', "")
    df.insert(6, 'Tier 5', "")
    df.insert(7, 'Tier 6', "")
    # For tier 1, if the sum is == 1, then the user is in tier 1
    df.loc[df['Sum'] == 1, 'Tier 1'] = 1
    # For tier 2, if the sum is 2<=sum<=3, then the user is in tier 2
    df.loc[(df['Sum'] >= 2) & (df['Sum'] <= 3), 'Tier 2'] = 1
    # For tier 3, if the sum is 4<=sum<=5, then the user is in tier 3
    df.loc[(df['Sum'] >= 4) & (df['Sum'] <= 5), 'Tier 3'] = 1
    # For tier 4, if the sum is 6<=sum<=9, then the user is in tier 4
    df.loc[(df['Sum'] >= 6) & (df['Sum'] <= 9), 'Tier 4'] = 1
    # For tier 5, if the sum is 10<=sum<=15, then the user is in tier 5
    df.loc[(df['Sum'] >= 10) & (df['Sum'] <= 15), 'Tier 5'] = 1
    # For tier 6, if the sum is >=16, then the user is in tier 6
    df.loc[df['Sum'] >= 16, 'Tier 6'] = 1
    # Fill the empty cells with 0
    df.fillna(0, inplace=True)
    df.loc['Total'] = df.iloc[:, 2:].sum()

    if export_to_excel:
        path = f'Snap\\{date}\\'
        if not os.path.exists(path):
            # Create a new directory because it does not exist
            os.makedirs(path)

        df.to_excel(path + f'{filename} {timestamp}.xlsx')
    elif get_df:
        return df

def run():
    print("Running Boop And Frens")
    col = NftCollection(collectionID=BOOP_AND_FRENS)
    # Scrap the nft_id from the collection
    col.get_collection_nfts(limit=col.get_item_count())
    # Generate snapshot for nft_id_list at time = time
    print(f"Generating holders list for {col.metadata['name']}")
    time = datetime.now()
    get_holders_for_list_at_time(nft_id_list=col.get_nftId_list(), time=time, filename="BOOP_TIERS", export_to_excel=True)


if __name__ == "__main__":

    print("Can't run this file directly")