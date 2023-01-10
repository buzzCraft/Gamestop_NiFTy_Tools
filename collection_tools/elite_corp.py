from nifty_tools import *
from datetime import timezone


ELIT_CORP = "55bae9ce-9c03-4a63-a162-3e02659de5c6"
ELIT_CORP_AIR = "dde5c4e6-9ce6-4a72-af4c-52cad986b088"
XMAS = "13472dc9-1db1-4ba2-b1d9-ea87987e338e"
GUILD_LIST = ["Sand Fox",  "Hit Beez", "B-Hawkz"]


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
        team = nft.data['attributes'][0]['value']
    return [holders_purged, nft.data["name"], team]

def get_holders_for_list_at_time(nft_id_list, time, calculate_sets = False, filename="none", export_to_excel=True, get_df=False):

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

    if calculate_sets:
        for guild in GUILD_LIST:
        # Find columns ending with "Sand Fox"
            g = [col for col in df.columns if col.endswith(guild)]
            # See if both columns have values != 0
            if df[g[0]].sum() > 0 and df[g[1]].sum() > 0:
                # Add a new colum with the minimum value
                df[guild] = df[g].min(axis=1)
        # If all guild colums are != 0, add a new column with the minumum value
        if df['Sand Fox'].sum() > 0 and df['Hit Beez'].sum() > 0 and df['B-Hawkz'].sum() > 0:
            df['Full set'] = df[['Sand Fox', 'Hit Beez', 'B-Hawkz']].min(axis=1)

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

def run():
    print("Running Elite Corp")
    nft_list = []
    col = NftCollection(collectionID=ELIT_CORP)
    # Scrap the nft_id from the collection
    col.get_collection_nfts(limit=col.get_item_count())
    col2 = NftCollection(collectionID=ELIT_CORP_AIR)
    # Scrap the nft_id from the collection
    col2.get_collection_nfts(limit=col2.get_item_count())
    # Generate snapshot for nft_id_list at time = time
    xmas = NftCollection(collectionID=XMAS)
    # Scrap the nft_id from the collection
    xmas.get_collection_nfts(limit=xmas.get_item_count())

    nft_list.extend(col2.get_nftId_list())
    nft_list.extend(xmas.get_nftId_list())
    print(f"Generating holders list for {col.metadata['name']}")

    time = datetime.now()
    get_holders_for_list_at_time(nft_id_list=col.get_nftId_list(), time=time, calculate_sets=True,filename="Elite Corp", export_to_excel=True)
    get_holders_for_list_at_time(nft_id_list=nft_list, time=time, filename="Elite Corp AirDrop + Xmas",
                                 export_to_excel=True)
    plot_eth_volume(nft_list+col.get_nftId_list() , [1, 7, 0], show_fig=False,save_file=True, file_name="EliteCorp", subfolder="Elite Corp")
    nft_list.extend(col.get_nftId_list())
    for nft in nft_list:
        plot_price_history(nft, usd=True, show_fig=False, save_file=True, plt_current_floor=True, subfolder="Elite Corp")


if __name__ == "__main__":

    print("Can't run this file directly")