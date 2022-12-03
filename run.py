from nifty_tools import *
from datetime import timezone
import collection_tools.emerge_tools as emerge
import collection_tools.thedholes_tools as th
from collection_tools.sleeper_tools import *
import collection_tools.elite_corp as elite
import collection_tools.plsty_tools as plsty

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

def get_time_from_timestamp(timestamp):
    return datetime.fromtimestamp(t, tz=timezone.utc)


def data_drop(collection_id, show = True, subfolder = None, time = datetime.now()):
    col = NftCollection(collectionID=collection_id)
    # Scrap the nft_id from the collection
    col.get_collection_nfts(limit=col.get_item_count())
    # Generate snapshot for nft_id_list at time = time
    print(f"Generating holders list for {col.metadata['name']}")
    get_holders_for_list_at_time(nft_id_list=col.get_nftId_list(), time=time, filename=col.metadata["name"])
    print(f"Plotting ETH volume for {col.metadata['name']}")
    plot_eth_volume(col.get_nftId_list(), [1,30,0], show_fig=show, save_file=True, subfolder=subfolder)

    for nft in col.get_nftId_list():
        print(f"Saving chart for {nft} ")
        plot_price_history(nft,limit_volume=False, save_file=True, plt_current_floor=True, show_fig=show, subfolder=subfolder)

def snap_shot(collection_id, time=datetime.utcnow()):
    col = NftCollection(collectionID=collection_id)
    # Scrap the nft_id from the collection
    col.get_collection_nfts(limit=col.get_item_count())
    # Generate snapshot for nft_id_list at time = time
    get_holders_for_list_at_time(nft_id_list=col.get_nftId_list(), time=time, filename=col.metadata["name"])
    for nft in col.get_nftId_list():
        print(f"Saving {nft} holder")
        save_nft_holders(nft)

def morning_routine():
    Eng = "ffaf27fb-84a8-47d0-a1cd-78587410b0c2"
    EVA_VESSEL = "6aaa8fe2-ef7f-4fac-9509-d22f939e4e16"
    EVA_EVA = "d1b7b3fd-dc44-495b-af14-4bf8d72a8b1b"
    EVA_NATE = "0cd5edc0-ed69-4b12-8029-8bb7d9328862"
    KIRA_TREE = 	"7ffa3d1a-8d57-44a5-96ce-78842257f10b"

    # Emerge
    time = datetime.now()
    print("Starting Emerge")
    emerge.emerge_data(total_holders=True, show=False, subfolder="Emerge")

    # Thedholes
    combolist = th.TH_LIST
    subfolder = "ThedHoles"
    print("Starting ThedHoles")
    th.get_holders_for_list_at_time(combolist, time)
    th.get_subscription_count(combolist, time)
    plot_eth_volume(combolist, [1, 7, 0], save_file=True, show_fig=False, file_name="ThedHolesEthHist",
                    subfolder=subfolder)
    for e in combolist:
        plot_price_history(e, usd=False, show_fig=False, save_file=True, plt_current_floor=True, subfolder=subfolder)
    #
    # Engwind

    data_drop(Eng, show=False, subfolder="Engwind")
    S_s = "25c7b5a7-6348-479c-becf-12000a5d5599"
    data_drop(S_s, show=False, subfolder="Engwind\\space")
    print("Starting Kira")
    # Kira
    combolist = [EVA_NATE, EVA_EVA, EVA_VESSEL, KIRA_TREE]
    subfolder = "Kira"
    plot_eth_volume(combolist, [1, 7, 0], save_file=True, show_fig=False, subfolder=subfolder,
                    file_name="EVA_and_Nate_Eth_Volume")
    # plot_items_per_wallet(combolist)

    for e in combolist:
        plot_price_history(e, usd=False, show_fig=False, save_file=True, plt_current_floor=True, subfolder=subfolder)
    get_holders_for_list_at_time(combolist, time, filename="EVA_and_Nate_Ownership")
    # Batlepass
    print("Starting Battlepass")
    Kira_BP_COL_ID = "c1978e57-5a91-4d6b-81e8-21ea66d8380f"
    data_drop(Kira_BP_COL_ID, show=False, subfolder="Battlepass")

    elite.run()


    print("Starting CC")
    col = NftCollection(collectionID=CC_COLLECTION_ID)
    # Scrap the nft_id from the collection
    col.get_collection_nfts(limit=col.get_item_count())
    cel =NftCollection(collectionID=CC_CELEBRATION_ID)
    cel.get_collection_nfts(limit=cel.get_item_count())
    airdrop = NftCollection(collectionID=CC_AIRDROP_ID)
    airdrop.get_collection_nfts(limit=airdrop.get_item_count())
    for n in col.get_nftId_list():
        plot_price_history(n, usd=True, show_fig=False, save_file=True, plt_current_floor=True, subfolder="CC")
    for n in cel.get_nftId_list():
        plot_price_history(n, usd=True, show_fig=False, save_file=True, plt_current_floor=True, subfolder="CC_CELEBRATION")
    for n in airdrop.get_nftId_list():
        plot_price_history(n, usd=True, show_fig=False, save_file=True, plt_current_floor=True, subfolder="CC_AIRDROP")


    # Kick-ass
    print("Starting KickAss")
    kickass()

def kickass():
    KICKASS_AIRDROP1= "cacf6464-d9d9-48be-9aac-d0ae81fdf5f1"
    KICKASS_2 ="d421177f-c183-4b42-83d1-aaf01539e604"
    KICKASS_1 = "09cf5dc0-47b9-4970-9487-0770d2f1142b"
    data_drop(KICKASS_1, show=False, subfolder="KickAss1")
    data_drop(KICKASS_2, show=False, subfolder="KickAss2")
    data_drop(KICKASS_AIRDROP1, show=False, subfolder="KickAssAirdrop1")

def inequity():
    inequity_air = "0f0247b3-3838-4456-9d0e-1bba0eba05ec"
    inequity = "a6873f0d-1d81-4cc8-8716-019c3a23f17b"
    data_drop(inequity_air, show=False, subfolder="inequity_air")
    data_drop(inequity, show=False, subfolder="inequity")



if __name__ == '__main__':

    grab_new_blocks()
    morning_routine()
    # # data_drop(PLS_COLLECTION_ID, show=False, subfolder="PLS")





