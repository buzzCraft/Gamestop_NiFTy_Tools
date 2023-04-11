from nifty_tools import *
from datetime import timezone
import collection_tools.elite_corp as elite
import collection_tools.boop_tools as boop
import collection_tools.emerge_tools as emerge
import collection_tools.thedholes_tools as th
import collection_tools.domi_tools as do
from collection_tools.sleeper_tools import *
import collection_tools.plsty_tools as plsty
import collection_tools.elite_corp as elite
from mp_tools.stats import run
from run import *


# col = NftCollection(collectionID='152a1dc8-515d-48b1-8457-298afd5a3a66')
# col.get_collection_nfts(limit=col.get_item_count())
# plot_eth_volume(col.get_nftId_list(), [1, 7, 0], save_file=True, show_fig=False, file_name="EVA_and_Nate_Eth_Volume")
weez = "7cf0a0ed-fb9e-4522-9ed5-bc38aa1e6b74"

grab_new_blocks()

# elite.airdrop()
# boop.run()
# # do.run()
emerge.emerge_data(total_holders=True, show=False, subfolder="Emerge")



