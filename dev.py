from nifty_tools import *
from datetime import timezone
import collection_tools.elite_corp as elite
import collection_tools.thedholes_tools as th
from collection_tools.sleeper_tools import *
import collection_tools.plsty_tools as plsty

col = NftCollection(collectionID='152a1dc8-515d-48b1-8457-298afd5a3a66')
col.get_collection_nfts(limit=col.get_item_count())

grab_new_blocks()
elite.run()