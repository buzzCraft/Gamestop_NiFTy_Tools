import collection_tools.thedholes_tools as th
from nifty_tools import *



def get_snap_thedholes():
    time = datetime.now()
    # Thedholes
    combolist = th.TH_LIST
    print("Starting ThedHoles")
    th.get_holders_for_list_at_time(combolist, time, filename="Thedholes")
    th.get_subscription_count(combolist, time)

if __name__ == "__main__":
    grab_new_blocks(find_new_users=True)
    get_snap_thedholes()


