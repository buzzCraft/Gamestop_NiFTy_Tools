from nifty_tools import *
from datetime import timezone
from run import data_drop

def run():
    Eng = "ffaf27fb-84a8-47d0-a1cd-78587410b0c2"

    data_drop(Eng, show=False, subfolder="Engwind")
    S_s = "25c7b5a7-6348-479c-becf-12000a5d5599"
    data_drop(S_s, show=False, subfolder="Engwind\\space")

def identify_transfers():
    # Get the holders list from around
    # 1676419200
    # Wed Feb 15 2023 00:00:00 GMT+0000

    # From that date look at transfers only
    # If an account transfer an item, check how much that 