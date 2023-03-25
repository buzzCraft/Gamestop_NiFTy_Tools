import requests
from ratelimit import limits, sleep_and_retry
from concurrent.futures import ThreadPoolExecutor, as_completed
import gamestop_api
import nifty_database as nifty
import yaml
import time
from random import randint

MAX_INPUT = 13
API_COUNTER = 0

class LoopringAPI:
    def __init__(self):
        with open('config.yml', 'r') as config:
            self.config = yaml.safe_load(config)['loopring']

        self.api_keys = self.config['api_keys']
        number_of_keys = len(self.api_keys)-1
        self.lr = requests.session()
        self.lr.headers.update({
            'Accept': 'application/json',
            'X-API-KEY': self.api_keys[randint(0, number_of_keys)]
        })

    @sleep_and_retry
    @limits(calls=5, period=1)
    def get_user_nft_balance(self, accountId):
        limit = 50
        offset = 0
        data = []
        while True:
            api_url = (f"https://api3.loopring.io/api/v3/user/nft/balances?accountId={accountId}"
                       f"&offset={offset}&limit={limit}")
            try:
                response = self.lr.get(api_url).json()
                data.extend(response['data'])
                if response['totalNum'] == 0:
                    break
                offset += limit
            except requests.exceptions.JSONDecodeError:
                continue

        return data

    def get_accountId_from_address(self, address):
        api_url = f"https://api3.loopring.io/api/v3/account?owner={address}"
        response = self.lr.get(api_url).json()
        if 'accountId' in response:
            return response['accountId']
        else:
            return None

    def get_nft_inventory_for_address(self, address):
        accountId = self.get_accountId_from_address(address)
        if accountId:
            inventory = self.get_user_nft_balance(accountId)
            return inventory
        else:
            return None

    def get_nft_id_from_address(self, address):
        inventory = self.get_nft_inventory_for_address(address)
        if inventory:
            nft_ids = [nft['nftId'] for nft in inventory]
            return nft_ids
        else:
            return None

if __name__ == "__main__":
    lr = LoopringAPI()
    print(lr.get_nft_id_from_address('0x9D44E07C8d1EF2B8d12dBc9A32d908fA3D75D626'))