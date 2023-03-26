import pandas as pd
from sqlalchemy.orm import relationship, sessionmaker
from SQL.sqlAlchemy import User, Transaction, NFT, Collection, CryptoPrice, FloorPrice, Base
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import IntegrityError
from sqlalchemy import desc, or_, and_, func, select, update, distinct
from sqlalchemy.orm import aliased, joinedload
from datetime import datetime
# Base = declarative_base()

class NiftyDB:
    def __init__(self, db_path='sqlite:///nifty22.db'):
        self.engine = create_engine(db_path, echo=True)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def __del__(self):
        self.engine.dispose()

    def close(self):
        self.engine.dispose()

    def get_user_info(self, accountId=None, address=None, username=None):
        """
        Get user info from the database
        """
        session = self.Session()

        if address is not None:
            result = session.query(User).filter_by(address=address).first()
        elif accountId is not None:
            result = session.query(User).filter_by(accountId=accountId).first()
        elif username is not None:
            result = session.query(User).filter_by(username=username).first()

        session.close()

        if result is None:
            return None, None, None
        else:
            return result.accountId, result.address, result.username

    def insert_user_info(self, accountId, address, username, discord_username):
        """
        Insert user info into the database
        """
        session = self.Session()
        user = User(accountId=accountId, address=address, username=username, discord_username=discord_username)
        session.add(user)
        session.commit()
        session.close()

    def insert_nft(self, nftId, nftData, tokenId, contractAddress, creatorEthAddress, name, amount, attributes,
                   collectionId, createdAt, firstMintedAt, updatedAt, thumbnailUrl, mintPrice):
        """
        Insert NFT info into the database
        """
        session = self.Session()
        nft = NFT(nftId=nftId, nftData=nftData, tokenId=tokenId, contractAddress=contractAddress, creatorEthAddress=creatorEthAddress, name=name, amount=amount, attributes=attributes, collectionId=collectionId, createdAt=createdAt, firstMintedAt=firstMintedAt, updatedAt=updatedAt, thumbnailUrl=thumbnailUrl, mintPrice=mintPrice)
        session.add(nft)
        session.commit()
        session.close()

    def insert_transaction(self, blockId, createdAt, txType, nftData, sellerAccount, buyerAccount, amount, price, priceUsd):
        """
        Insert transaction info into the database
        Data is recived from the block
        """
        session = self.Session()
        transaction = Transaction(blockId=blockId, createdAt=createdAt, txType=txType, nftData=nftData, sellerAccount=sellerAccount, buyerAccount=buyerAccount, amount=amount, price=price, priceUsd=priceUsd)
        session.add(transaction)
        session.commit()
        session.close()

    def insert_floor_price(self, nftId, floor_price, last_updated):
        # create a new session
        session = self.Session()

        # create a new FloorPrice object
        floor_price = FloorPrice(nft_id=nftId, floor_price=floor_price, last_updated=last_updated)

        try:
            # add the floor price object to the session
            session.add(floor_price)

            # commit the changes
            session.commit()
        except IntegrityError:
            # handle any integrity errors (e.g. duplicate entries)
            session.rollback()
        finally:
            # close the session
            session.close()

    # def insert_floor_price(self, nftId, floor_price, last_updated):
    #     query = (f"INSERT INTO floor_prices VALUES ('{nftId}', '{floor_price}', '{last_updated}')")
    #     self.c.execute(query)
    #     self.conn.commit()

    def insert_collection(self, collectionId, name, slug, creator, description, bannerUri, avatarUri, tileUri,
                          createdAt, numNfts, layer):
        session = self.Session()
        collection = Collection(
            collection_id=collectionId, name=name, slug=slug, creator=creator, description=description,
            banner_uri=bannerUri, avatar_uri=avatarUri, tile_uri=tileUri, created_at=createdAt, num_nfts=numNfts,
            layer=layer
        )
        session.add(collection)
        session.commit()
        session.close()

    def get_old_floor_price(self, nftId):
        session = self.Session()
        floor_price = session.query(FloorPrice).filter_by(nft_id=nftId).first()
        session.close()
        if floor_price is None:
            return None
        else:
            return floor_price.floor_price




    def get_collection_slug(self, collectionId):
        session = self.Session()
        collection = Collection(collection_id=collectionId)
        slug = collection.slug
        session.close()
        return slug

    def check_if_block_exists(self, blockId):
        session = self.Session()
        exists = session.query(Transaction).filter_by(blockId=blockId).first() is not None
        session.close()
        return exists

    # def get_holder_stats(self, nftId):
    #     self.c.execute(f"SELECT * FROM nft_stats WHERE nftId='{nftId}' ORDER BY timestamp")
    #     result = self.c.fetchall()
    #     if result is None:
    #         return None
    #     else:
    #         return result

    def get_latest_saved_block(self):
        session = self.Session()
        result = session.query(Transaction).order_by(Transaction.blockId.desc()).first()
        session.close()
        if result is None:
            return None
        else:
            return result.blockId

    def get_last_historical_price_data(self, currency):
        session = self.Session()
        result = session.query(CryptoPrice).filter(CryptoPrice.currency == currency).order_by(
            CryptoPrice.timestamp.desc()).first()
        session.close()
        if result is None:
            return None
        else:
            return result.timestamp

    def get_historical_price(self, currency, timestamp, print_str=True):
        start = timestamp - 1000
        end = timestamp + 500
        session = self.Session()
        result = session.query(CryptoPrice).filter(CryptoPrice.currency == currency,
                                                         CryptoPrice.timestamp.between(start, end)) \
                                                 .order_by(desc(CryptoPrice.timestamp)).first()
        if print_str:
            print(f"Retrieving price for {currency} at {timestamp}")
        session.close()
        if result is None:
            print(f"No historical price data found for {currency} at {timestamp}")
            return None
        else:
            return result.price

    def get_all_nfts(self):
        session = self.Session()
        result = session.query(NFT).all()
        session.close()
        if result is None:
            return None
        else:
            return result

    def get_last_buyer_for_nft(self, nftData):
        session = self.Session()
        tx = aliased(Transaction)
        buyer = aliased(User)
        result = session.query(tx.buyer_account.label('accountId'), buyer.address, buyer.username). \
            join(buyer, tx.buyer_account == buyer.accountId). \
            filter(tx.nft_data == nftData). \
            order_by(tx.created_at.desc()). \
            first()
        session.close()
        if result is None:
            return None
        else:
            return result

    def insert_historical_price_data(self, currency, dataFrame):
        session = self.Session()
        for index, row in dataFrame.iterrows():

            timestamp = row['time'].timestamp()
            datetime = row['time'].strftime('%Y-%m-%d %H:%M:%S')
            price = row['open']
            historical_price = CryptoPrice(timestamp=timestamp, datetime=datetime, currency=currency,
                                                     price=price)
            session.add(historical_price)
            try:
                session.commit()
                print(f"Inserted {timestamp} {datetime} | {currency}-USD: ${price}")
            except IntegrityError:
                print(f"Duplicate entry for {timestamp} {datetime} | {currency}-USD: ${price}")
                session.rollback()
        session.close()

    def get_nft_transactions(self, nftData):
        session = self.Session()
        result = session.query(Transaction).filter_by(nftData=nftData).order_by(Transaction.blockId).all()
        session.close()
        return result

    def get_all_nft_trades(self):
        session = self.Session()
        result = session.query(Transaction).filter_by(txType='SpotTrade').order_by(Transaction.blockId).all()
        session.close()
        return result


    def get_nft_data(self, nft_id):
        session = self.Session()
        if len(nft_id) == 66:
            result =session.query(NFT).filter_by(nftData=nft_id).one_or_none()
        elif len(nft_id) == 109:
            result =session.query(NFT).filter_by(tokenId=nft_id[:66]).one_or_none()
        elif len(nft_id) == 36:
            result =session.query(NFT).filter_by(nftId=nft_id).one_or_none()
        else:
            result = None
        session.close()
        return result

    def get_user_trade_history(self, accountId, nftData_List=None):
        session = self.Session()
        buyer_alias = aliased(User)
        seller_alias = aliased(User)

        query = session.query(Transaction, NFT.nftData, NFT.nftId, NFT.name,
                                   buyer_alias.username.label('buyer'), seller_alias.username.label('seller')) \
            .join(NFT, Transaction.nftData == NFT.nftData) \
            .join(buyer_alias, Transaction.buyerAccount == buyer_alias.accountId) \
            .join(seller_alias, Transaction.sellerAccount == seller_alias.accountId) \
            .filter(or_(Transaction.buyerAccount == accountId, Transaction.sellerAccount == accountId))

        if nftData_List is not None:
            query = query.filter(Transaction.nftData.in_(nftData_List))

        query = query.order_by(Transaction.blockId)

        result = query.all()
        session.close()

        if result is None:
            return None
        else:
            return result

    def get_nft_trade_history(self, nft_id):
        nftData = self.get_nft_data(nft_id)['nftData']
        session = self.Session()


        result = session.query(Transaction, User.username.label('seller'), User.username.label('buyer')). \
            join(User, Transaction.sellerAccount == User.accountId). \
            join(User, Transaction.buyerAccount == User.accountId). \
            filter(Transaction.nftData == nftData). \
            options(joinedload(Transaction.sellerAccount), joinedload(Transaction.buyerAccount)). \
            all()
        session.close()
        if not result:
            print(f"No transactions found for {nft_id}")
            return None
        else:
            return result

    def get_number_of_tx(self, nftData_List):
        formatted_nftData_List = [str(nftData) for nftData in nftData_List]
        result = self.session.query(func.count(Transaction)).filter(
            Transaction.nftData.in_(formatted_nftData_List)).all()
        return result[0][0] if result else 0

    def get_nft_collection_tx(self, collectionId):
        tx = aliased(Transaction)
        nfts = aliased(NFT)

        # Define the query
        query = self.session.query(tx, nfts.nftId, nfts.name). \
            join(nfts, nfts.nftData == tx.nftData). \
            filter(nfts.collectionId == collectionId). \
            order_by(tx.blockId)

        result = query.all()

        if result is None:
            return None
        else:
            return result

    def get_nft_price_at_time(self, nftId, timestamp):
        session = self.Session()

        result = session.query(NFT).filter(
            NFT.nftId == nftId,
            NFT.createdAt <= datetime.fromtimestamp(timestamp)
        ).order_by(NFT.createdAt.desc()).first()

        session.close()

        if result is None:
            return None
        else:
            return result.price

    def get_collection_info(self, collectionId):
        session = self.Session()
        stmt = select(Collection).where(Collection.collectionId == collectionId)
        result = session.execute(stmt).fetchone()
        session.close()
        if result is None:
            return None
        else:
            return dict(result.items())

    # def get_orderbook_data(self, nftId, collection):
    #     # First, get the available snapshot times
    #     query = f"SELECT orders.nftId, orders.snapshotTime from {collection + '_orders'} AS orders GROUP BY snapshotTime ORDER BY snapshotTime"
    #     self.c.execute(query)
    #     snapshotTimes = self.c.fetchall()
    #
    #     # Then, get the orderbook data
    #     query = "SELECT users.username, orders.ownerAddress, orders.amount, orders.price, orders.orderId, orders.fulfilledAmount," \
    #             f" nfts.name, orders.nftId, orders.snapshotTime from {collection + '_orders'} AS orders " \
    #             "INNER JOIN users ON orders.ownerAddress = users.address " \
    #             "INNER JOIN nfts ON nfts.nftId = orders.nftId " \
    #             "ORDER BY snapshotTime"
    #     self.c.execute(query)
    #     result = self.c.fetchall()
    #     if result is None:
    #         return None
    #     else:
    #         return snapshotTimes, result

    def get_all_gamestop_nft_users(self, blockId=None):
        session = self.Session()
        buyer_query = session.query(distinct(Transaction.buyerAccount)).order_by(Transaction.buyerAccount)
        if blockId is not None:
            buyer_query = buyer_query.filter(Transaction.blockId > blockId)
        query = session.query(User).filter(User.accountId.in_(buyer_query))
        result = query.all()
        session.close()
        if result is None:
            return None
        else:
            return result

    def get_users_without_usernames(self):
        session = self.Session()
        user_length = func.length(User.username).label('user_length')
        users = session.query(User, user_length).filter(user_length == 42).all()
        session = self.Session()
        if users is None:
            return None
        else:
            return users

    # def get_last_collection_stats_timestamp(self, collectionId):
    #     query = f"SELECT MAX(timestamp) AS timestamp FROM collection_stats WHERE collectionId='{collectionId}'"
    #     self.c.execute(query)
    #     result = self.c.fetchone()
    #     if result is None:
    #         return None
    #     else:
    #         return result['timestamp']

    def update_username(self, accountId, username):
        session = self.Session()
        stmt = update(User).where(User.accountId == accountId).values(username=username)
        session.execute(stmt)
        session.commit()
        session.close()

    def get_tx_by_timestamp(self, timestamp_start, timestamp_end):
        session = self.Session()
        result = session.query(Transaction).filter(
            and_(Transaction.createdAt >= timestamp_start, Transaction.createdAt <= timestamp_end)).all()
        session.close()
        if not result:
            return None
        else:
            return result

    def get_collection(self, collectionId):
        session = self.Session()
        result = session.query(Collection).filter(Collection.collectionId == collectionId).first()
        session.close()
        return result

    def get_collection_ids(self):
        session = self.Session()
        result = session.query(Collection.collectionId).order_by(Collection.createdAt.desc()).all()
        session.close()
        return result

    def get_newest_collection(self):
        session = self.Session()
        result = session.query(Collection).order_by(Collection.createdAt.desc()).first()
        session.close()
        return result

    def get_nfts_in_collection(self, collectionId):
        session = self.Session()
        result = session.query(NFT).filter(NFT.collectionId == collectionId).all()
        session.close()
        print(result)
        return result

    def get_all_nftdatas(self):
        session = self.Session()
        result = session.query(NFT.nftData).all()
        session.close()
        return result

    def update_num_nfts_in_collection(self, collectionId, numNfts):
        session = self.Session()
        collection = Collection(collectionId=collectionId, numNfts=numNfts)
        collection = session.merge(collection)
        collection.numNfts = numNfts
        session.commit()
        session.close()

    # def get_loopingu_rarity(self, number):
    #     query = f"SELECT * FROM loopingu_rarity WHERE number='{number}'"
    #     self.c.execute(query)
    #     result = self.c.fetchone()
    #     if result is None:
    #         return None
    #     else:
    #         return result

    def get_nft_by_name(self, name):
        session = self.Session()
        result = session.query(NFT).filter_by(name=name).first()
        session.close()
        return result

    def get_nft_owner(self, nftData):
        session = self.Session()
        result = session.query(Transaction.buyerAccount).filter_by(nftData=nftData).order_by(
            Transaction.createdAt.desc()).first()
        session.close()
        if result is None:
            return None
        else:
            return result[0]

if __name__ == "__main__":
    pass
