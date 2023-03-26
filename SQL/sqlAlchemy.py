from sqlalchemy import create_engine, Column, Integer, Text, Float, ForeignKey, DateTime, PrimaryKeyConstraint
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session


Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    accountId = Column(Integer, primary_key=True, unique=True, index=True)
    address = Column(Text)
    username = Column(Text)

class Transaction(Base):
    __tablename__ = 'transactions'
    transactionId = Column(Integer, primary_key=True, autoincrement=True)
    blockId = Column(Integer)
    createdAt = Column(Integer)
    txType = Column(Text)
    nftData = Column(Text)
    sellerAccount = Column(Integer, ForeignKey('users.accountId'))
    buyerAccount = Column(Integer, ForeignKey('users.accountId'))
    amount = Column(Integer)
    price = Column(Float)
    priceUsd = Column(Float)

class Collection(Base):
    __tablename__ = 'collections'
    collectionId = Column(Text, primary_key=True)
    name = Column(Text)
    slug = Column(Text)
    creator = Column(Text)
    description = Column(Text)
    bannerUri = Column(Text)
    avatarUri = Column(Text)
    tileUri = Column(Text)
    createdAt = Column(Integer)
    numNfts = Column(Integer)
    layer = Column(Text)


class NFT(Base):
    __tablename__ = 'nfts'
    nftId = Column(Text, primary_key=True, unique=True, index=True)
    nftData = Column(Text)
    tokenId = Column(Text)
    contractAddress = Column(Text)
    creatorEthAddress = Column(Text)
    name = Column(Text)
    amount = Column(Integer)
    attributes = Column(Text)
    collectionId = Column(Text, ForeignKey('collections.collectionId'))
    createdAt = Column(Integer)
    firstMintedAt = Column(Integer)
    updatedAt = Column(Integer)
    thumbnailUrl = Column(Text)
    mintPrice = Column(Float)

class CryptoPrice(Base):
    __tablename__ = 'historical_crypto_prices'
    timestamp = Column(Integer, primary_key=True)
    datetime = Column(Text)
    currency = Column(Text, primary_key=True)
    price = Column(Float)

class FloorPrice(Base):
    __tablename__ = 'floor_prices'

    nft_id = Column(Text, ForeignKey('nfts.nftId'), primary_key=True)
    last_updated = Column(DateTime, primary_key=True)
    floor_price = Column(Float)

    nft = relationship('NFT')

    __table_args__ = (
        PrimaryKeyConstraint('nft_id', 'last_updated'),
        {},
    )