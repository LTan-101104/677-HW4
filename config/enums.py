from enum import Enum


# Role of each peer
class Role(str, Enum):
    BUYER = "buyer"
    SELLER = "seller"
    BOTH = "both"


# Message Type
class MessageType(str, Enum):
    BUY = "BUY"
    SELL_DEPOSIT = "SELL_DEPOSIT"
    BUY_RESP = "BUY_RESP"
    SOLD_NOTIFY = "SOLD_NOTIFY"
    ACK = "ACK"
    ELECTION = "ELECTION"
    OK = "OK"
    COORDINATOR = "COORDINATOR"
    RESIGN = "RESIGN"


class BuyStatus(str, Enum):
    SUCCESS = "SUCCESS"
    OUT_OF_STOCK = "OUT_OF_STOCK"


class Item(str, Enum):
    SALT = "salt"
    FISH = "fish"
    BOAR = "boar"
