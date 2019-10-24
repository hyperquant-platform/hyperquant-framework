import hashlib
import itertools
import json
import logging
import math
import sys
import threading
import time
from base64 import b64decode
from collections import defaultdict
from collections.abc import Iterable
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
from inspect import signature
from threading import Thread, RLock
from typing import Any, List
from urllib.parse import urlencode, urljoin
from zlib import MAX_WBITS, decompress

import requests
from dateutil import parser
from requests import Session
from signalr import Connection
from signalr.events import EventHook
from websocket import WebSocketApp

from hyperquant.api import (CandleInterval, Currency, CurrencyPair, Direction,
                            Endpoint, ErrorCode, OrderBookDirection,
                            OrderStatus, OrderType, ParamName, Platform,
                            Sorting, TransactionType, apply_data_on_obj,
                            convert_items_obj_to_dict,
                            convert_items_obj_to_list, convert_items_to_obj,
                            item_format_by_endpoint)
from hyperquant.clients.singleton_helper import SingleDataAggregator
from hyperquant.utils import log_util, time_util
from hyperquant.utils.log_util import items_to_interval_string, make_short_str
from hyperquant.utils.math_util import drop_trailing_zeros as dtz

"""
API clients for various trading platforms: REST and WebSocket.

Note: All cases which are special for particular platforms should be commented
with platform name and some small description.

Cancel_all_orders(), close_all_positions() and others added to provide common 
interface for usual actions regardless any implementation particularities 
of a platform. Some of them have separate endpoint and are easy to implement, 
some use other endpoints and multiple calls to API to implement this behavior, 
some don't have any possibility to do this. Anyway client's user must not know 
about all that stuff. He can just call a client's method. 

Symbol format: AAABBB or AAA_BBB. If a platform provides some delimiter keep it 
transforming to standard delimeter "_". For example, 
btcusd" -> "BTCUSD", "btc-usd" -> "BTC_USD"

"""

# TODO Standardize more common behavior for all clients:
# todo check all empty, None and wrong params - make same behavior for all
# todo? convert amount, price, and similar to Decimal type (float to decimal, int to int)

# Value objects


class ValueObject:
    def to_json(self, is_list=False, is_stringify=False):
        endpoint = ProtocolConverter.endpoint_by_item_class.get(self.__class__)
        item_format = item_format_by_endpoint.get(endpoint)
        data = self._convert_to_json(self, item_format, is_list)
        if is_stringify:
            data = json.dumps(data)
        return data

    def _convert_to_json(self, obj, item_format, is_list=False):
        return convert_items_obj_to_list(obj, item_format) if is_list else \
            convert_items_obj_to_dict(obj, item_format)

    def from_json(self, data):
        if isinstance(data, str):
            data = json.loads(data)
        endpoint = ProtocolConverter.endpoint_by_item_class.get(self.__class__)
        item_format = item_format_by_endpoint.get(endpoint)
        data = apply_data_on_obj(self, data, item_format)
        return data


# WS
class Info(ValueObject):
    code = None
    message = None


# WS
class Channel(ValueObject):
    channel_id = None
    channel = None
    symbol = None


class Error(ValueObject):
    code = None
    message = None

    def __init__(self, code=None, message=None) -> None:
        super().__init__()
        self.code = code
        self.message = message

    def __str__(self) -> str:
        return "<Trading-Error code: %s msg: %s>" % (self.code, self.message)


class DataObject(ValueObject):
    is_milliseconds = True
    # subscription acts as unique id for combination (endpoint, symbol, params) in connectors,
    # needed to choose right callback
    subscription = None

    # Use only main endpoint name if there are possibly multiple ones (e.g. TICKER instead of TICKER_ALL)
    endpoint = None
    # # Plural, because WS may have multiple endpoints for same item type
    # endpoints = None

    def __eq__(self, o: object) -> bool:
        return o and isinstance(o, self.__class__)

class ItemObject(DataObject):
    # (Note: Order is from abstract to concrete)
    platform_id = None
    symbol = None
    timestamp = None  # Unix timestamp in milliseconds
    item_id = None  # There is no item_id for candle, ticker, bookticker, only for trade, mytrade and order

    # <<<<<<< HEAD
    #     @classmethod
    #     def _get_class_fields(cls):
    #         if hasattr(cls, "_determining_fields") and cls._determining_fields and len(cls._determining_fields):
    #             return cls._determining_fields
    #         return [f for f in cls.__dict__.keys() if not callable(f) and not f.startswith("_")]

    #     def __hash__(self):
    #         return hash(tuple(self._get_class_fields()))

    #     def __eq__(self, other):
    #         return all(getattr(self, f) == getattr(other, f) for f in self._get_class_fields())

    # =======
    # >>>>>>> parent of c50d6cd... modefy comparison function for ItemObject and its descendants
    @property
    def platform_name(self):
        return Platform.get_platform_name_by_id(self.platform_id)

    @property
    def timestamp_s(self):
        timestamp_s = self.timestamp / 1000 if self.timestamp and self.is_milliseconds else self.timestamp
        return timestamp_s

    @property
    def timestamp_ms(self):
        timestamp_ms = self.timestamp * 1000 if self.timestamp and not self.is_milliseconds else self.timestamp
        return timestamp_ms

    @property
    def timestamp_iso(self):
        timestamp_s = self.timestamp / 1000 if self.timestamp and self.is_milliseconds else self.timestamp
        # timestamp_iso = datetime.utcfromtimestamp(timestamp_s).isoformat() if timestamp_s else timestamp_s
        dt = datetime.utcfromtimestamp(timestamp_s).astimezone(
            tz=timezone.utc) if timestamp_s else None
        timestamp_iso = dt.isoformat().replace('+00:00',
                                               'Z') if dt else timestamp_s
        return timestamp_iso

    def __init__(self,
                 platform_id=None,
                 symbol=None,
                 timestamp=None,
                 item_id=None,
                 is_milliseconds=False) -> None:
        super().__init__()
        self.platform_id = platform_id
        self.symbol = symbol
        self.timestamp = int(
            timestamp) if timestamp and is_milliseconds else timestamp
        self.item_id = item_id

        self.is_milliseconds = is_milliseconds

    def __eq__(self, o: object) -> bool:
        # Identifying params:
        # (Timestamp may change (for trades on Bitfinex), but it is still the same item)
        return o and isinstance(o, self.__class__) and \
               self.platform_id == o.platform_id and \
               self.item_id == o.item_id and \
               self.symbol == o.symbol and \
               (self.item_id is not None or self.timestamp == o.timestamp)

    def __hash__(self) -> int:
        return hash((self.platform_id, self.item_id, self.timestamp))

    def __repr__(self) -> str:
        return "<Item-%s symbol:%s time:%s %s item_id:%s>" % (
            self.platform_name, self.symbol, self.timestamp,
            self.timestamp_iso, self.item_id)

    def __getattribute__(self, name: str) -> Any:
        if name.startswith(ParamName.PRICE) and name.endswith("_real"):
            # "price_real", "price_open_real", ... "price_xxx_real"
            # Used in PlatformRESTConnector.buy/sell() to calculate appropriate amount in contracts for BitMEX
            # (to convert XBT to contracts)
            normal_name = name.replace("_real", "")
            value = super().__getattribute__(normal_name)

            if self.platform_id == Platform.BITMEX:
                if self.symbol == "ETHUSD":
                    # https://www.bitmex.com/app/contract/ETHUSD
                    #  "0.001 mXBT за 1 USD (в настоящее время 0.00026765 XBT за контракт)"
                    value = value * Decimal(
                        "0.000001") if value is not None else None
                elif self.symbol == "XBTUSD":
                    # https://www.bitmex.com/app/contract/XBTUSD
                    value = round(Decimal("1") / value, 8) if value else None

            return value

        return super().__getattribute__(name)

    def __setattr__(self, key, value):
        if key.startswith(ParamName.PRICE) and key.endswith("_real"):
            # "price_real", "price_open_real", ... "price_xxx_real"
            normal_name = key.replace("_real", "")
            key = normal_name

            if self.platform_id == Platform.BITMEX:
                if self.symbol == "ETHUSD":
                    # https://www.bitmex.com/app/contract/ETHUSD
                    value = value * Decimal(
                        "1000000") if value is not None else None
                elif self.symbol == "XBTUSD":
                    # https://www.bitmex.com/app/contract/XBTUSD
                    value = round(Decimal("1") / value, 8) if value else None

        return super().__setattr__(key, value)

    # ?@property
    # def price_real(self):
    #     return self.__getattribute__("price_real")

    # @property
    # def price_real(self):
    #     price = getattr(self, "price")  # Raises exception if object hasn't price field
    #     if self.platform_id == Platform.BITMEX:
    #         if self.symbol == "ETHUSD":
    #             # https://www.bitmex.com/app/contract/ETHUSD
    #             #  "0.001 mXBT за 1 USD (в настоящее время 0.00026765 XBT за контракт)"
    #             return price * Decimal("0.000001") if price is not None else None
    #         elif self.symbol == "XBTUSD":
    #             # https://www.bitmex.com/app/contract/XBTUSD
    #             return Decimal("1") / price if price else None
    #     return price


class Trade(ItemObject):
    # Trade data:
    # In base (asset) currency (BTC in BTCUSD)
    amount = None
    # In quote currency (USD in BTCUSD)
    price = None

    # Not for all platforms or versions:
    direction = None  # Can be None if not defined

    @property
    def direction_name(self):
        return Direction.name_by_value.get(self.direction)

    @property
    def is_buy(self):
        return self.direction == Direction.BUY

    @property
    def is_sell(self):
        return self.direction == Direction.SELL

    # @property
    # def volume(self):
    #     return self.amount * self.price if self.amount is not None and self.price is not None else None

    def __init__(self,
                 platform_id=None,
                 symbol=None,
                 timestamp=None,
                 item_id=None,
                 amount=None,
                 price=None,
                 direction=None,
                 is_milliseconds=False) -> None:
        super().__init__(platform_id, symbol, timestamp, item_id,
                         is_milliseconds)
        self.amount = amount
        self.price = price
        self.direction = direction

    def __repr__(self) -> str:
        return "<Trade-%s symbol:%s time:%s %s item_id:%s %s am:%s pr:%s>" % (
            self.platform_name,
            self.symbol,
            self.timestamp,
            self.timestamp_iso,
            self.item_id,
            self.direction_name,
            # (Make big irrational decimal number shorter)
            log_util.make_short_str(self.amount, 8, False),
            log_util.make_short_str(self.price, 8, False))

    def __hash__(self) -> int:
        return hash((self.platform_id, self.item_id, self.amount, self.price,
                     self.direction))

    def __eq__(self, o: object) -> bool:
        # Supposed, that we always receive from all platforms not None item_id,
        # but for some tests we check other properties to do not set item_id.
        if not super().__eq__(o):
            return False

        if self.item_id is None and o and (self.amount != o.amount
                                           or self.price != o.price
                                           or self.direction != o.direction):
            return False

        return True


class MyTrade(Trade):
    order_id = None

    # Optional (not for all platforms):
    fee = None  # Комиссия биржи  # must be always positive; 0 if not supported
    rebate = None  # Возврат денег, скидка после покупки  # must be always positive; 0 if not supported

    # fee_symbol = None  # Currency symbol, by default, it's the same as for price
    # Note: volume = amount * price, total = volume - fee + rebate

    def __init__(self,
                 platform_id=None,
                 symbol=None,
                 timestamp=None,
                 item_id=None,
                 amount=None,
                 price=None,
                 direction=None,
                 order_id=None,
                 fee=None,
                 rebate=None,
                 is_milliseconds=False) -> None:
        super().__init__(platform_id, symbol, timestamp, item_id, amount,
                         price, direction, is_milliseconds)
        self.order_id = order_id
        self.fee = fee
        self.rebate = rebate

    def __repr__(self) -> str:
        return "<MyTrade-%s symbol:%s time:%s %s item_id:%s %s am:%s pr:%s order:%s%s%s>" % (
            self.platform_name, self.symbol, self.timestamp,
            self.timestamp_iso, self.item_id, self.direction_name,
            log_util.make_short_str(self.amount, 8, False),
            log_util.make_short_str(self.price, 8, False), self.order_id,
            " fee: %s" % self.fee if self.fee else "",
            " rebate: %s" % self.rebate if self.rebate else "")


class Candle(ItemObject):
    # (price_xxx values: None if not defined or candle has no trades for the interval)
    # (volume, trades_count: None if not defined, 0 if candle has no trades for the interval)

    # platform_id = None
    # symbol = None
    interval = None  # CandleInterval
    # timestamp = None  # timestamp_open  # included
    timestamp_close = None  # excluded

    # Prices in quote currency (USD in BTCUSD)
    price_open = None
    price_close = None
    price_high = None
    price_low = None
    # Sum of amounts (of base (asset) currency, BTC in BTCUSD) of all trades within the interval
    # Note: this is different kind of volume than for trade, where volume = amount * price
    volume = None

    # Optional
    trades_count = None

    def __init__(self,
                 platform_id=None,
                 symbol=None,
                 interval=None,
                 timestamp=None,
                 timestamp_close=None,
                 price_open=None,
                 price_close=None,
                 price_high=None,
                 price_low=None,
                 volume=None,
                 trades_count=None,
                 is_milliseconds=False) -> None:
        super().__init__(platform_id, symbol, timestamp, None, is_milliseconds)
        self.interval = interval
        self.timestamp_close = int(
            timestamp_close
        ) if timestamp_close and is_milliseconds else timestamp_close
        self.price_open = price_open
        self.price_close = price_close
        self.price_high = price_high
        self.price_low = price_low
        self.volume = volume
        self.trades_count = trades_count

    def __repr__(self) -> str:
        return "<Candle-%s symbol:%s interval:%s time:%s %s pr op/cl:%s/%s hi/lo:%s/%s vol:%s count:%s>" % (
            self.platform_name, self.symbol, self.interval, self.timestamp,
            self.timestamp_iso, self.price_open, self.price_close,
            self.price_high, self.price_low, self.volume, self.trades_count)

    def __hash__(self) -> int:
        return hash((self.platform_id, self.timestamp, self.price_open,
                     self.price_close, self.volume))

    def __eq__(self, o: object) -> bool:
        if not super().__eq__(o):
            return False

        if (self.interval != o.interval
                or  # self.timestamp_close != o.timestamp_close or
                self.price_open != o.price_open or
                self.price_close != o.price_close or
                self.price_high != o.price_high or
                self.price_low != o.price_low or self.volume != o.volume or
                self.trades_count != o.trades_count):
            return False
        return True

    # Note: timestamp_close is defined only for our candles - from MDS. From platforms all candles
    # return empty timestamp_close, so is_finished will be always False.
    @property
    def is_finished(self):
        return self.timestamp_close is not None

    @staticmethod
    def sorted(candles):
        if not candles:
            return candles
        result = sorted(candles,
                        key=lambda c:
                        (c.timestamp_close or c.timestamp + CandleInterval.
                         convert_to_minutes(c.interval) * 60, -c.timestamp))
        # --CandleInterval.convert_to_minutes(c.interval)))
        return result


class Ticker(ItemObject):
    # platform_id = None
    # symbol = None
    # timestamp = None

    # Last price (of last trade) in quote currency (USD in BTCUSD)
    price = None

    def __init__(self,
                 platform_id=None,
                 symbol=None,
                 timestamp=None,
                 price=None,
                 is_milliseconds=False) -> None:
        super().__init__(platform_id, symbol, timestamp, None, is_milliseconds)
        self.price = price

    def __repr__(self) -> str:
        return "<Ticker-%s symbol:%s time:%s %s pr:%s>" % (
            self.platform_name, self.symbol, self.timestamp,
            self.timestamp_iso, log_util.make_short_str(self.price, 8, False))

    def __hash__(self) -> int:
        return hash((self.platform_id, self.timestamp, self.price))

    def __eq__(self, o: object) -> bool:
        if not super().__eq__(o):
            return False

        return self.price == o.price


# class BookTicker(DataObject):
#     symbol = None
#     amount_bid = None
#     price_bid = None
#     amount_ask = None
#     price_ask = None


class OrderBook(ItemObject):
    _name = "OrderBook"

    _asks = None
    _bids = None

    @property
    def asks(self):
        return self._asks

    @asks.setter
    def asks(self, value):
        self._asks = value
        if self._asks and isinstance(self._asks[0],
                                     OrderBookItem):  # (Check type for BitMEX)
            for item in self._asks:
                item.platform_id = self.platform_id
                item.symbol = self.symbol

    @property
    def bids(self):
        return self._bids

    @bids.setter
    def bids(self, value):
        self._bids = value
        if self._bids and isinstance(self._bids[0],
                                     OrderBookItem):  # (Check type for BitMEX)
            for item in self._bids:
                item.platform_id = self.platform_id
                item.symbol = self.symbol

    def __init__(self,
                 platform_id=None,
                 symbol=None,
                 timestamp=None,
                 item_id=None,
                 is_milliseconds=False,
                 asks=None,
                 bids=None) -> None:
        super().__init__(platform_id, symbol, timestamp, item_id,
                         is_milliseconds)
        self.asks = asks
        self.bids = bids

    def __repr__(self) -> str:
        return "<%s-%s symbol:%s time:%s %s item_id:%s asks:%s bids:%s>" % (
            self._name, self.platform_name,
            self.symbol, self.timestamp, self.timestamp_iso, self.item_id,
            len(self.asks) if self.asks is not None else self.asks,
            len(self.bids) if self.bids is not None else self.bids)

    def __eq__(self, o: object) -> bool:
        if not super().__eq__(o):
            return False

        if o and (self.asks != o.asks or self.bids != o.bids):
            return False

        return True

    def __hash__(self):
        return hash((self.platform_id, self.symbol, self.item_id,
                     self.timestamp, self.asks, self.bids))

    def _convert_to_json(self, obj, item_format, is_list=False):
        result = super()._convert_to_json(obj, item_format, is_list)
        for k, v in enumerate(result) if isinstance(result,
                                                    list) else result.items():
            if v and isinstance(v, list) and isinstance(v[0], OrderBookItem):
                result[k] = convert_items_obj_to_list(v, OrderBookItem.ITEM_FORMAT) if is_list else \
                    convert_items_obj_to_dict(v, OrderBookItem.ITEM_FORMAT)
        return result

    def from_json(self, data):
        super().from_json(data)

        self.asks = convert_items_to_obj(self.asks, OrderBookItem.ITEM_FORMAT,
                                         OrderBookItem)
        self.bids = convert_items_to_obj(self.bids, OrderBookItem.ITEM_FORMAT,
                                         OrderBookItem)
        self._on_bids_asks_updated()


class Quote(ItemObject):
    bestbid = None
    bestask = None

    def __init__(self,
                 platform_id=None,
                 symbol=None,
                 timestamp=None,
                 bestbid=None,
                 bestask=None,
                 is_milliseconds=False) -> None:
        super().__init__(platform_id, symbol, timestamp, None, is_milliseconds)
        self.bestask = bestask
        self.bestbid = bestbid

    def __repr__(self) -> str:
        return "<Quote-%s symbol:%s time:%s %s bid %s, ask %s>" % (
            self.platform_name, self.symbol, self.timestamp,
            self.timestamp_iso, log_util.make_short_str(
                self.bestbid, 8,
                False), log_util.make_short_str(self.bestask, 8, False))

    def __hash__(self) -> int:
        return hash(
            (self.platform_id, self.timestamp, self.bestask, self.bestbid))

    def __eq__(self, o: object) -> bool:
        if not super().__eq__(o):
            return False

        return self.bestask == o.bestask and self.bestbid == o.bestbid


class OrderBookDiff(OrderBook):
    _name = "OrderBookDiff"


# todo OrderBookAgg - to fit common pattern of class names (OrderBookDiff) and endpoint names (ORDER_BOOK_DIFF, ORDER_BOOK_AGG)
class AggOrderBook(OrderBook):
    _name = "AggOrderBook"

    @property
    def platform_ids(self):
        return self.platform_id

    @platform_ids.setter
    def platform_ids(self, ids):
        self.platform_id = Platform.convert_to_platform_ids(ids)

    @property
    def platform_name(self):
        return [
            Platform.get_platform_name_by_id(id) for id in self.platform_ids
        ]


class OrderBookItem(ItemObject):
    ITEM_FORMAT = [ParamName.AMOUNT, ParamName.PRICE, ParamName.DIRECTION]

    # platform_id = None
    # order_book_item_id = None  # item_id = None
    # symbol = None

    amount = None
    price = None
    direction = None

    # Optional
    orders_count = None

    @property
    def direction_name(self):
        return OrderBookDirection.name_by_value.get(self.direction)

    def __init__(self,
                 platform_id=None,
                 symbol=None,
                 timestamp=None,
                 item_id=None,
                 is_milliseconds=False,
                 amount=None,
                 price=None,
                 direction=None,
                 orders_count=None) -> None:
        super().__init__(platform_id, symbol, timestamp, item_id,
                         is_milliseconds)

        self.amount = amount
        self.price = price
        self.direction = direction
        self.orders_count = orders_count

    def __repr__(self) -> str:
        return "<OBI-%s %s am:%s pr:%s>" % (
            self.platform_name,  # self.item_id, self.symbol,
            self.direction_name,
            log_util.make_short_str(self.amount, 8, False),
            log_util.make_short_str(self.price, 8, False))

    def __hash__(self):
        return hash((self.platform_id, self.symbol, self.item_id,
                     self.timestamp, self.amount, self.price, self.direction))

    def __eq__(self, o: object) -> bool:
        if not super().__eq__(o):
            return False

        if o and (self.amount != o.amount or self.price != o.price
                  or self.direction != o.direction
                  or self.orders_count != o.orders_count):
            return False

        return True


# todo inherit from ItemObject?
class Account(DataObject):
    platform_id = None
    timestamp = None

    # balances = None

    # Binance other params:
    # "makerCommission": 15,
    # "takerCommission": 15,
    # "buyerCommission": 0,
    # "sellerCommission": 0,
    # "canTrade": true,
    # "canWithdraw": true,
    # "canDeposit": true,

    platform_name = ItemObject.platform_name
    timestamp_s = ItemObject.timestamp_s
    timestamp_ms = ItemObject.timestamp_ms
    timestamp_iso = ItemObject.timestamp_iso

    # @property
    # def platform_name(self):
    #     return Platform.get_platform_name_by_id(self.platform_id)
    #
    # @property
    # def timestamp_iso(self):
    #     timestamp_s = self.timestamp / 1000 if self.is_milliseconds else self.timestamp
    #     timestamp_iso = datetime.utcfromtimestamp(timestamp_s).isoformat() if timestamp_s else timestamp_s
    #     return timestamp_iso

    # @property
    # def available_balances(self):
    #     return [balance for balance in self.balances if balance.amount_available and balance.amount_available > 0]
    #
    # @property
    # def balances_with_money(self):
    #     return [balance for balance in self.balances if balance.amount_available or balance.amount_reserved]

    def __init__(self, platform_id=None,
                 timestamp=None) -> None:  # , balances=None
        super().__init__()
        self.platform_id = platform_id
        self.timestamp = timestamp
        # self.balances = balances

    def __repr__(self) -> str:
        return "<Account-%s time:%s>" % (  # balances_with_money:%s
            self.platform_name, self.timestamp_iso
        )  # , self.balances_with_money

    def __eq__(self, o) -> bool:
        if not super().__eq__(o):
            return False

        if o and (self.platform_id != o.platform_id
                  or self.timestamp != o.timestamp):
            return False

        return True


# todo inherit from ItemObject?
class Balance(DataObject):
    # Asset, currency
    platform_id = None
    symbol = None
    amount_available = None
    amount_reserved = None
    # for okex
    amount_borrowed = None
    pnl = None
    margin_balance = None

    # Getters
    platform_name = ItemObject.platform_name

    def __init__(self,
                 platform_id=None,
                 symbol=None,
                 amount_available=None,
                 amount_reserved=None) -> None:
        super().__init__()
        self.platform_id = platform_id
        self.symbol = symbol
        self.amount_available = amount_available
        self.amount_reserved = amount_reserved

    def __repr__(self) -> str:
        return "<Balance-%s symbol:%s amount:%s (+%s=%s) pnl:%s>" % (
            self.platform_name, self.symbol, self.amount_available,
            self.amount_reserved, self.amount_total, self.pnl)

    @property
    def is_borrowed(self):
        return bool(self.amount_borrowed)

    @property
    def amount_total(self):
        # Same as "walletBalance" field on BitMEX
        # amount_reserved used for futures platforms (currently BitMEX). For spot it has no sense.
        return self.amount_available + self.amount_reserved \
            if self.amount_available is not None and self.amount_reserved is not None else self.amount_available

    @property
    def amount_total_resulted(self):
        # Same as "marginBalance" field on BitMEX
        return self.amount_total + self.pnl \
            if self.amount_total is not None and self.pnl is not None else self.amount_total

    def __eq__(self, o: object) -> bool:
        if not super().__eq__(o):
            return False

        if o and (self.platform_id != o.platform_id or self.symbol != o.symbol
                  or self.amount_available != o.amount_available
                  or self.amount_reserved != o.amount_reserved
                  or self.amount_borrowed != o.amount_borrowed):
            return False

        return True

    def __hash__(self):
        return hash((self.platform_id, self.symbol))


# todo tests
class BalanceTransaction(ItemObject):
    transaction_type = None
    # (> 0 - Deposit, < 0 - Withdrawal)
    amount = None
    fee = None

    # todo
    # transaction_status = None

    @property
    def is_created_by_user(self):
        return TransactionType.check_is_created_by_user(self.transaction_type)

    def __init__(self,
                 platform_id=None,
                 symbol=None,
                 timestamp=None,
                 item_id=None,
                 transaction_type=None,
                 amount=None,
                 fee=None,
                 is_milliseconds=False) -> None:
        super().__init__(platform_id, symbol, timestamp, item_id,
                         is_milliseconds)
        # self.platform_id = platform_id
        # self.symbol = symbol
        # self.timestamp = timestamp
        # self.item_id = item_id

        self.transaction_type = transaction_type
        self.amount = amount
        self.fee = fee

    def __repr__(self) -> str:
        return "<BalanceTransaction-%s symbol:%s time:%s %s amount:%s>" % (
            self.platform_name, self.symbol, self.timestamp,
            self.timestamp_iso, self.amount)

    def __hash__(self):
        return hash(
            (self.platform_id, self.symbol, self.item_id, self.timestamp,
             self.amount, self.transaction_type, self.fee))

    def __eq__(self, o: object) -> bool:
        if not super().__eq__(o):
            return False

        if o and (self.platform_id != o.platform_id or self.symbol != o.symbol
                  or self.amount != o.amount
                  or self.transaction_type != o.transaction_type):
            return False

        return True


class Order(ItemObject):
    # platform_id = None
    # item_id = None
    # symbol = None
    # timestamp = None  # (transact timestamp)
    user_order_id = None

    order_type = None  # limit and market
    amount_original = None
    amount_executed = None
    price = None
    price_stop = None  # currently informational
    direction = None

    order_status = None  # open and close

    def __hash__(self):
        return hash((self.platform_id, self.symbol, self.item_id,
                     self.timestamp, self.order_type, self.amount_original,
                     self.price, self.direction))

    @property
    def direction_name(self):
        return Direction.name_by_value.get(self.direction)

    @property
    def is_buy(self):
        return self.direction == Direction.BUY

    @property
    def is_sell(self):
        return self.direction == Direction.SELL

    @property
    def order_type_name(self):
        return OrderType.name_by_value.get(self.order_type)

    @property
    def is_limit(self):
        return self.order_type == OrderType.LIMIT

    @property
    def is_market(self):
        return self.order_type == OrderType.MARKET

    @property
    def order_status_name(self):
        return OrderStatus.name_by_value.get(self.order_status)

    @property
    def is_open(self):
        return self.order_status in OrderStatus.open

    @property
    def is_closed(self):
        return self.order_status in OrderStatus.closed

    @property
    def is_new(self):
        # -return self.order_status == OrderStatus.NEW  # there is also status OPEN
        return self.is_open and self.amount_executed and self.amount_executed == 0

    @property
    def is_partially_filled(self):
        # -return self.order_status == OrderStatus.  # there is also status OPEN
        return self.is_open and self.amount_executed and self.amount_original and \
               0 < self.amount_executed < self.amount_original

    @property
    def is_filled(self):
        # (Should be always is_closed=True if amount_executed reaches amount_original)
        return self.is_closed and self.amount_executed and self.amount_original and \
               self.amount_executed >= self.amount_original

    @property
    def amount_left(self):
        return self.amount_original - self.amount_executed \
            if self.amount_original and self.amount_executed else self.amount_original

    def __init__(self,
                 platform_id=None,
                 symbol=None,
                 timestamp=None,
                 item_id=None,
                 is_milliseconds=False,
                 user_order_id=None,
                 order_type=None,
                 amount_original=None,
                 amount_executed=None,
                 price=None,
                 price_stop=None,
                 direction=None,
                 order_status=None) -> None:
        super().__init__(platform_id, symbol, timestamp, item_id,
                         is_milliseconds)
        self.user_order_id = user_order_id
        self.order_type = order_type
        self.amount_original = amount_original
        self.amount_executed = amount_executed
        self.price = price
        self.price_stop = price_stop
        self.direction = direction
        self.order_status = order_status

    def __repr__(self) -> str:
        return "<Order-%s item_id:%s time:%s symbol:%s %s type:%s status:%s am:%s/%s pr:%s>" % (
            self.platform_name, self.item_id, self.timestamp_iso, self.symbol,
            self.direction_name, self.order_type_name, self.order_status_name,
            self.amount_executed, self.amount_original, self.price)

    def __eq__(self, o: object) -> bool:
        if not super().__eq__(o):
            return False

        if o and (self.user_order_id != o.user_order_id
                  or self.order_type != o.order_type
                  or self.amount_original != o.amount_original
                  or self.amount_executed != o.amount_executed
                  or self.price != o.price or self.direction != o.direction
                  or self.order_status != o.order_status):
            return False

        return True


# todo inherit from ItemObject?
class Position(DataObject):
    platform_id = None
    symbol = None
    timestamp = None

    amount = None
    direction = None
    average_price = None
    margincall_price = None
    profit_n_loss = None

    # for okex
    amount_borrowed = None

    _is_open = None

    @property
    def is_open(self):
        return self._is_open if self._is_open is not None else self.amount and self.amount > 0

    @is_open.setter
    def is_open(self, value):
        self._is_open = value

    # Getters
    platform_name = ItemObject.platform_name
    timestamp_s = ItemObject.timestamp_s
    timestamp_ms = ItemObject.timestamp_ms
    timestamp_iso = ItemObject.timestamp_iso

    @property
    def direction_name(self):
        return Direction.name_by_value.get(self.direction)

    @property
    def is_buy(self):
        return self.direction == Direction.BUY

    @property
    def is_sell(self):
        return self.direction == Direction.SELL

    @property
    def is_borrowed(self):
        return bool(self.amount_borrowed)

    # @property
    # def is_open(self):
    #     return bool(self.amount)

    def __init__(self,
                 platform_id=None,
                 symbol=None,
                 timestamp=None,
                 amount=None,
                 direction=None) -> None:
        super().__init__()
        self.platform_id = platform_id
        self.symbol = symbol
        self.timestamp = timestamp

        self.amount = amount
        self.direction = direction

    def __repr__(self) -> str:
        return "<Position-%s symbol:%s time:%s %s %s amount:%s>" % (
            self.platform_name, self.symbol, self.timestamp_iso,
            ("open" if self.is_open else "closed") if self.is_open is not None
            else self.is_open, self.direction_name, self.amount)

    def __eq__(self, o: object) -> bool:
        if not super().__eq__(o):
            return False

        if o and (self.platform_id != o.platform_id or self.symbol != o.symbol
                  or self.timestamp != o.timestamp or self.amount != o.amount
                  or self.direction != o.direction):
            return False

        return True


class Transfer(ItemObject):
    amount = None
    from_transfer = None
    to_transfer = None

    def __hash__(self):
        return hash(
            (self.platform_id, self.symbol, self.item_id, self.timestamp,
             self.amount, self.from_transfer, self.to_transfer))

    def __repr__(self) -> str:
        return "<Transfer-%s symbol:%s time:%s from %s to %s amount:%s>" % (
            self.platform_name, self.symbol, self.timestamp_iso,
            self.from_transfer, self.to_transfer, self.amount)

    def __eq__(self, o: object) -> bool:
        if not super().__eq__(o):
            return False

        if o and (self.amount != o.amount
                  or self.from_transfer != o.from_transfer
                  or self.to_transfer != o.to_transfer):
            return False

        return True


class ItemIdGeneratorMixin:
    @staticmethod
    def _create_trade_item_id_hash_str(item):
        return (str(item.timestamp) + str(item.symbol) + str(item.amount) +
                str(item.price) + str(item.direction))

    @staticmethod
    def _generate_trade_item_id(hash_str):
        item_id = hashlib.sha1()
        item_id.update(hash_str.encode('utf-8'))
        return item_id.hexdigest()

    def generate_item_ids(self, result):
        if isinstance(result, list):
            item_id_hash_strs = defaultdict(int)
            for item in result:
                if type(item) == Trade and item.item_id is None:
                    hash_str = self._create_trade_item_id_hash_str(item)
                    iter_hash_str = '{}-{}'.format(hash_str,
                                                   item_id_hash_strs[hash_str])
                    item_id_hash_strs[hash_str] += 1
                    item.item_id = self._generate_trade_item_id(iter_hash_str)


# Base


class ProtocolConverter:
    """
    Contains all the info and logic to convert data between
    our library API and remote platform API.
    """

    # Main params:
    # (Set by client or set it by yourself in subclass)
    platform_id = None
    version = None
    # (Define in subclass)
    base_url = None

    # Settings:
    is_use_max_limit = False
    is_delimiter_used = False

    # Converting info:
    # Our endpoint to platform_endpoint
    endpoint_lookup = None  # {"endpoint": "platform_endpoint", ...}
    # Our param_name to platform_param_name
    param_name_lookup = None  # {ParamName.FROM_TIME: "start", "not_supported": None, ...}
    # Our param_value to platform_param_value
    # (Also to convert "BTCUSD" to "XBTUSD" for BitMEX, and vice versa for Binance)
    param_value_lookup = None  # {Sorting.ASCENDING: 0}
    param_value_reversed_lookup = None
    max_limit_by_endpoint = None
    # todo
    # Disable symbol converting by default because it may cause unwanted results:
    # returning unexpected symbols, for example.
    # is_allow_symbol_converting = False

    # For parsing
    item_class_by_endpoint = {
        Endpoint.TRADE: Trade,
        Endpoint.TRADE_HISTORY: Trade,
        Endpoint.TRADE_MY: MyTrade,
        Endpoint.CANDLE: Candle,
        Endpoint.TICKER: Ticker,
        Endpoint.TICKER_ALL: Ticker,
        Endpoint.ORDER_BOOK: OrderBook,
        Endpoint.ORDER_BOOK_DIFF: OrderBookDiff,
        Endpoint.ORDER_BOOK_AGG: AggOrderBook,
        Endpoint.QUOTE: Quote,
        # Private
        Endpoint.ACCOUNT: Account,
        Endpoint.BALANCE: Balance,
        Endpoint.BALANCE_WALLET: Balance,
        Endpoint.BALANCE_TRANSACTION: BalanceTransaction,
        Endpoint.BALANCE_CONVERTED: Balance,
        Endpoint.ORDER: Order,
        Endpoint.ORDER_CREATE: Order,
        Endpoint.ORDER_CANCEL: Order,
        Endpoint.ORDERS_OPEN: Order,
        Endpoint.ORDERS_ALL: Order,
        Endpoint.ORDERS_ALL_CANCEL: Order,
        # Endpoint.ORDERS_HISTORY: Order,
        Endpoint.ORDER_TEST: Order,
        Endpoint.POSITION: Position,
        Endpoint.POSITION_CLOSE: Position,
        Endpoint.TRANSFER: Transfer,
        Endpoint.CURRENCY_PAIRS: CurrencyPair,
    }
    endpoint_by_item_class = {
        Trade: Endpoint.TRADE,
        MyTrade: Endpoint.TRADE_MY,
        Candle: Endpoint.CANDLE,
        Ticker: Endpoint.TICKER,
        Quote: Endpoint.QUOTE,
        OrderBook: Endpoint.ORDER_BOOK,
        AggOrderBook: Endpoint.ORDER_BOOK_AGG,
        # OrderBookDiff: Endpoint.ORDER_BOOK_DIFF,
        Account: Endpoint.ACCOUNT,
        Balance: Endpoint.BALANCE,
        BalanceTransaction: Endpoint.BALANCE_TRANSACTION,
        Order: Endpoint.ORDER,
        Position: Endpoint.POSITION,
    }
    # {Trade: {ParamName.ITEM_ID: "tid", ...}} - omitted properties won't be set
    param_lookup_by_class = None

    error_code_by_platform_error_code = None
    error_code_by_http_status = None

    # For converting time
    use_milliseconds = True  # Always use milliseconds!
    is_source_in_milliseconds = False
    is_source_in_timestring = False
    timestamp_platform_names = None  # ["startTime", "endTime"]
    # (If platform api is not consistent)
    timestamp_platform_names_by_endpoint = None  # {Endpoint.TRADE: ["start", "end"]}
    ITEM_TIMESTAMP_ATTR = ParamName.TIMESTAMP  # todo remove

    # For converting numbers
    currency_param_names = [
        ParamName.FROM_PRICE, ParamName.TO_PRICE, ParamName.AMOUNT_ORIGINAL,
        ParamName.AMOUNT_EXECUTED, ParamName.AMOUNT_AVAILABLE,
        ParamName.MARGIN_BALANCE, ParamName.AMOUNT_RESERVED, ParamName.AMOUNT,
        ParamName.AMOUNT_BORROWED, ParamName.PRICE_OPEN, ParamName.PRICE_CLOSE,
        ParamName.PRICE_HIGH, ParamName.PRICE_LOW, ParamName.PRICE,
        ParamName.PNL, ParamName.FEE, ParamName.REBATE
    ]
    # decimal_param_names = currency_param_names + [ParamName.AMOUNT]

    # For candles
    # Check for candles with "1w" interval. 0 - Monday, 6 - Sunday.
    # (See candle date or timestamp on web site or from API)
    weekday_start = 0
    intervals_supported = CandleInterval.ALL
    symbol_delimiter = None

    # todo defined platform_id in convert, so client must take its platform_id from its converter.
    # todo so remove platform_id from these parameters, because platform_id cannot be introduced here from outer code.
    def __init__(self, platform_id=None, version=None):
        if platform_id is not None:
            self.platform_id = platform_id
        if version is not None:
            self.version = version

        initial_param_value_reversed_lookup = self.param_value_reversed_lookup.copy() \
            if self.param_value_reversed_lookup else None
        if self.param_value_reversed_lookup is None:
            self.param_value_reversed_lookup = {}
        if self.param_value_lookup:
            for key, value in self.param_value_lookup.items():
                if isinstance(value, dict):
                    param_name = key
                    if not self.param_value_reversed_lookup.get(param_name):
                        self.param_value_reversed_lookup[param_name] = {}
                    self.param_value_reversed_lookup[param_name].update(
                        {v: k
                         for k, v in value.items()})
                else:
                    self.param_value_reversed_lookup[value] = key
            if initial_param_value_reversed_lookup:
                # todo apply on changed self.param_value_reversed_lookup to do not overwrite initial values
                pass

        # Create logger
        platform_name = Platform.get_platform_name_by_id(self.platform_id)
        self.logger = logging.getLogger(
            "%s.%s.v%s" % ("Converter", platform_name, self.version))

    # Convert to platform format

    def make_url_and_platform_params(self,
                                     endpoint=None,
                                     params=None,
                                     is_join_get_params=False,
                                     version=None):
        # Apply version on base_url
        version = version or self.version
        url = self.base_url.format(
            version=version) if self.base_url and version else self.base_url
        # Prepare path and params
        url_resources, platform_params = self.prepare_params(endpoint, params)

        # Make resulting URL
        # url=ba://se_url/resou/rces?p=ar&am=s
        # (Return None if endpoint turned to None)
        if endpoint and url_resources is None:
            return None, None
        if url_resources and url:
            url = urljoin(url + "/", "/".join(url_resources))
        if platform_params and is_join_get_params:
            url = url + "?" + urlencode(platform_params)
        return url, platform_params

    def prepare_params(self, endpoint=None, params=None):
        # Override in subclasses if it is the only way to adopt client to platform

        # Convert our code's names to custom platform's names
        platform_params = self._convert_params_to_platform(params, endpoint)
        self._convert_timestamp_values_to_platform(endpoint, platform_params)

        # Endpoint.TRADE -> "trades/ETHBTC" or "trades"
        platform_endpoint = self._get_platform_endpoint(endpoint, params)
        if platform_endpoint is None:
            return None, platform_params

        # Make path part of URL (as a list) using endpoint and params
        resources = [platform_endpoint] if platform_endpoint else []

        return resources, platform_params

    def _convert_params_to_platform(self, params, endpoint):
        # Convert our code's names to custom platform's names
        platform_params = {
            self._get_platform_param_name(key): self._process_param_value(
                key, value)
            for key, value in params.items() if value is not None
        } if params else {}
        # (Del not supported by platform params which defined in lookups as empty)
        platform_params.pop("", "")
        platform_params.pop(None, None)
        return platform_params

    def _convert_param_values_to_platform(self, params):
        # Convert our code's values to custom platform's values
        platform_params = {
            key: self._process_param_value(key, value)
            for key, value in params.items() if value is not None
        } if params else {}
        return platform_params

    def _process_param_value(self, name, value):
        # Convert values to platform values
        # if name in ParamValue.param_names:
        value = self._get_platform_param_value(value, name)
        return value

    def _get_platform_endpoint(self, endpoint, params):
        # Convert our code's endpoint to custom platform's endpoint

        # Endpoint.TRADE -> "trades/{symbol}" or "trades" or lambda params: "trades"
        platform_endpoint = self.endpoint_lookup.get(
            endpoint, endpoint) if self.endpoint_lookup else endpoint
        platform_params = self._convert_param_values_to_platform(params)
        if callable(platform_endpoint):
            platform_endpoint = platform_endpoint(platform_params)
        if platform_endpoint:
            # "trades", {"symbol": "ETHBTC"} => "trades" (no error)
            # "trades/{symbol}/hist", {"symbol": "ETHBTC"} => "trades/ETHBTC/hist"
            # "trades/{symbol}/hist", {} => Error!
            platform_endpoint = platform_endpoint.format(**platform_params)
        return platform_endpoint

    def _get_platform_param_name(self, name):
        # Convert our code's param name to custom platform's param name
        return self.param_name_lookup.get(
            name, name) if self.param_name_lookup else name

    def _get_platform_param_value(self, value, name=None):
        # Convert our code's param value to custom platform's param value
        lookup = self.param_value_lookup
        lookup_for_param = lookup.get(name, lookup) if lookup else None
        if isinstance(value, str) and name == ParamName.SYMBOL:
            if self.is_delimiter_used:
                value = value.replace(Currency.DELIMITER, self.symbol_delimiter
                                      or "")
        if isinstance(value, list):
            return value
        return lookup_for_param.get(value, value) if lookup_for_param else (
            lookup.get(value, value) if lookup else value)

    # Convert from platform format

    def preprocess_data(self, data, subscription, endpoint, symbol, params):
        return data

    def parse(self, endpoint, data):
        # if not endpoint or not data:
        #     self.logger.warning("Some argument is empty in parse(). endpoint: %s, data: %s", endpoint, data)
        #     return data
        if data is None or data == []:
            self.logger.debug(
                "Data argument is empty in parse(). endpoint: %s, data: %s",
                endpoint, data)
            return data

        # (If list of items data, but not an item data as a list)
        if isinstance(data, list):  # and not isinstance(data[0], list):
            result = [
                self._parse_item(endpoint, item_data) for item_data in data
            ]
            # (Skip empty)
            result = [item for item in result if item]
            return result
        else:
            return self._parse_item(endpoint, data)

    def _parse_item(self, endpoint, item_data):
        # Check item_class by endpoint
        if not endpoint or not self.item_class_by_endpoint or endpoint not in self.item_class_by_endpoint:
            self.logger.warning("Wrong endpoint: %s in parse_item().",
                                endpoint)
            return item_data
        item_class = self.item_class_by_endpoint[endpoint]

        # Create and set up item by item_data (using lookup to convert property names)
        item = self._create_and_set_up_object(item_class, item_data)
        item.endpoint = endpoint
        item = self._post_process_item(item, item_data)
        return item

    def _post_process_item(self, item, item_data=None):
        # Process parsed values (convert from platform)
        # Set platform_id
        if hasattr(item, ParamName.PLATFORM_ID) and item.platform_id is None:
            item.platform_id = self.platform_id
        if hasattr(item, ParamName.SYMBOL) and item.symbol:
            item.symbol = item.symbol.upper()
            if self.symbol_delimiter:
                item.symbol = item.symbol.replace(self.symbol_delimiter,
                                                  Currency.DELIMITER)
        # Stringify item_id
        if hasattr(item, ParamName.ITEM_ID) and item.item_id is not None:
            item.item_id = str(item.item_id)
        if hasattr(item, ParamName.ORDER_ID) and item.order_id is not None:
            item.order_id = str(item.order_id)
        # Convert timestamp
        # (If API returns milliseconds or string date we must convert them to Unix timestamp (in seconds or ms))
        # (Note: add here more timestamp attributes if you use another name in your VOs)
        if hasattr(item, self.ITEM_TIMESTAMP_ATTR):
            if item.timestamp:
                item.timestamp = self._convert_timestamp_from_platform(
                    item.timestamp)
            item.is_milliseconds = self.use_milliseconds

        # Convert asks and bids to OrderBookItem type
        if hasattr(item, ParamName.ASKS) and item.asks:
            item.asks = [
                self._create_and_set_up_object(OrderBookItem, item_data)
                for item_data in item.asks
            ]
        if hasattr(item, ParamName.BIDS) and item.bids:
            item.bids = [
                self._create_and_set_up_object(OrderBookItem, item_data)
                for item_data in item.bids
            ]
        # Convert items to Balance type
        # todo remove balances attribute as they were removed from account
        if hasattr(item, ParamName.BALANCES) and item.balances:
            item.balances = [
                self._create_and_set_up_object(Balance, item_data)
                for item_data in item.balances
            ]
            # Set platform_id
            for balance in item.balances:
                self._post_process_item(balance)

        return item

    def post_process_result(self, result, method, endpoint, params):
        # Process result using request data

        if isinstance(result, Error):
            return result

        self.propagate_param_to_result(ParamName.SYMBOL, params, result)
        self.propagate_param_to_result(ParamName.INTERVAL, params, result)
        # self.propagate_param_to_result(ParamName.LEVEL, params, result)
        return result

    @staticmethod
    def propagate_param_to_result(param_name, params, result):
        value = params.get(param_name) if params else None
        if value:
            if isinstance(result, list):
                for item in result:
                    if hasattr(item,
                               param_name) and not getattr(item, param_name):
                        setattr(item, param_name, value)
            else:
                if hasattr(result, param_name):
                    setattr(result, param_name, value)

    def parse_error(self, error_data=None, response=None):
        # (error_data=None and response!=None when REST API returns 404 and html response)
        if response and response.ok:
            return None

        result = self._create_and_set_up_object(Error, error_data) or Error()
        response_message = " (status: %s %s code: %s msg: %s)" % (
            response.status_code, response.reason, result.code, result.message) if response \
            else " (code: %s msg: %s)" % (result.code, result.message)
        if not result.code:
            result.code = response.status_code
        result.code = self.error_code_by_platform_error_code.get(result.code, result.code) \
            if self.error_code_by_platform_error_code else result.code
        result.message = ErrorCode.get_message_by_code(
            result.code) + response_message
        return result

    def _create_and_set_up_object(self, object_class, data):
        if not object_class or not data or isinstance(data, str):
            return None

        obj = object_class()
        lookup = self.param_lookup_by_class.get(
            object_class) if self.param_lookup_by_class else None
        if not lookup:
            # self.logger.error("There is no lookup for %s in %s", object_class, self.__class__)
            raise Exception("There is no lookup for %s in %s" %
                            (object_class, self.__class__))
        # (Lookup is usually a dict, but can be a list when item_data is a list)
        if lookup:
            key_pair = lookup.items() if isinstance(
                lookup, dict) else enumerate(lookup)
            is_data_dict = isinstance(data, dict)
            for platform_key, key in key_pair:
                if key and (not is_data_dict or platform_key in data):
                    # Convert value from platform
                    value = data[platform_key]
                    # if isinstance(value, float):
                    #     value = Decimal(value)
                    lookup = self.param_value_reversed_lookup.get(key) if key in self.param_value_reversed_lookup else \
                        self.param_value_reversed_lookup
                    value = lookup.get(value,
                                       value) if lookup and not isinstance(
                                           value, list) else value

                    if value is not None and value != "" and ParamName.is_decimal(
                            key):  # key in self.decimal_param_names:
                        value = Decimal(value)
                    setattr(obj, key, value)

        return obj

    # Convert from and to platform

    def _convert_timestamp_values_to_platform(self, endpoint, platform_params):
        if not platform_params:
            return
        timestamp_platform_names = self.timestamp_platform_names_by_endpoint.get(
            endpoint, self.timestamp_platform_names) \
            if self.timestamp_platform_names_by_endpoint else self.timestamp_platform_names
        if not timestamp_platform_names:
            return

        for name in timestamp_platform_names:
            if name in platform_params:
                value = platform_params[name]
                if isinstance(value, ValueObject):
                    value = getattr(value, self.ITEM_TIMESTAMP_ATTR, value)
                platform_params[name] = self._convert_timestamp_to_platform(
                    value)

    def _convert_timestamp_to_platform(self, timestamp):
        if not timestamp:
            return timestamp

        if self.use_milliseconds:
            timestamp /= 1000

        if self.is_source_in_milliseconds:
            timestamp *= 1000
        elif self.is_source_in_timestring:
            dt = datetime.utcfromtimestamp(timestamp)
            timestamp = dt.isoformat()
        return timestamp

    def _convert_timestamp_from_platform(self, timestamp):
        if not timestamp:
            return timestamp
        if self.is_source_in_milliseconds:
            timestamp /= 1000
            # if int(timestamp) == timestamp:
            #     timestamp = int(timestamp)
        elif self.is_source_in_timestring:
            timestamp = parser.parse(timestamp).timestamp()

        if self.use_milliseconds:
            timestamp = int(timestamp * 1000)
        return timestamp

    @classmethod
    def convert_items_to_obj_by_endpoint(cls, item_or_items, endpoint):
        item_format = item_format_by_endpoint.get(endpoint)
        item_type = cls.item_class_by_endpoint.get(endpoint)
        return convert_items_to_obj(item_or_items, item_format, item_type)


class BaseClient:
    """
    All time params are unix timestamps in seconds (float or int).
    """

    # Main params
    _log_prefix = "Client"
    platform_id = None
    version = None
    _api_key = None
    _api_secret = None
    _passphrase = None
    _credentials = None
    default_converter_class = ProtocolConverter
    _converter_class_by_version = None
    _converter_by_version = None
    max_log_len = 500
    max_items_in_log = 10

    helper = SingleDataAggregator()

    # If True then if "symbol" param set to None that will return data for "all symbols"
    IS_NONE_SYMBOL_FOR_ALL_SYMBOLS = False

    @property
    def headers(self):
        # Usually returns auth and other headers (Don't return None)
        # (as a dict for requests (REST) and a list for WebSockets (WS))
        return []

    @property
    def use_milliseconds(self):
        return self.converter.use_milliseconds

    @use_milliseconds.setter
    def use_milliseconds(self, value):
        self.converter.use_milliseconds = value

    def __init__(self, version=None, **kwargs) -> None:
        super().__init__()

        if version is not None:
            self.version = str(version)

        # Set up settings
        for key, value in kwargs.items():
            setattr(self, key, value)

        # Create logger
        platform_name = Platform.get_platform_name_by_id(self.platform_id)
        self.logger = logging.getLogger(
            "%s.%s.v%s" % (self._log_prefix, platform_name, self.version))
        # self.logger.debug("Create %s client for %s platform. url+params: %s",
        #                   self._log_prefix, platform_name, self.make_url_and_platform_params())

        # Create converter
        self.converter = self.get_or_create_converter()
        if not self.converter:
            raise Exception(
                "There is no converter_class in %s for version: %s. May be wrong version."
                % (self.__class__, self.version))

    # TODO add passphrase into credentials
    def set_credentials(self,
                        api_key,
                        api_secret,
                        passphrase=None,
                        credentials=None):
        self._api_key = api_key
        self._api_secret = api_secret
        self._passphrase = passphrase
        if credentials:
            self._credentials = credentials
            self._apply_credentials()

    def _apply_credentials(self):
        if self._credentials:
            # Needed for cases when credentials could change during application
            credentials = self._credentials() if callable(
                self._credentials) else self._credentials
            if credentials:
                if len(credentials) < 3:
                    self._api_key, self._api_secret = credentials
                else:
                    self._api_key, self._api_secret, self._passphrase = credentials

    def get_or_create_converter(self, version=None):
        # Converter stores all the info about a platform
        # Note: Using version to get converter at any time allows us to easily
        # switch version for just one request or for all further requests
        # (used for bitfinex, for example, to get symbols which enabled only for v1)

        if not version:
            version = self.version
        version = str(version)

        if not self._converter_by_version:
            self._converter_by_version = {}
        if version in self._converter_by_version:
            return self._converter_by_version[version]

        # Get class
        converter_class = self._converter_class_by_version.get(version) \
            if self._converter_class_by_version else self.default_converter_class
        # Note: platform_id could be set in converter or in client
        if not self.platform_id:
            self.platform_id = converter_class.platform_id
        # Create and store
        converter = converter_class(self.platform_id,
                                    version) if converter_class else None
        self._converter_by_version[version] = converter

        return converter

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


# REST


class RESTConverter(ProtocolConverter):
    # sorting values: ASCENDING, DESCENDING (newest first), None
    # DEFAULT_SORTING = Param.ASCENDING  # Const for current platform. See in param_name_lookup
    # todo rename to IS_SORTING_SUPPORTED
    IS_SORTING_ENABLED = False  # False - SORTING param is not supported for current platform
    sorting = Sorting.DESCENDING  # Choose default sorting for all requests

    sorting_endpoints = [
        Endpoint.TRADE,
        Endpoint.TRADE_MY,
        Endpoint.TRADE_HISTORY,
        Endpoint.TRADE_MY_HISTORY,
        Endpoint.CANDLE,
    ]
    secured_endpoints = [
        Endpoint.ACCOUNT,
        Endpoint.BALANCE,
        Endpoint.BALANCE_TRANSACTION,
        Endpoint.BALANCE_WALLET,
        Endpoint.TRADE_MY,
        Endpoint.ORDER,
        Endpoint.ORDER_CREATE,
        Endpoint.ORDER_CANCEL,
        Endpoint.ORDERS_OPEN,
        Endpoint.ORDERS_ALL,
        Endpoint.ORDERS_ALL_CANCEL,
        Endpoint.ORDER_TEST,
        Endpoint.POSITION,
        Endpoint.POSITION_CLOSE,
        Endpoint.REPAYMENT,
        Endpoint.TRANSFER,
        Endpoint.LEVERAGE_SET,
    ]

    # endpoint -> endpoint (if has different endpoint for history)
    history_endpoint_lookup = {
        Endpoint.TRADE: Endpoint.TRADE_HISTORY,
    }

    # endpoint -> platform_endpoint
    # Note: XXX_HISTORY should be always set if XXX is set (XXX is subset of XXX_HISTORY)
    endpoint_lookup = None
    max_limit_by_endpoint = None

    # todo
    rate_limit_period_sec = None
    rate_limit_count_for_period = None

    @property
    def default_sorting(self):
        # Default sorting for current platform if no sorting param is specified
        return self._get_platform_param_value(Sorting.DEFAULT_SORTING)

    def preprocess_params(self, endpoint, params):
        self._process_limit_param(endpoint, params)
        self._process_sorting_param(endpoint, params)
        # Must be after sorting added
        self._process_from_item_param(endpoint, params)
        self._process_id_params(endpoint, params)
        return params

    def _process_limit_param(self, endpoint, params):
        # (If LIMIT param is set to None (expected, but not defined))
        is_use_max_limit = self.is_use_max_limit or (
            params.pop(ParamName.IS_USE_MAX_LIMIT)
            if params and ParamName.IS_USE_MAX_LIMIT in params else False)
        is_limit_supported_here = params and ParamName.LIMIT in params
        if is_use_max_limit and is_limit_supported_here and params[
                ParamName.LIMIT] is None:
            value = self.max_limit_by_endpoint.get(
                endpoint, 1000000) if self.max_limit_by_endpoint else None
            if value is not None:
                # Set limit to maximum supported by a platform
                params[ParamName.LIMIT] = value

    def _process_sorting_param(self, endpoint, params):
        # (Add only if a platform supports it, and it is not already added)
        is_sorting_available = self.IS_SORTING_ENABLED and endpoint in self.sorting_endpoints

        if not is_sorting_available and ParamName.SORTING in params:
            del params[ParamName.SORTING]
        elif is_sorting_available and not params.get(ParamName.SORTING):
            params[ParamName.SORTING] = self.sorting

    def _get_real_sorting(self, params):
        sorting = params.get(ParamName.SORTING) if params else None
        return sorting or self.default_sorting

    def _process_from_item_param(self, endpoint, params):
        from_item = params.get(ParamName.FROM_ITEM)

        to_item = params.get(ParamName.TO_ITEM)
        is_descending = self._get_real_sorting(params) == Sorting.DESCENDING

        if not from_item or not to_item or not params:  # or not self.IS_SORTING_ENABLED:
            return

        # (from_item <-> to_item)
        # is_from_newer_than_to = getattr(from_item, self.ITEM_TIMESTAMP_ATTR, 0) > \
        #                         getattr(to_item, self.ITEM_TIMESTAMP_ATTR, 0)
        is_from_newer_than_to = (from_item.timestamp or 0) > (to_item.timestamp
                                                              or 0)

        # TODO: from_item should be greater than to_item for desc sorting
        # (read comments at test_fetch_history_from_and_to_item)
        if from_item and to_item and is_from_newer_than_to:
            params[ParamName.FROM_ITEM] = to_item
            params[ParamName.TO_ITEM] = from_item

        # (from_item -> to_item)
        if is_descending and not to_item:
            params[ParamName.TO_ITEM] = from_item
            del params[ParamName.FROM_ITEM]

    def _process_id_params(self, endpoint, params):
        ID_PARAM_NAMES = [
            ParamName.ITEM_ID, ParamName.ORDER_ID, ParamName.TRADE_ID
        ]
        for id_param_name in ID_PARAM_NAMES:
            # Convert item to item.item_id
            item = params.get(id_param_name)
            if item and isinstance(item, ItemObject):
                params[id_param_name] = item.item_id

    def process_secured(self,
                        method,
                        url,
                        endpoint,
                        platform_params,
                        headers,
                        api_key,
                        api_secret,
                        passphrase=None):
        if endpoint in self.secured_endpoints:
            platform_params, headers = self._generate_and_add_signature(
                method, url, endpoint, platform_params, headers, api_key,
                api_secret, passphrase)
        return url, platform_params, headers

    def _generate_and_add_signature(self,
                                    method,
                                    url,
                                    endpoint,
                                    platform_params,
                                    headers,
                                    api_key,
                                    api_secret,
                                    passphrase=None):
        # Generate and add signature here
        return platform_params, headers

    def post_process_result(self, result, method, endpoint, params):
        result = super().post_process_result(result, method, endpoint, params)
        result = self._filter_result_by_params(result, params)
        return result

    def _filter_result_by_params(self, result, params):
        # Filter items between from_item and to_item
        # (for example, for Binance which doesn't have to_id param for trade history)
        from_item = params.get(ParamName.FROM_ITEM)
        to_item = params.get(ParamName.TO_ITEM)
        from_time = time_util.get_timestamp(
            from_item.timestamp, self.use_milliseconds) if from_item else None
        to_time = time_util.get_timestamp(
            to_item.timestamp, self.use_milliseconds) if to_item else None
        # (For binance - items from queue in sec (ms truncated) and from platform in ms)
        if to_time is not None:
            to_time += 1000 if self.use_milliseconds else 1

        if from_item or to_item:
            # Filter result by from and to items
            filtered_result = self._filter_result(result, from_item, to_item,
                                                  from_time, to_time)

            if len(result) != len(filtered_result):
                log_method = self.logger.warning if len(
                    filtered_result) == 0 else self.logger.debug
                log_method(
                    "Filtering result: %s to %s items with from_item: %s and to_item: %s",
                    log_util.items_to_interval_string(result), len(result),
                    from_item, to_item)
            result = filtered_result

        return result

    def _filter_result(self,
                       result,
                       from_item=None,
                       to_item=None,
                       from_time=None,
                       to_time=None):
        # result = [item for item in result if
        #           (not from_time or item.timestamp >= from_time) and
        #           (not to_time or item.timestamp <= to_time)
        #           ]
        # return result

        filtered_result = []
        start_index = result.index(
            from_item) if from_item and from_item in result else -1
        for item in result[start_index:] if start_index > 0 else result:
            if not from_time or item.timestamp >= from_time:
                filtered_result.append(item)
                if item == to_item or to_time and item.timestamp > to_time:
                    break
        return filtered_result


class BaseRESTClient(BaseClient):
    # Settings:
    _log_prefix = "RESTClient"

    default_converter_class = RESTConverter
    is_block_on_rate_limit = False

    # State:
    is_rate_limit_caught = False
    delay_before_next_request_sec = 0
    pivot_symbol = None

    session = None
    _last_response_for_debugging = None

    @property
    def headers(self):
        return {
            "Accept": "application/json",
            "User-Agent": "client/python",
        }

    def __init__(self, version=None, **kwargs) -> None:
        super().__init__(version, **kwargs)
        self.session = requests.session()
        self.make_request = self.session.request

    def close(self):
        if self.session:
            self.session.close()

    # todo tests
    # Single entry point for all endpoints
    def fetch(self,
              endpoint,
              symbols=None,
              params=None,
              version=None,
              **kwargs):
        result = []
        if symbols is None:
            symbols = [None]
        # TODO add flag is_multi_symbols_allowed_for_request &
        #  is_multi_symbols_allowed_for_request_by_endpoint - if needed
        for symbol in symbols:
            item_params = {ParamName.SYMBOL: symbol}
            if params:
                item_params.update(params)
            request_result = self._send("GET", endpoint, item_params, version,
                                        **kwargs) or []
            if isinstance(request_result, list):
                result += request_result
            else:
                result.append(request_result)
        self.logger.debug(
            "Fetch endpoint: %s symbols: %s params: %s result: %s %s .. %s",
            endpoint, symbols, params,
            *((len(result), result[0], result[-1]) if result else
              (0, None, None)))
        return result

    def _send(self, method, endpoint, params=None, version=None, **kwargs):
        if self.is_rate_limit_caught and self.is_block_on_rate_limit:
            self.logger.warning(
                "It happened rate limit once, so all further sends will be skipped. "
                "Reset is_rate_limit_caught or set is_block_on_rate_limit=False to unlock the client."
            )
            return

        self._apply_credentials()

        converter = self.get_or_create_converter(version)

        # Prepare
        params = dict(**kwargs, **(params or {}))
        # params = dict(**kwargs, **params) if isinstance(params, dict) else kwargs
        # todo save initial symbol (before it will be converted to platform symbol in converter)
        params = converter.preprocess_params(endpoint, params)
        self.logger.debug(
            "(Sending: %s %s %s %s)", method, endpoint, params, "version: " +
            version if version and self.version != version else "")
        url, platform_params = converter.make_url_and_platform_params(
            endpoint, params, version=version)
        if not url:
            self.logger.error(
                "No url: %s for endpoint: %s params: %s (-> platform_params: %s)",
                url, endpoint, params, platform_params)
            return None
        if not method:
            self.logger.error("No method defined: %s", method)
            return None

        # Add signature
        url, platform_params, headers = converter.process_secured(
            method, url, endpoint, platform_params, self.headers,
            self._api_key, self._api_secret, self._passphrase)

        # Send
        kwargs = {"headers": headers}
        params_name = "params" if method.lower() == "get" else "data"
        kwargs[params_name] = platform_params
        self.logger.debug("Send: %s %s %s", method, url, platform_params)
        response = self.make_request(method, url, **kwargs)
        if not isinstance(response, requests.Response):
            return Error(code=ErrorCode.APP_ERROR, message="Connection error")

        # Parse
        self._last_response_for_debugging = response

        data_json, data_text = self._get_data_from_response(response)

        if not self._is_response_error(response, data_json, data_text):
            result = converter.parse(endpoint, data_json)
            result = converter.post_process_result(result, method, endpoint,
                                                   params)
        else:
            self.logger.error("Send to exchange failed. m: %s, u: %s, k: %s",
                              method, url, kwargs)
            result = converter.parse_error(
                data_json if data_json else data_text, response)
        # todo use initial symbol saved earlier if it was changed in converter to platform symbol
        self.logger.debug(
            "Response: %s %s\n Parsed result: %s",
            response,
            log_util.make_short_str(response.content, self.max_log_len),
            # response.content,  # temp
            log_util.items_to_interval_string(result, self.max_items_in_log)
            # result  # temp
        )
        self._on_response(response, result)

        # Return parsed value objects or Error instance
        return result

    def _is_response_error(self, response, data_json=None, data_text=None):
        return not response.ok

    def _get_data_from_response(self, response):
        try:
            data_json = response.json(parse_float=Decimal)
        except json.JSONDecodeError as err:
            data_json = None
            self.logger.error('JSONDecodeError: %s (response content: %s)',
                              err, response.content)
        data_text = response.text
        return data_json, data_text

    def _on_response(self, response, result):
        self._process_result_for_rate_limit(response, result)

    # todo fix ratelimit delay
    # reset_ratelimit_timestamp = None
    #
    # @property
    # def delay_before_next_request_sec(self):
    #     return min(0, self.reset_ratelimit_timestamp - time.time())

    def _process_result_for_rate_limit(self, response, result):
        pass
        # this is handled in proxy connector
        # # Set up delay_before_next_request_sec on rate limit error
        # self.delay_before_next_request_sec = 0
        # if isinstance(result, Error):
        #     if result.code == ErrorCode.RATE_LIMIT:
        #         self.ratelimit_error_in_row_count += 1
        #         self.delay_before_next_request_sec = 60 * 2 * self.ratelimit_error_in_row_count  # some number - change
        #         self.is_rate_limit_caught = True
        #     elif result.code == ErrorCode.IP_BAN:
        #         self.ratelimit_error_in_row_count += 1
        #         self.delay_before_next_request_sec = 60 * 10 * self.ratelimit_error_in_row_count  # some number - change
        #         self.is_rate_limit_caught = True
        #     else:
        #         self.ratelimit_error_in_row_count = 0

        #     if self.ratelimit_error_in_row_count:
        #         self.logger.info(
        #             "Rate limit error: %s in row. Wait %s seconds before next request.",
        #             self.ratelimit_error_in_row_count,
        #             self.delay_before_next_request_sec)
        # else:
        #     self.ratelimit_error_in_row_count = 0


class PlatformRESTClient(BaseRESTClient):
    """
    Important! Behavior when some param is None or for any other case should be same for all platforms.
    Important! from and to params must be including: [from, to], not [from, to) or (from, to).

    Закомментированные методы скорее всего не понадобятся, но на всякий случай они добавлены,
    чтобы потом не возвращаться и не думать заново.
    """
    # Settings
    is_futures = False

    # State
    _server_time_diff_s = None
    _symbols = None

    fetch_method_by_endpoint = {
        Endpoint.CURRENCY_PAIRS: 'fetch_currency_pairs',
        Endpoint.SYMBOLS: 'fetch_symbols',
        Endpoint.TRADE: 'fetch_trades',
        Endpoint.CANDLE: 'fetch_candles',
        Endpoint.TICKER: 'fetch_ticker',
        Endpoint.TICKER_ALL: 'fetch_tickers',
        Endpoint.ORDER_BOOK: 'fetch_order_book',
        Endpoint.QUOTE: 'fetch_quote',
    }

    def ping(self, version=None, **kwargs):
        endpoint = Endpoint.PING
        return self._send("GET", endpoint, version=version, **kwargs)

    def get_server_timestamp(self,
                             force_from_server=False,
                             version=None,
                             **kwargs):
        endpoint = Endpoint.SERVER_TIME

        if not force_from_server and self._server_time_diff_s is not None:
            # (Calculate using time difference with server taken from previous call)
            result = self._server_time_diff_s + time.time()
            return int(result * 1000) if self.use_milliseconds else result

        time_before = time.time()

        result = self._send("GET", endpoint, version=version, **kwargs)
        if isinstance(result, Error):
            return result
        if result:
            # (Update time diff)
            self._server_time_diff_s = (result / 1000 if self.use_milliseconds else
                                        result) - time_before
        return result

    def call_fetch_by_endpoint(self, endpoint, **kwargs):
        call_method = self.fetch_method_by_endpoint[endpoint]
        return self.__getattribute__(call_method)(**kwargs)

    def fetch_currency_pairs(self, version=None, **kwargs):
        endpoint = Endpoint.CURRENCY_PAIRS
        return self._send("GET", endpoint, version=version,**kwargs)

    def get_symbols(self, force_fetching=False, version=None, **kwargs):
        if not self._symbols or force_fetching:
            # self._symbols = self.fetch_symbols(version, **kwargs)
            self.fetch_symbols(version, **kwargs)
        return self._symbols

    def fetch_symbols(self, version=None, **kwargs):
        endpoint = Endpoint.SYMBOLS
        response = self._send("GET", endpoint, version=version, **kwargs)
        if not isinstance(response, Error):
            self._symbols = Currency.convert_to_symbols(response)
            return self._symbols
        else:
            return response

    def fetch_history(self,
                      endpoint,
                      symbol,
                      limit=None,
                      from_item=None,
                      to_item=None,
                      sorting=None,
                      is_use_max_limit=False,
                      from_time=None,
                      to_time=None,
                      version=None,
                      **kwargs):
        # Common method for fetching history for any endpoint. Used in REST connector.

        # (Convert endpoint to history endpoint if they differ)
        history_endpoint_lookup = self.converter.history_endpoint_lookup
        endpoint = history_endpoint_lookup.get(
            endpoint, endpoint) if history_endpoint_lookup else endpoint
        params = {
            ParamName.SYMBOL: symbol,
            ParamName.LIMIT: limit,
            ParamName.FROM_ITEM: from_item,
            ParamName.TO_ITEM: to_item,
            ParamName.SORTING: sorting,
            ParamName.IS_USE_MAX_LIMIT: is_use_max_limit,
            ParamName.FROM_TIME: from_time,
            ParamName.TO_TIME: to_time,
        }

        self.logger.debug("fetch_history from: %s to: %s", from_item
                          or from_time, to_item or to_time)
        result = self._send("GET", endpoint, params, version, **kwargs)
        return result

    # Trade

    def fetch_trades(self, symbol, limit=None, version=None, **kwargs):
        # Fetch current (last) trades to display at once.

        endpoint = Endpoint.TRADE
        params = {
            ParamName.SYMBOL: symbol,
            ParamName.LIMIT: limit,
        }

        result = self._send("GET", endpoint, params, version, **kwargs)
        return result

    def fetch_trades_history(self,
                             symbol,
                             limit=None,
                             from_item=None,
                             to_item=None,
                             sorting=None,
                             is_use_max_limit=False,
                             from_time=None,
                             to_time=None,
                             version=None,
                             **kwargs):
        # Fetching whole trades history as much as possible.
        # from_time and to_time used along with from_item and to_item as we often need to fetch
        # history by time and only Binance (as far as I know) doesn't support that (only by id)

        return self.fetch_history(Endpoint.TRADE, symbol, limit, from_item,
                                  to_item, sorting, is_use_max_limit,
                                  from_time, to_time, version, **kwargs)

    # Candle

    # todo sorting
    def fetch_candles(self,
                      symbol,
                      interval,
                      limit=None,
                      from_time=None,
                      to_time=None,
                      is_use_max_limit=False,
                      version=None,
                      **kwargs):
        endpoint = Endpoint.CANDLE
        params = {
            ParamName.SYMBOL: symbol,
            ParamName.INTERVAL: interval,
            ParamName.LIMIT: limit,
            ParamName.FROM_TIME: from_time,
            ParamName.TO_TIME: to_time,
            ParamName.IS_USE_MAX_LIMIT: is_use_max_limit,
        }

        result = self._send("GET", endpoint, params, version, **kwargs)
        return result

    # Ticker

    def fetch_ticker(self, symbol=None, version=None, **kwargs):
        # if not symbol:
        #     return symbol

        endpoint = Endpoint.TICKER
        params = {
            ParamName.SYMBOL: symbol,
        }

        result = self._send("GET", endpoint, params, version, **kwargs)
        return result

    def fetch_tickers(self, symbols=None, version=None, **kwargs):
        endpoint = Endpoint.TICKER_ALL
        # (Send None for all symbols)
        # params = {
        #     ParamName.SYMBOLS: None,
        # }

        result = self._send("GET", endpoint, None, version, **kwargs)

        if symbols:
            # Filter result for symbols defined
            symbols = [
                symbol.upper() if symbol else symbol for symbol in symbols
            ]
            return [item for item in result if item.symbol in symbols]

        return result

    # Order Book

    def fetch_order_book(self,
                         symbol=None,
                         limit=None,
                         is_use_max_limit=False,
                         version=None,
                         **kwargs):
        # Level 2 (price-aggregated) order book for a particular symbol.

        endpoint = Endpoint.ORDER_BOOK
        params = {
            ParamName.SYMBOL: symbol,
            ParamName.LIMIT: limit,
        }

        result = self._send("GET", endpoint, params, version, **kwargs)
        return result

    def fetch_quote(self, symbol: str = None, version: str = None, **kwargs):
        endpoint = Endpoint.QUOTE
        params = {
            ParamName.SYMBOL: symbol,
        }
        result = self._send("GET", endpoint, params, version, **kwargs)
        return result


class PrivatePlatformRESTClient(PlatformRESTClient):
    # Base currencies for positions (to do not use a balance as position intended to be closed)
    is_position_supported = False
    is_balance_transactions_supported = False
    use_balance_as_position = True

    wait_before_fetch_s = 0
    supported_order_types = (OrderType.LIMIT, OrderType.MARKET)

    fetch_method_by_endpoint = PlatformRESTClient.fetch_method_by_endpoint
    fetch_method_by_endpoint.update({
        Endpoint.BALANCE: 'fetch_balance',
        Endpoint.POSITION: 'get_positions',
        Endpoint.TRADE_MY: 'fetch_my_trades',
        Endpoint.ORDER: 'fetch_orders',
    })

    def __init__(self,
                 api_key=None,
                 api_secret=None,
                 passphrase=None,
                 version=None,
                 credentials=None,
                 **kwargs) -> None:
        super().__init__(version=version, **kwargs)
        self.set_credentials(api_key, api_secret, passphrase, credentials)


    def _send(self, method, endpoint, params=None, version=None, **kwargs):
        if endpoint == Endpoint.BALANCE_TRANSACTION and not self.is_balance_transactions_supported:
            return None
        return super()._send(method, endpoint, params, version, **kwargs)

    # Trades

    def check_credentials(self, version=None, **kwargs):
        endpoint = Endpoint.BALANCE
        params = {}

        result = self._send("GET", endpoint, params, version, **kwargs)
        is_list_error = isinstance(
            result, list) and len(result) > 0 and isinstance(result[0], Error)
        return not (isinstance(result, Error) or is_list_error)

    def set_leverage(self, leverage, symbol=None, version=None, **kwargs):
        endpoint = Endpoint.LEVERAGE_SET
        params = {
            ParamName.LEVERAGE: leverage,
            ParamName.SYMBOL: symbol,
        }
        result = self._send("POST",
                            endpoint,
                            params,
                            version=version,
                            **kwargs)
        return result

    def get_account_info(self, version=None, **kwargs):
        # Balance included to account
        endpoint = Endpoint.ACCOUNT
        params = {}

        result = self._send("GET", endpoint, params, version, **kwargs)
        return result

    # todo fetch_balances()?
    def fetch_balance(self, version=None, **kwargs):
        # Balance included to account
        endpoint = Endpoint.BALANCE
        params = {}

        result = self._send("GET", endpoint, params, version, **kwargs)
        return result

    def fetch_balance_transactions(self,
                                   limit=None,
                                   page=None,
                                   is_only_by_user=False,
                                   version=None,
                                   **kwargs):
        # Balance included to account
        endpoint = Endpoint.BALANCE_TRANSACTION
        params = {
            ParamName.LIMIT: limit,
            ParamName.LIMIT_SKIP: limit * page if limit and page else None
        }
        # params = {}

        result = self._send("GET", endpoint, params, version, **kwargs)
        if is_only_by_user and isinstance(result, list):
            result = [
                item for item in result if
                TransactionType.check_is_created_by_user(item.transaction_type)
            ]

        return result

    def fetch_my_trades(self,
                        symbol,
                        limit=None,
                        from_item=None,
                        version=None,
                        **kwargs):
        endpoint = Endpoint.TRADE_MY
        params = {
            ParamName.SYMBOL: symbol,
            ParamName.LIMIT: limit,
            ParamName.FROM_ITEM: from_item,
        }

        result = self._send("GET", endpoint, params, version, **kwargs)
        return result

    # def fetch_my_trades_history(self, symbol, limit=None, from_item=None, to_item=None,
    #                             sorting=None, is_use_max_limit=False, version=None, **kwargs):
    #     pass

    # Order (private)
    def _adjsut_order_parameters(self,
                                 symbol,
                                 amount,
                                 price=None,
                                 price_stop=None,
                                 price_limit=None):
        assert (symbol and amount)
        assert (price is not None or (price_limit or price_stop))
        symbol: CurrencyPair = self.helper.get_currency_pair(self.platform_id, symbol)
        assert symbol
        if not (symbol.lot_step and symbol.price_step):
            self.logger.error("Symbol info for %s missing", symbol)
            return amount, price, price_stop, price_limit
        amount = dtz(int(Decimal(amount) / symbol.lot_step) * symbol.lot_step)
        if symbol.min_notional and amount < symbol.min_notional:
            self.logger.warning(
                "Order amount %s less than minimal notional filter %s.",
                amount, symbol.min_notional)
        if price:
            price = dtz(
                int(Decimal(price) / symbol.price_step) * symbol.price_step)
        if price_stop:
            price_stop = dtz(
                int(Decimal(price_stop) / symbol.price_step) *
                symbol.price_step)
        if price_limit:
            price_limit = dtz(
                int(Decimal(price_limit) / symbol.price_step) *
                symbol.price_step)
        return str(amount), str(price), str(price_stop), str(price_limit)

    # todo add volume=None as amount*price. Price or volume will be used and converted to each other
    #  (for BitMEX volume is used instead of price)
    def create_order(self,
                     symbol,
                     order_type,
                     direction,
                     amount=None,
                     price=None,
                     is_test=False,
                     price_stop=None,
                     price_limit=None,
                     version=None,
                     **kwargs):
        if order_type not in self.supported_order_types:
            msg = "Not supported order type %s" % OrderType.name_by_value.get(
                order_type)
            self.logger.error(msg)
            return Error(code=ErrorCode.WRONG_PARAM, message=msg)
        endpoint = Endpoint.ORDER_TEST if is_test else Endpoint.ORDER_CREATE
        amount, price, price_stop, price_limit = self._adjsut_order_parameters(
            symbol, amount, price, price_stop, price_limit)
        params_base = {
            ParamName.SYMBOL: symbol,
            ParamName.ORDER_TYPE: order_type,
            ParamName.DIRECTION: direction,
            ParamName.AMOUNT: amount
        }
        if order_type in [OrderType.MARKET, OrderType.LIMIT]:
            params_extra = {
                ParamName.PRICE:
                price if order_type == OrderType.LIMIT else None,
            }
        else:
            params_extra = {
                ParamName.PRICE_STOP: price_stop,
                ParamName.PRICE_LIMIT: price_limit,
            }
        params = {**params_base, **params_extra}
        result = self._send("POST",
                            endpoint,
                            params,
                            version=version,
                            **kwargs)
        return result

    def cancel_order(self, order, symbol=None, version=None, **kwargs):
        # symbol needed when order is order_id
        if isinstance(order, Order) and order.is_closed:
            self.logger.info(
                "Order %s is already closed and cannot be canceled.", order)
            return order

        endpoint = Endpoint.ORDER_CANCEL
        params = {
            # ParamName.ORDER_ID: order.item_id if isinstance(order, Order) else order,
            ParamName.ORDER_ID:
            order,
            ParamName.SYMBOL:
            symbol,
            # move to converter(?):  or (order.symbol if hasattr(order, ParamName.SYMBOL) else None),
        }

        result = self._send("DELETE", endpoint, params, version, **kwargs)

        # (BitMEX returns list even for 1 order)
        if isinstance(result, list) and len(result) == 1:
            return result[0]

        return result

    def cancel_all_orders(self, symbol=None, version=None, **kwargs):
        if Endpoint.ORDERS_ALL_CANCEL in self.converter.endpoint_lookup:
            endpoint = Endpoint.ORDERS_ALL_CANCEL
            params = {
                ParamName.SYMBOL: symbol,
            }

            result = self._send("DELETE", endpoint, params, version, **kwargs)
        else:
            orders = self.fetch_orders(symbol,
                                       version,
                                       is_open_only=True,
                                       **kwargs)
            if isinstance(orders, Error):
                return orders
            result = []
            if orders:
                for order in orders:
                    result.append(
                        self.cancel_order(order, symbol, version, **kwargs))
        return result

    # ? todo test
    # def cancel_all_orders(self, symbol=None, version=None, **kwargs):
    #     orders = self.fetch_orders(symbol, version, **kwargs)
    #     for order in orders:
    #         self.cancel_order(order, symbol, version, **kwargs)

    # was check_order
    def fetch_order(self, order_or_id, symbol=None, version=None,
                    **kwargs):  # , direction=None
        # item_id should be enough, but some platforms also need symbol and direction
        endpoint = Endpoint.ORDER
        if isinstance(order_or_id, Order):
            symbol = order_or_id.symbol
        params = {
            ParamName.SYMBOL: symbol,
            ParamName.ORDER_ID: order_or_id,
            # ParamName.: ,
        }

        result = self._send("GET", endpoint, params, version, **kwargs)
        return result

    def fetch_orders(self,
                     symbol=None,
                     limit=None,
                     from_item=None,
                     is_open_only=False,
                     version=None,
                     **kwargs):
        if is_open_only:
            endpoint = Endpoint.ORDERS_OPEN
            kwargs[ParamName.IS_OPEN] = is_open_only
        else:
            endpoint = Endpoint.ORDERS_ALL

        params = {
            ParamName.SYMBOL: symbol,
            # ParamName.: ,
            ParamName.LIMIT: limit,
            ParamName.FROM_ITEM: from_item,
            # ParamName.: ,
        }
        result = self._send("GET", endpoint, params, version, **kwargs)
        return result

    # About positions.
    # Each position is created after some order starts executing, is partially or fully filled.
    # Position is still existing even if no order left open. If you have some contracts bought
    # (for BitMEX which has positions in API) or some currency bought which is not a base one
    # (for Binance which doesn't have any positions in API but have balances), then this is a
    # position. For BitMEX, and the most other similar platforms if you buy some amount of a
    # symbol and then sell some other amount, then these amounts subtracted, while in some other
    # stock exchanges there will be created to different positions: buying and selling.

    def get_positions(self,
                      symbol: str = None,
                      limit=None,
                      is_open_only: bool = False,
                      version=None,
                      **kwargs):
        result = []
        if self.is_position_supported:
            endpoint = Endpoint.POSITION
            params = {
                ParamName.SYMBOL: symbol,
                ParamName.LIMIT: limit,
            }

            result = self._send("GET", endpoint, params, version, **kwargs)
        elif self.use_balance_as_position:
            if not self.pivot_symbol:
                return Error(code=ErrorCode.WRONG_PARAM,
                             message="pivot symbol missing")
            # subsequent to do unclear
            # todo use Endpoint.POSITION: "balance" instead
            balances = self.fetch_balance(version=version,
                                                         **kwargs)
            if not isinstance(balances, list):
                return

            # mot generator because of complex hardly-readable condition and limit parameter
            for balance in balances:
                position = self.helper.create_position_if_exist(balance, self.pivot_symbol, symbol)
                if position:
                    result.append(position)
                    if limit and len(result) >= limit:
                        break


        if result and is_open_only:
            result = [p for p in result if p.is_open]

        return result

    def close_position(self, position: Position, version=None, **kwargs):
        if self.is_position_supported:
            result = self._close_position(position, version, **kwargs)
        elif self.use_balance_as_position:
            if not (position and self.pivot_symbol):
                return Error(code=ErrorCode.WRONG_PARAM,
                             message="missing params")
            assert (position.amount)
            amount = position.amount
            # referencing_pair: CurrencyPair = self.helper.get_currency_pair(self.platform_id, position.symbol)
            # if referencing_pair.base == self.pivot_symbol:
                # amount = self.helper.convert_amount_to_pivot(self.platform_id,
                #                                              position.amount,
                #                                              referencing_pair.base,
                #                                              referencing_pair.quote)
            result = self.create_order(
                position.symbol if position else position,
                OrderType.MARKET,
                Direction.inverse(position.direction) if position else None,
                amount,
                price=0,
                version=version)
            if isinstance(result, Error):
                return result
            else:
                position.is_open = False
                return position
        else:
            result = None
            self.logger.warning(
                "Positions are not supported by this platform. ")

        return result

    def _close_position(self, position_or_symbol, version=None, **kwargs):
        endpoint = Endpoint.POSITION_CLOSE
        params = {
            ParamName.SYMBOL:
            position_or_symbol.symbol if hasattr(
                position_or_symbol, ParamName.SYMBOL) else position_or_symbol,
        }

        result = self._send("POST", endpoint, params, version, **kwargs)
        return result

    def close_all_positions(self, symbol=None, version=None, **kwargs):
        if self.is_position_supported:
            result = self._close_all_positions(symbol, version, **kwargs)
            # if not isinstance(result, (list, Error)):
            if isinstance(result, Position):
                result = [result]
        elif self.use_balance_as_position:
            if not self.pivot_symbol:
                return Error(code=ErrorCode.WRONG_PARAM,
                             message="pivot symbol missing")
            positions = self.get_positions(symbol=symbol,
                                           limit=None,
                                           version=version,
                                           **kwargs)
            if not positions or not len(positions):
                return Error(code=ErrorCode.WRONG_SYMBOL,
                             message="No position having the criteria")
            result = [
                self.close_position(position, version=version, **kwargs)
                for position in positions
            ]
        else:
            self.logger.warning(
                "Positions are not supported by this platform. ")
            result = []
        return result

    def _close_all_positions(self, symbol=None, version=None, **kwargs):
        endpoint = Endpoint.POSITION_CLOSE
        params = {
            ParamName.SYMBOL: symbol,
        }

        result = self._send("POST", endpoint, params, version, **kwargs)
        return result


# WebSocket


class WSConverter(ProtocolConverter):
    # Indicates that there are separated command for subscribing to endpoints that can be used
    # during an established connection
    IS_SUBSCRIPTION_COMMAND_SUPPORTED = True
    is_orderbook_snapshot_goes_first = True
    is_receive_current_data_on_subscribe = False

    # supported_endpoints = None
    # symbol_endpoints = None  # In subclass you can call REST API to get symbols
    supported_endpoints = [
        Endpoint.TRADE,
        Endpoint.CANDLE,
        Endpoint.TICKER,
        Endpoint.TICKER_ALL,
        Endpoint.ORDER_BOOK,
        Endpoint.ORDER_BOOK_DIFF,
        Endpoint.QUOTE,
    ]
    symbol_endpoints = [
        Endpoint.TRADE,
        Endpoint.CANDLE,
        Endpoint.TICKER,  # can be used as symbol and as generic endpoint
        Endpoint.ORDER_BOOK,
        Endpoint.ORDER_BOOK_DIFF,
        Endpoint.ORDER_BOOK_AGG,
        Endpoint.QUOTE,
    ]
    # generic_endpoints = None  # = supported_endpoints.difference(symbol_endpoints)
    supported_symbols = None

    # Converting info:
    # For converting to platform

    # For parsing from platform
    subscription_param = None
    event_type_param = None
    endpoint_by_event_type = None
    item_class_by_endpoint = dict(
        **ProtocolConverter.item_class_by_endpoint,
        **{
            # # Item class by event type
            # "error": Error,
            # "info": Info,
            # "subscribed": Channel,
        })
    endpoint_symbol_params_by_subscription = None

    # todo convert back only those symbols (and for those endpoints) in items to be returned which were converted
    #  during request:
    # convert_result_symbol_by_platform_symbol_by_endpoint = None
    #  Set up this dict on subscription and use on result. Note: convert symbol only into duplicated item, the initial
    #  item should be always returned along with the duplicated one.

    def __init__(self, platform_id=None, version=None):
        super().__init__(platform_id, version)
        self.endpoint_symbol_params_by_subscription = {}

    # For converting time

    @property
    def generic_endpoints(self):
        # Non-symbol endpoints
        return set(self.supported_endpoints).difference(self.symbol_endpoints or set()) \
            if self.supported_endpoints else set()

    def generate_subscriptions(self, endpoints, symbols, **params):
        result = set()
        for endpoint in endpoints:
            params_list = self._break_params_to_params_list(endpoint,
                                                            params) or [{}]
            for params_item in params_list:
                if endpoint in self.symbol_endpoints:
                    if symbols:
                        for symbol in symbols:
                            # (There is an exception when setting symbol in method params and in **params)
                            subscription_params = {
                                ParamName.SYMBOL: symbol,
                                **params_item
                            }
                            result.add(
                                self._generate_subscription(
                                    endpoint, **subscription_params))
                    else:
                        result.add(
                            self._generate_subscription(
                                endpoint, **params_item))
                else:
                    result.add(
                        self._generate_subscription(endpoint, **params_item))
        return result

    def _break_params_to_params_list(self, endpoint, params):
        if not params:
            return []

        result = None
        for name, value in params.items():
            # Don't break platform_id-s for ORDER_BOOK_AGG (for MDS, as aggregated order book is available only there)
            is_break_values_allowed = endpoint != Endpoint.ORDER_BOOK_AGG or name != ParamName.PLATFORM_ID

            result = self._introduce_values_to_params_list(
                result, name, value, is_break_values_allowed)

        return result

    def _introduce_values_to_params_list(self,
                                         params_list,
                                         name,
                                         values,
                                         is_break_values_allowed=True):
        # Skip
        if not name or not values:
            return params_list
        # Set up empty for start
        if not params_list:
            params_list = [{}]
        # Values to iterable
        values = values if isinstance(
            values,
            (list, tuple, set)) and is_break_values_allowed else [values]
        is_one_value = len(values) == 1

        result = []
        for value in values:
            for params in params_list:
                if not is_one_value:
                    params = deepcopy(params)
                params[name] = value
                result.append(params)

        return result

    def _store_subscription(self, endpoints, symbols=None, **params):
        params = {
            k: v if isinstance(v, list) else [v]
            for k, v in params.items()
        }
        symbols = symbols or [""]
        i_v = itertools.product(*list(params.values()))
        i_k = params.keys()
        for endpoint, symbol, param in itertools.product(
                endpoints, symbols, map(lambda a: dict(zip(i_k, a)), i_v)):
            for subscription in self.generate_subscriptions([endpoint],
                                                            [symbol], **param):
                self.endpoint_symbol_params_by_subscription[subscription] = (
                    endpoint, symbol, params)

    def _generate_subscription(self, endpoint, symbol=None, **params):
        # Channel name - subscription
        params = {ParamName.SYMBOL: symbol, **params}
        subscription = self._get_platform_endpoint(endpoint, params)
        # Save to get endpoint and other params by subscription on parsing
        return subscription

    # def _get_subscription_by_item(self, item):
    #     if not item:
    #         return None
    #
    #     item_class = item.__class__
    #     endpoints = [e for e, ic in self.item_class_by_endpoint.items() if ic == item_class]
    #     endpoint = endpoints[0] if endpoints else None
    #
    #     symbol = item.symbol
    #
    #     params = {}
    #     if isinstance(item, Candle):
    #         params[ParamName.INTERVAL] = item.interval
    #     # if isinstance(item, OrderBook):
    #     #     params[ParamName.LEVEL] =
    #
    #     channel = self._generate_subscription(endpoint, symbol, **params)
    #     return channel

    def get_subscription_info(self, endpoint, data):
        if isinstance(data, list):
            subscription = data[0].get(
                self.subscription_param) if self.subscription_param else None
        else:
            subscription = data.get(
                self.subscription_param) if self.subscription_param else None
        # Get endpoint and other data by subscription name
        symbol, params = None, None
        if self.endpoint_symbol_params_by_subscription:
            prev_endpoint = endpoint
            if subscription:
                endpoint, symbol, params = self.endpoint_symbol_params_by_subscription.get(
                    subscription, (endpoint, None, None))
            if prev_endpoint and endpoint != prev_endpoint:
                self.logger.warning(
                    "Endpoint: %s changed to: %s for subscription: %s",
                    prev_endpoint, endpoint, subscription)
        return subscription, endpoint, symbol, params

    def parse(self, endpoint, data):
        # (Get endpoint from event type)
        event_type = None
        if not endpoint and data and isinstance(
                data, dict) and self.event_type_param:
            event_type = endpoint = data.get(self.event_type_param, endpoint)

        endpoint = self.endpoint_by_event_type.get(endpoint, endpoint) \
            if self.endpoint_by_event_type else endpoint
        # if not endpoint:
        #     self.logger.error("Cannot find event type by name: %s in data: %s", self.event_type_param, data)
        # self.logger.debug("Endpoint: %s by name: %s in data: %s", endpoint, self.event_type_param, data)

        if isinstance(endpoint, list):
            # If different endpoints has same platform_endpoint -
            # parse same data as different types, e.g. tickers and candles for BitMEX
            endpoints = endpoint
            result = (super().parse(endpoint, data) for endpoint in endpoints)
        else:
            result = super().parse(endpoint, data)
            # subscription acts as unique id for combination (endpoint, symbol, params)
            # result.subscription = event_type or self._get_subscription_by_item(result)

            # result.subscription = self._get_subscription_by_item(result) or event_type

        return result


# TODO resubscribe on <WSClient.BitMEX.v1> On message: {"status":500,"error":"Rate limit exceeded, retry in 1 seconds.","meta":{"request":{"op":"subscribe","args":["trade:XBTUSD"]}}}
class WSClient(BaseClient):
    """
    Using:
        client = WSClient(api_key, api_secret)
        client.subscribe([Endpoint.TRADE], ["ETHUSD", "ETHBTC"])
        # (Will reconnect for platforms which needed that)
        client.subscribe([Endpoint.TRADE], ["ETHBTC", "ETHUSD"])
        # Resulting subscriptions: [Endpoint.TRADE] channel for symbols:
        # ["ETHUSD", "ETHBTC", "ETHBTC", "ETHUSD"]
    """
    # Settings:
    _log_prefix = "WSClient"

    default_converter_class = WSConverter

    is_auto_reconnect = True
    ping_interval_sec = 5
    reconnect_delay_sec = 3
    reconnect_count = 3

    def on_connect(self):
        pass

    def on_data(self, items):
        pass

    def on_data_item(self, items):
        pass

    # on_data_item_with_subscription = None
    def on_disconnect(self):
        pass

    _subscription_limit_by_endpoint = {}
    # State:
    # # Subscription sets
    # # endpoints = None
    # # symbols = None
    # symbols_by_endpoint = None
    # # endpoints + symbols = subscriptions
    # current_subscriptions = None
    # pending_subscriptions = None
    # successful_subscriptions = None
    # failed_subscriptions = None

    # Indicates that subscription was made using connection url.
    # It's not the opposite of `IS_SUBSCRIPTION_COMMAND_SUPPORTED`.
    is_subscribed_with_url = False
    connecting_message_queue = None
    THROTTLE_MAX_DELAY = 60

    # Connection
    is_started = False
    _is_reconnecting = True
    _reconnect_tries = 0
    ws = None
    ping_thread = None
    thread = None
    _data_buffer = None
    _prev_ping_timestamp = None
    _is_ping = False
    lock = RLock()

    subscription_limit_by_endpoint = {}

    @property
    def url(self):
        # Override if you need to introduce some get params
        # (Set self.is_subscribed_with_url=True if subscribed in here in URL)
        url, platform_params = self.converter.make_url_and_platform_params()
        return url if self.converter else ""

    @property
    def is_connecting(self):
        return self.is_started and not self.is_connected

    @property
    def is_connected(self):
        return self.ws.sock.connected if self.ws and self.ws.sock else False
        # return self.is_started and not self._is_reconnecting

    def __init__(self,
                 api_key=None,
                 api_secret=None,
                 version=None,
                 credentials=None,
                 **kwargs) -> None:
        super().__init__(version, **kwargs)
        self.symbols_by_endpoint = defaultdict(set)
        self.set_credentials(api_key, api_secret, credentials=credentials)

        self.current_subscriptions = set()
        self.failed_subscriptions = set()
        self.pending_subscriptions = set()
        self.successful_subscriptions = set()
        self.throttle_counter = [datetime.now().minute, 0]

        # (For convenience)
        self.IS_SUBSCRIPTION_COMMAND_SUPPORTED = self.converter.IS_SUBSCRIPTION_COMMAND_SUPPORTED

    # Subscription

    def get_adding_subscriptions_for(self,
                                     endpoints=None,
                                     symbols=None,
                                     **params):
        # if not endpoints:
        #     # endpoints = self.endpoints or self.converter.supported_endpoints
        #     endpoints = self.symbols_by_endpoint or self.converter.supported_endpoints
        if not endpoints and not symbols:
            return self.symbols_by_endpoint, None, self.current_subscriptions.copy(
            )

        return self._get_subscriptions_for(endpoints, symbols, **params)

    def get_removing_subscriptions_for(self,
                                       endpoints=None,
                                       symbols=None,
                                       **params):
        if not endpoints and not symbols:
            return self.symbols_by_endpoint, None, self.current_subscriptions.copy(
            )

        return self._get_subscriptions_for(endpoints, symbols, **params)

    def _get_subscriptions_for(self, endpoints=None, symbols=None, **params):
        if not endpoints:
            # ?? self.endpoints
            # ?? self.converter.supported_endpoints
            # endpoints = self.endpoints or self.converter.supported_endpoints
            endpoints = self.symbols_by_endpoint or self.converter.supported_endpoints
        # else:

        # subscribe(symbols_by_endpoints) -> subscribe(endpoint, symbols)
        if isinstance(endpoints, dict):
            endpoints = set()
            symbols = set()
            subscriptions = set()
            symbols_by_endpoints = endpoints
            for endpoint, symbols in symbols_by_endpoints:
                if endpoint:
                    _endpoints, _symbols, _subscriptions = self._get_subscriptions_for(
                        endpoint, symbols, **params)
                    endpoints.update(_endpoints)
                    symbols.update(_symbols)
                    subscriptions.update(_subscriptions)
            return endpoints, symbols, subscriptions

        endpoints = Endpoint.convert_to_endpoints(endpoints)
        endpoints = set(endpoints).intersection(
            self.converter.supported_endpoints)

        if not endpoints:
            return None, None, None

        if not symbols or not isinstance(symbols, Iterable):
            # ??
            # symbols = self.symbols or self.converter.supported_symbols
            symbols = self.converter.supported_symbols
        # else:
        #     symbols = Symbol.convert_to_symbols(symbols)
        symbols = Currency.convert_to_symbols(symbols)

        subscriptions = self.converter.generate_subscriptions(
            endpoints, symbols, **params)
        return endpoints, symbols, subscriptions

    def subscribe(self, endpoints=None, symbols=None, **params):
        """
        Subscribe and connect.

        None means all: all previously subscribed or (if none) all supported.

            subscribe()  # subscribe to all supported endpoints (currently only generic ones)
            unsubscribe()  # unsubscribe all
            subscribe(symbols=["BTCUSD"])  # subscribe to all supported endpoints for "BTCUSD"
            unsubscribe(endpoints=["TRADE"])  # unsubscribe all "TRADE" channels - for all symbols
            unsubscribe()  # unsubscribe all (except "TRADE" which has been already unsubscribed before)

            subscribe(endpoints=["TRADE"], symbols=["BTCUSD"])  # subscribe to all supported endpoints for "BTCUSD"
            unsubscribe()  # unsubscribe all "TRADE" channels
            subscribe()  # subscribe to all "TRADE" channels back because it was directly
            unsubscribe(endpoints=["TRADE"])  # unsubscribe all "TRADE" channels directly (currently only for "BTCUSD")
            subscribe()  # subscribe all supported channels for symbol "BTCUSD" (as this symbol wasn't unsubscribed directly)
            unsubscribe(symbols=["BTCUSD"])  # unsubscribe all channels for "BTCUSD"

        :param endpoints: endpoints or symbols_by_endpoints (if the latter, symbols param is ignored)
        :param symbols:
        :return:
        """
        symbols = self.convert_symbols_to_platform_format(symbols)
        self.logger.debug(
            "Subscribe on endpoints: %s and symbols: %s prev: %s", endpoints,
            symbols, self.symbols_by_endpoint)
        endpoints, symbols, subscriptions = self.get_adding_subscriptions_for(
            endpoints, symbols, **params)
        self.converter._store_subscription(endpoints, symbols, **params)

        # Get subscriptions
        # endpoints = set(endpoints) if endpoints else set()
        # symbols = set(symbols) if symbols else set()
        # self.endpoints = self.endpoints.union(endpoints) if self.endpoints else endpoints
        # self.symbols = self.symbols.union(symbols) if self.symbols else set(symbols)

        # (To prevent multiple subscriptions)
        if not self.IS_SUBSCRIPTION_COMMAND_SUPPORTED:
            subscriptions.difference_update(self.current_subscriptions)

        # Apply changes
        # todo complex tests
        for endpoint in endpoints:
            if symbols:
                self.symbols_by_endpoint[endpoint].update(set(symbols))

        self.current_subscriptions.update(subscriptions)

        # Subscribe
        if subscriptions:
            self._subscribe(subscriptions)

        # Always is a set, not None
        return subscriptions

    def convert_symbols_to_platform_format(self, symbols):
        return symbols

    def unsubscribe(self, endpoints=None, symbols=None, **params):
        # None means "all"

        if endpoints and not symbols or not endpoints and symbols:
            self.logger.error(
                "Wrong parameters given for unsubscribe! Remove all given "
                "arguments or specify both endpoint s and symbols")

        self.logger.debug(
            "Unsubscribe from endpoints: %s for symbols: %s and params: %s",
            endpoints, symbols, params)
        # Get subscriptions
        endpoints, symbols, subscriptions = self.get_removing_subscriptions_for(
            endpoints, symbols, **params)

        # Apply changes
        # endpoints = set(endpoints) if endpoints else set()
        # symbols = set(symbols) if symbols else set()
        # self.endpoints = self.endpoints.difference(endpoints) if self.endpoints else set()
        # self.symbols = self.symbols.difference(symbols) if self.symbols else set()
        # todo complex tests
        _symbols = {}
        for endpoint in endpoints:
            if symbols:
                self.symbols_by_endpoint[endpoint].difference_update(
                    set(symbols))

        # Unsubscribe
        subscribed = self.pending_subscriptions.union(self.successful_subscriptions or set()) \
            if self.pending_subscriptions else set()
        if not subscribed:
            subscribed = set(self.current_subscriptions)

        self.current_subscriptions.difference_update(subscriptions)
        self.failed_subscriptions.difference_update(subscriptions)
        self.pending_subscriptions.difference_update(subscriptions)
        self.successful_subscriptions.difference_update(subscriptions)

        self._unsubscribe(subscriptions.intersection(subscribed))

        # Always is a set, not None
        return subscriptions

    def resubscribe(self):
        self.logger.debug("Resubscribe all current subscriptions")
        # Unsubscribe & subscribe all
        if self.IS_SUBSCRIPTION_COMMAND_SUPPORTED:
            # Send unsubscribe all and subscribe all back again not interrupting a connection
            self.unsubscribe()
            self.subscribe()
        else:
            # Platforms which subscribe in WS URL need reconnection
            self.reconnect()

    def _subscribe(self, subscriptions):
        # Call subscribe command with "subscriptions" param or reconnect with
        # "self.current_subscriptions" in URL - depending on platform
        self.logger.debug(" Subscribe to subscriptions: %s", subscriptions)
        # if not self.is_started or not self.IS_SUBSCRIPTION_COMMAND_SUPPORTED:
        #     # Connect on first subscribe() or reconnect on the further ones
        #     self.reconnect()
        # else:
        # # if self.IS_SUBSCRIPTION_COMMAND_SUPPORTED:
        #     self._send_subscribe(subscriptions)
        if not self.is_started:
            self.reconnect()
            # if not self.is_subscribed_with_url:
            #     self._send_subscribe(subscriptions)
        else:
            if not self.IS_SUBSCRIPTION_COMMAND_SUPPORTED:
                self.reconnect()
            else:
                to_subscribe = subscriptions.difference(self.pending_subscriptions)
                self.pending_subscriptions.update(subscriptions)
                self._send_subscribe(to_subscribe)
        time.sleep(0)

    def _unsubscribe(self, subscriptions):
        # Call unsubscribe command with "subscriptions" param or reconnect with
        # "self.current_subscriptions" in URL - depending on platform
        self.logger.debug(" Unsubscribe from subscriptions: %s", subscriptions)
        if not self.is_started or not self.IS_SUBSCRIPTION_COMMAND_SUPPORTED:
            self.reconnect()
        else:
            # if self.IS_SUBSCRIPTION_COMMAND_SUPPORTED:
            self._send_unsubscribe(subscriptions)

    def _send_subscribe(self, subscriptions):
        # Implement in subclass
        pass

    def _send_unsubscribe(self, subscriptions):
        # Implement in subclass
        pass

    # Connection

    def connect(self, version=None):
        # Check ready
        if not self.current_subscriptions:
            self.logger.warning("Please subscribe before connect.")
            return

        # Do nothing if was called before
        if self.ws and self.is_started:
            self.logger.warning("WebSocket is already started.")
            return

        if not self.url:
            self.logger.warning("Cannot start WebSocket with empty url: %s" %
                                self.url)
            return

        # Connect
        self.is_started = True
        # while self.is_started:
        if not self.ws:
            self.ws = WebSocketApp(self.url,
                                   header=self.headers,
                                   on_open=self._on_open,
                                   on_message=self._on_message,
                                   on_error=self._on_error,
                                   on_close=self._on_close)
        else:
            self.ws.url = self.url
            self.ws.header = self.headers

        # (run_forever() will raise an exception if previous socket is still not closed)
        self.logger.info("Start WebSocket with url: %s" % self.ws.url)
        kwargs = {"ping_interval": 20}
        kwargs_proxy = {}
        if hasattr(self, "get_next_proxy"):
            self.proxy = self.get_next_proxy(self.__class__.__name__)
            assert self.proxy is not None
            """
                https://github.com/websocket-client/websocket-client/blob/master/websocket/_core.py#L207
                 "http_proxy_host" - http proxy host name.
                 "http_proxy_port" - http proxy port. If not set, set to 80.
                 "http_proxy_auth" - http proxy auth information.
                                     tuple of username and password.
                                     default is None
            """
            kwargs_proxy = {
                "http_proxy_host": self.proxy.address,
                "http_proxy_port": self.proxy.port,
                "http_proxy_auth": (self.proxy.username, self.proxy.password)
            }
        self.thread = Thread(target=self.ws.run_forever,
                             kwargs={
                                 **kwargs,
                                 **kwargs_proxy
                             },
                             daemon=True)
        self.thread.start()
        # if not self.ping_thread:
        #     self.ping_thread = Thread(target=self._process_ping, daemon=True)
        #     self.ping_thread.start()
        # self.thread.join()
        # self.ws = None

    def throttle(self):
        minute = datetime.now().minute
        if self.throttle_counter[0] == minute:
            self.throttle_counter[1] += 1
        else:
            self.throttle_counter[0] = minute
            self.throttle_counter[1] = 0
        if self.throttle_counter[1]:
            attempts = min(self.throttle_counter[1],
                           self.THROTTLE_MAX_DELAY - 1)
            delay = math.log10(attempts)
            # delay = self.THROTTLE_MAX_DELAY / (self.THROTTLE_MAX_DELAY -
            #                                    attempts)
            self.logger.debug("ws throttle: sleeping for %s seconds", delay)
            time.sleep(delay)

    def reconnect(self):
        self.ws and self.logger.info("Reconnecting WebSocket: %s",
                                     self.ws.sock)
        self.close()
        self.throttle()
        # time.sleep(1)
        self.connect()

    def close(self):
        if not self.is_started:
            # Nothing to close
            return
        # print("\n\n")
        self.logger.info(
            "Close WebSocket. is_connected: %s is_connecting: %s thread: %s %s",
            self.is_connected, self.is_connecting, self.platform_id,
            threading.current_thread())
        # self.is_started = False  # - makes self.is_connecting==False
        # (If called directly or from _on_close())
        if self.ws:  # or self.is_connecting:
            # (If called directly)
            # self.logger.debug(" Close WebSocket is_connected: %s is_connecting: %s keep_running: %s",
            #                  self.is_connected, self.is_connecting, self.ws.keep_running)
            self.is_started = False
            self.ws.close()
        self.is_started = False
        # self.logger.debug("Closed WebSocket is_connected: %s is_connecting: %s keep_running: %s",
        #                   self.is_connected, self.is_connecting, self.ws.keep_running)
        del self.ws
        self.ws = None

        super().close()

    def _on_open(self):
        self.logger.info(
            "On open. %s is_started:%s connected:%s keep-running:%s thread: %s %s %s",
            "Connected."
            if self.is_connected else "NOT CONNECTED. It's impossible!",
            self.is_started, self.ws.sock.connected if self.ws else None,
            self.ws.keep_running if self.ws else None, self.platform_id,
            threading.current_thread(), self)
        if not self.is_started:
            self.close()
            return

        # (Stop reconnecting)
        self._is_reconnecting = False
        self._reconnect_tries = 0

        if self.connecting_message_queue:
            self.logger.info("Send all %s messages from queue.",
                             len(self.connecting_message_queue))
            while self.connecting_message_queue:
                self._send(self.connecting_message_queue.pop(0))

        if self.on_connect:
            self.on_connect()

        # Subscribe by command on connect
        if self.IS_SUBSCRIPTION_COMMAND_SUPPORTED and not self.is_subscribed_with_url:
            self.subscribe()

    def _preprocess_message(self, message):
        self.logger.debug("On message: %s thread: %s %s",
                          make_short_str(message, 200), self.platform_id,
                          threading.current_thread())
        # str -> json
        try:
            return json.loads(message, parse_float=Decimal)
        except json.JSONDecodeError:
            self.logger.error("Wrong JSON is received! Skipped. message: %s",
                              message)

    def _on_message(self, message):
        data = self._preprocess_message(message)
        if data is None:
            return
        # json -> items
        result = self._parse(None, data)
        # Process items
        self._data_buffer = []

        if result and isinstance(result, list):
            for item in result:
                self.on_item_received(item)
        else:
            self.on_item_received(result)
            # if result and isinstance(result, (list, tuple)):
            #     for item in result:
            #         # (For different endpoints which have same platform_endpoint)
            #         if item and isinstance(item, list):
            #             items = item
            #             for item in items:
            #                 self.on_item_received(item)
            #         else:
            #             self.on_item_received(item)
            # else:
            #     self.on_item_received(result)

        if self.on_data and self._data_buffer:
            self.logger.debug("Send data out of client - on_data: %s ",
                              items_to_interval_string(self._data_buffer))
            sig = signature(self.on_data)
            if len(sig.parameters) == 1:
                # on_data(items)
                self.on_data(self._data_buffer)
            else:
                # on_data(ws_client, items)
                self.on_data(self, self._data_buffer)

        self._prev_ping_timestamp = time.time()
        self._is_ping = False

    def _parse(self, endpoint, data):
        # (Get subscribing params)
        # -self.logger.debug("###parse endpoint: %s data: %s data_by_subscr: %s",
        #                   endpoint, data, self.converter.endpoint_symbol_params_by_subscription)
        subscription, endpoint, symbol, params = self.converter.get_subscription_info(
            endpoint, data)
        # -self.logger.debug("   ###parse subscription: %s endpoint: %s symbol: %s params: %s",
        #                   subscription, endpoint, symbol, params)
        data = self.converter.preprocess_data(data, subscription, endpoint,
                                              symbol, params)
        # Parse
        result = self.converter.parse(endpoint, data)
        # (Set some params by subscribing params)
        result = self.converter.post_process_result(result, None, endpoint,
                                                    params)
        # (Set subscription param)

        self.converter.propagate_param_to_result(
            "subscription", {"subscription": subscription}, result)
        return result

    def on_item_received(self, item):
        # To skip empty and unparsed data
        if isinstance(item, DataObject):
            if self.on_data_item:
                sig = signature(self.on_data_item)
                if len(sig.parameters) == 1:
                    # on_data_item(item)
                    self.on_data_item(item)
                else:
                    # on_data_item(ws_client, item)
                    self.on_data_item(self, item)
            #     self.on_data_item(item)
            # # if self.on_data_item_with_subscription:
            # #     subscription = None  # generate subscription again
            # #     self.on_data_item_with_subscription(item, subscription)
            self._data_buffer.append(item)
        else:
            if item:
                if self.platform_id == Platform.BITFINEX:
                    # todo fix None for bitfinex
                    self.logger.debug("Unparsed data: %s", item)
                else:
                    self.logger.warning("Unparsed data: %s", item)

    def _on_error(self, error_exc):
        self.logger.info(
            "On error. Note: Ignore errors if websocket was closed while trying to connect. "
            "The problem is in websockets library. thread: %s %s",
            self.platform_id, threading.current_thread())
        self.logger.exception("On error exception from websockets: %s",
                              error_exc)

    def _on_close(self):
        self.logger.info(
            "On WebSocket close (disconnect) is_started: %s thread: %s %s",
            self.is_started, self.platform_id, threading.current_thread())

        if self.on_disconnect:
            self.on_disconnect()

        if self.IS_SUBSCRIPTION_COMMAND_SUPPORTED and not self.is_subscribed_with_url:
            self.pending_subscriptions.clear()

        # Note: is_started is always True, so now reconnect_count doesn't make effect
        if self.is_started or (self._is_reconnecting and
                                self._reconnect_tries < self.reconnect_count):
            # NOTE: reconnecting on close doesn't work due to bugs in websocket library
            # ("self.sock = None" after "self._callback(self.on_close, *close_args)" where on_close runs reconnect()
            # which fails because sock is not empty and sock.connected is still True
            # when is disconnected and should be False)
            # (Try to fix that by setting sock=None by ourselves)
            self._finalize_connection()  # that should fix

            self._is_reconnecting = True
            self.logger.info(
                "Reconnecting... is_started: %s delay_s: %s. tries: %s of %s",
                self.is_started, self.reconnect_delay_sec,
                self._reconnect_tries + 1, self.reconnect_count)
            if self._reconnect_tries != 0 and self.reconnect_delay_sec > 0:
                self.reconnect_delay_sec = 2**(min(self._reconnect_tries, 8))
                # Don't wait before the first reconnection try
                self.logger.info("Wait %s seconds before reconnect.",
                                 self.reconnect_delay_sec)
                time.sleep(self.reconnect_delay_sec)
            self._reconnect_tries += 1
            self.reconnect()
            return
        self._is_reconnecting = False
        # self.logger.warning(
        #     "No more reconnect tries available - close client. tries: %s of %s",
        #     self._reconnect_tries + 1, self.reconnect_count)

        self.close()

    def _finalize_connection(self):
        if self.ws and getattr(self.ws, 'sock', None):
            self.ws.sock = None

    def _send(self, data):
        if not data:
            return

        message = data if isinstance(data, str) else json.dumps(data)
        if self.is_connecting:
            if self.connecting_message_queue is None:
                self.connecting_message_queue = []
            self.connecting_message_queue.append(message)
            self.logger.debug(
                "Add message: %s to queue (len: %s) while client is only connecting.",
                message, len(self.connecting_message_queue))
        elif self.is_connected:
            self.logger.debug("Send message: %s", message)
            self.ws.send(message)
        else:
            self.logger.warning("Disconnected, skip message: %s", message)

    # is_reconnect = False
    def _process_ping(self):
        while self.is_started and self._send_ping and self.ping_interval_sec:
            if not self._prev_ping_timestamp or time.time(
            ) - self._prev_ping_timestamp >= self.ping_interval_sec:
                # if self._is_ping:
                #     self.logger.warning("Didn't receive pong after ping during %s seconds. Seems as disconnected. "
                #                         "To be reconnected.")
                #     # close??
                #     # self._on_close()
                #     return
                self._is_ping = True
                self._send_ping()
                self._prev_ping_timestamp = time.time()

            time.sleep(self.ping_interval_sec)

    def get_endpoint_symbol_params_by_subscription(self, subscription):
        return self.converter.endpoint_symbol_params_by_subscription.get(
            subscription)

    def _send_ping(self):
        pass

    # Processing


class SignalRConnection(Connection):
    def __init__(self, url, session):
        super().__init__(url, session)
        self.started_ev = EventHook()
        self.closed_ev = EventHook()

    def close(self):
        super().close()
        self.closed_ev.fire()

    # TODO: fork signalr-python-threads and apply this fixes there because this looks kinda ugly
    def start(self):
        self.starting.fire()

        transport = getattr(self, '_Connection__transport')
        negotiate_data = transport.negotiate()
        self.token = negotiate_data['ConnectionToken']

        listener = transport.start()

        def wrapped_listener():
            while self.is_open:
                try:
                    listener()
                except:
                    self.exception.fire(*sys.exc_info())
                    self.is_open = False

        self.is_open = True
        listener_thread = Thread(target=wrapped_listener, daemon=True)
        setattr(self, '_Connection__listener_thread', listener_thread)
        listener_thread.start()
        self.started = True
        self.started_ev.fire()


class SignalRClient(WSClient):
    hub = None
    connection = None

    @property
    def is_connected(self):
        return self.connection and self.connection.started

    def connect(self, version=None):
        if not self.current_subscriptions:
            self.logger.warning("Please subscribe before connect.")
            return

        if self.connection and self.is_started:
            self.logger.warning("SignalR is already started.")
            return

        self.is_started = True

        if not self.connection:
            self.connection = SignalRConnection(self.converter.base_url,
                                                Session())
            self.hub = self.connection.register_hub(self.converter.hub_name)

            self.connection.error += self._on_error
            self.connection.started_ev += self._on_open
            self.connection.closed_ev += self._on_close

        self.logger.info("Start SignalR with url: %s" %
                         self.converter.base_url)
        self.connection.start()

    def close(self):
        if not self.is_started:
            # Nothing to close
            return

        self.logger.info("Close SignalR connection")
        # (If called directly or from _on_close())
        self.is_started = False
        if self.is_connected:
            # (If called directly)
            self.connection.close()
        self.connection = None
        self.hub = None

    def _send_subscribe(self, subscriptions):
        for subscription in subscriptions:
            sub_params = subscription.split(",")
            self._send(*sub_params)

    def _send(self, method, data=None):
        self.logger.debug("Send message: method - %s, data - %s", method, data)
        args = [method]
        if data:
            args.append(data)
        self.hub.server.invoke(*args)

    def _on_event_subsription(self, method):
        event = self.converter.event_by_method_lookup.get(method)
        if event:
            self.hub.client.on(event,
                               lambda *args: self._on_received(event, *args))

    def _on_received(self, event, *args, **kwargs):
        if len(args) > 0:
            for message in args:
                super()._on_message(self.__get_deflated_message(
                    message, event))

    def __get_deflated_message(self, message, event_type):
        deflated_msg = decompress(b64decode(message), -MAX_WBITS)
        deflated_msg = deflated_msg.decode('utf-8')
        deflated_msg = deflated_msg[:len(deflated_msg) - 1] + \
                       f",\"e\":\"{event_type}\"" + \
                       deflated_msg[len(deflated_msg) - 1:]
        return deflated_msg
