import re
from collections import defaultdict
from collections.abc import Iterable, Mapping
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from functools import lru_cache

from dateutil.relativedelta import relativedelta

from hyperquant.utils import time_util
from hyperquant.utils.time_util import get_timestamp_s

"""
Common out API format is defined here.
When we calling any other platform API like Binance or Bitfinex we convert 
all response data to this format. 
When anyone calling our REST API this format is used too.
"""

# Trading platforms, REST API, and DB:

# Constants


class Platform:
    BINANCE = 1
    BITFINEX = 2
    BITMEX = 3
    HITBTC = 4
    HUOBI = 5
    OKEX = 6
    BITTREX = 7
    BILAXY = 8
    COINSUPER = 9

    MDS = 100

    internal_name_by_id = {
        BINANCE: "Binance",
        BITFINEX: "Bitfinex",
        BITMEX: "BitMEX",
        HITBTC: "HitBTC",
        HUOBI: "Huobi",
        OKEX: "OKEx",
        BITTREX: "Bittrex",
        BILAXY: "Bilaxy",
        COINSUPER: "Coinsuper",
        MDS: "MDS",
    }
    internal_id_by_name = {v: k for k, v in internal_name_by_id.items()}
    _internal_id_by_name = {
        v.upper(): k
        for k, v in internal_name_by_id.items()
    }

    name_by_id = {k: v for k, v in internal_name_by_id.items() if v != "MDS"}
    id_by_name = {v: k for k, v in internal_name_by_id.items() if v != "MDS"}

    choices = tuple(sorted(name_by_id.items()))

    @classmethod
    def get_platform_name_by_id(cls, platform_id):
        return cls.internal_name_by_id.get(platform_id)

    @classmethod
    def get_platform_id_by_name(cls, platform, is_check_valid_id=False):
        # platform - name or id, all other values will be converted to None
        if isinstance(platform, str) and platform.isnumeric():
            platform = int(platform)
        return cls._internal_id_by_name.get(
            str(platform).upper(), platform
            if not is_check_valid_id or platform in cls.name_by_id else None)

    @classmethod
    def convert_to_platform_ids(cls, platforms):
        if isinstance(platforms, str):
            platforms = platforms.split(",")
        if isinstance(platforms, int):
            platforms = [platforms]
        return [Platform.get_platform_id_by_name(p)
                for p in platforms] if platforms else []


class PlatformCredentials:
    ACCESS_KEY = 'access_key'
    SECRET_KEY = 'secret_key'
    PASSPHRASE = 'passphrase'

    name_by_id = {
        ACCESS_KEY: 'Access key',
        SECRET_KEY: 'Secret key',
        PASSPHRASE: 'Passphrase',
    }

    common = (ACCESS_KEY, SECRET_KEY)
    common_with_passphrase = (ACCESS_KEY, SECRET_KEY, PASSPHRASE)

    template_by_id = {
        Platform.BINANCE: common,
        Platform.BITFINEX: common,
        Platform.BITMEX: common,
        Platform.HITBTC: common,
        Platform.HUOBI: common,
        Platform.OKEX: common_with_passphrase,
        Platform.BITTREX: common,
        Platform.BILAXY: common,
        Platform.COINSUPER: common,
    }

    @classmethod
    def check_templates(cls):
        platforms_with_unset_templates = set(Platform.name_by_id.keys()) - set(
            cls.template_by_id.keys())
        if platforms_with_unset_templates:
            required_templates = ','.join(
                str((platform_id, Platform.name_by_id[platform_id]))
                for platform_id in platforms_with_unset_templates)
            raise NotImplementedError(
                f'PlatformCredentials templates must be set for platforms: [{required_templates}]'
            )

    @classmethod
    def to_common_with_passphrase(cls, credentials) -> tuple:
        """Helper method to convert credentials iterable to the format of `Client` class."""
        if isinstance(credentials, Mapping):
            return tuple(
                credentials.get(key) for key in cls.common_with_passphrase)
        elif isinstance(credentials,
                        Iterable) and not isinstance(credentials, str):
            result = list(credentials)
            max_len = len(cls.common_with_passphrase)
            # Pad with None's up to needed length
            if len(result) < max_len:
                result += [None] * (max_len - len(result))
            return tuple(result)
        raise TypeError(
            f"Type '{credentials.__class__.__name__}' of 'credentials' is not supported"
        )

    @classmethod
    def get_name_by_id(cls, credential):
        return cls.name_by_id.get(credential)

    @classmethod
    def get_template_by_id(cls, platform_id):
        return cls.template_by_id.get(platform_id, ())


PlatformCredentials.check_templates()


class Market:
    # Process only markets defined by lookups (which could be updated from JSON on app run)
    # For quote markets use another class which uses rest_client to get all symbols and all quote symbols from them.

    BTC = "BTC"
    XBT = "XBT"  # For BitMEX
    USD = "USD"
    BNB = "BNB"
    EOS = "EOS"
    ETH = "ETH"
    TRX = "TRX"

    # Add more market names here...
    ALTERNATIVES = "ALTS"  # All except other markets
    ALL = "ALL"  # All - only one market

    IS_MARKET_CURRENCY_AT_END_ONLY = True

    # markets_by_platform_id = {
    #     "default": [BTC, USD, ALTERNATIVES],
    #     Platform.BINANCE: [BTC, USD, BNB, ALTERNATIVES]
    # }
    # todo
    markets_by_platform_id = {
        # "default": [BTC, USD, ALTERNATIVES],
        "default": [ALL],
        Platform.BINANCE: [BTC, USD, BNB, ETH, TRX, ALTERNATIVES],
        # Platform.BITMEX: [ALL],  # No markets
    }
    regexp_by_market_by_platform_id = defaultdict(dict)
    # regexp_by_market_by_platform_id = {
    #     Platform.BINANCE: {
    #         "USD": "USD$|.USD$|USD.$|PAX$",
    #         "ALTS": "ETH$|XRP$",
    #     }
    # }
    currency_regexps_by_market_by_platform_id = {
        "default": {
            USD: ["USD", ".USD", "USD.", "PAX"],
            ALL: [".+"],
        },
        # Platform.BINANCE: {
        #     USD: ["USD", ".USD", "USD.", "PAX"],
        # },
    }

    @classmethod
    def clear_cache(cls):
        cls.regexp_by_market_by_platform_id.clear()

    @classmethod
    @lru_cache(maxsize=128)
    def get_markets(cls, platform_id):
        markets = cls.markets_by_platform_id.get(platform_id)
        if markets is None:
            markets = cls.markets_by_platform_id.get("default")
        return markets

    @classmethod
    def get_regexp(cls, platform_id, markets):
        markets = [markets] if isinstance(markets, str) else markets
        regexps = [cls._market_to_regexp(platform_id, m) for m in markets]
        result = "|".join(regexps)
        return f"{result}"

    @classmethod
    def get_regexp_by_market_lookup(cls, platform_id):
        markets = cls.get_markets(platform_id)
        if not markets:
            return {}

        result = {
            market: cls._market_to_regexp(platform_id, market)
            for market in markets
        }
        return result

    @classmethod
    @lru_cache(maxsize=128)
    def check_symbol_in_market(cls, platform_id, market, symbol):
        regexp = cls.get_regexp(platform_id, market)
        if not symbol or not regexp:
            return None
        result = re.search(regexp, symbol, re.IGNORECASE)
        return not result if market == cls.ALTERNATIVES else bool(result)

    @classmethod
    @lru_cache(maxsize=128)
    def get_market_for_symbol(cls, platform_id, symbol):
        markets = cls.get_markets(platform_id)
        for market in markets:
            if cls.check_symbol_in_market(platform_id, market, symbol):
                return market
        return None

    @classmethod
    def _market_to_regexp(cls, platform_id, market):
        regexp_by_market = cls.regexp_by_market_by_platform_id[platform_id]
        # Try to get in cache
        result = regexp_by_market.get(market)
        if result:
            return result

        if market == cls.ALTERNATIVES:
            markets = cls.get_markets(platform_id)
            # all_markets_except_alts = [m for m in markets if m != market]
            except_regexps = [
                cls._market_to_regexp(platform_id, m) for m in markets
                if m != market
            ]
            # NOTE: Matching should be negated for market=ALTERNATIVES
            result = "|".join(except_regexps)
            # (Doesn't work for Python: re.error: look-behind requires fixed-width pattern)
            # result = f"^(.*(?<!{result}))$"  # Invert result regexp
            # todo check solution from Pozhoga f"^(?:(?!USD$).)+$"
        else:
            currency_regexps_by_market = cls.currency_regexps_by_market_by_platform_id.get(
                platform_id,
                cls.currency_regexps_by_market_by_platform_id.get("default"))
            # Get by market or by "default" or use market name itself as the very default
            currency_regexps = currency_regexps_by_market.get(
                market, currency_regexps_by_market.get("default", [market])) \
                if currency_regexps_by_market else [market]
            # Check market currency should only in the end of symbol
            if cls.IS_MARKET_CURRENCY_AT_END_ONLY:
                currency_regexps = [f"{re}$" for re in currency_regexps]
            # Join to single regexp
            result = "|".join(currency_regexps)

        # Set up cache
        if result:
            result = result.upper()
            regexp_by_market[market] = result
        return result

    @classmethod
    def get_symbols_by_market_lookup(cls, platform_id, symbols):
        if symbols is None:
            return None

        result = defaultdict(list)
        for symbol in symbols:
            market = cls.get_market_for_symbol(platform_id, symbol)
            result[market].append(symbol)
        return result


class Endpoint:
    # Note: you can use any value, but remember they will be used in all our APIs,
    # and they must differ from each other

    # ALL = "*"  # Used by WS Client

    # For all platforms and our REST API (except *_HISTORY)
    # Note: All XXX_HISTORY should be always set in endpoint_lookup if XXX is set (XXX is subset of XXX_HISTORY)
    PING = "ping"
    SERVER_TIME = "time"
    SYMBOLS = "symbols"
    SYMBOLS_ACTIVE = "symbols/active"
    CURRENCY_PAIRS = "currency/pair"
    TRADE = "trade"

    TRADE_HISTORY = "trade/history"
    TRADE_MY = "trade/my"  # Private
    TRADE_MY_HISTORY = "trade/my/history/"  # Private
    CANDLE = "candle"
    # CANDLE_HISTORY = "candle/history"
    TICKER = "ticker"
    TICKER_ALL = "ticker_all"
    # TICKER_HISTORY = "ticker/history"
    ORDER_BOOK = "orderbook"
    # ORDER_BOOK_HISTORY = "orderbook/history"
    ORDER_BOOK_DIFF = "orderbook_diff"  # WS
    ORDER_BOOK_AGG = "orderbook_agg"
    # Private
    ACCOUNT = "account"
    BALANCE = "balance"
    BALANCE_WALLET = "balance/wallet"  # for OKEx - to move money between wallets
    BALANCE_TRANSACTION = "balance/transactions"  # For BitMEX (deposits, withdrawals, realisedPNLs)
    BALANCE_CONVERTED = "balance_converted"
    ORDER = "order"
    ORDER_CREATE = "order/create"  # for Huobi
    ORDER_CANCEL = "order/cancel"  # for Huobi
    ORDERS_OPEN = "order/current"
    ORDERS_ALL = "order/all"
    ORDERS_ALL_CANCEL = "order/all/cancel"
    # ORDERS_HISTORY = "order/history"
    ORDER_TEST = "order/test"
    POSITION = "position"
    POSITION_CLOSE = "position/close"
    REPAYMENT = "repayment"
    TRANSFER = "transfer"
    QUOTE = "quote"

    # For our REST API only
    # ITEM = ""
    # HISTORY = "history"
    INFO = "info"
    COUNT = "count"
    # PRICE = "price"  # Use TICKER instead
    FORMAT = "format"
    EXPORT = "export"
    STATUS = "status"

    PLATFORMS = "platforms"
    ERRORS = "errors"
    LEVERAGE_SET = "leverage/set"

    ALL = [
        SERVER_TIME,
        SYMBOLS,
        TRADE,
        TRADE_HISTORY,
        TRADE_MY,
        TRADE_MY_HISTORY,
        CANDLE,
        TICKER,
        TICKER_ALL,
        ORDER_BOOK,
        ORDER_BOOK_DIFF,
        ORDER_BOOK_AGG,
        ACCOUNT,
        BALANCE,
        BALANCE_CONVERTED,
        SYMBOLS_ACTIVE,
        ORDER,
        ORDER_CREATE,
        ORDER_CANCEL,
        ORDERS_OPEN,
        ORDERS_ALL,
        ORDERS_ALL_CANCEL,
        ORDER_TEST,  # ORDERS_HISTORY,
        POSITION,
        POSITION_CLOSE,
        QUOTE,
        FORMAT,  # ITEM, HISTORY, INFO
        LEVERAGE_SET,
    ]

    private_endpoints = [
        BALANCE, POSITION, ORDER, TRADE_MY, ACCOUNT, BALANCE_WALLET, ORDER,
        ORDER_CREATE, ORDER_CANCEL, ORDERS_OPEN, ORDERS_ALL, ORDERS_ALL_CANCEL,
        ORDER_TEST, POSITION_CLOSE, REPAYMENT, TRANSFER, LEVERAGE_SET
    ]

    @classmethod
    def convert_to_endpoints(cls, endpoints):
        # Always return list
        if isinstance(endpoints, str):
            endpoints = endpoints.split(",")
        return [e.lower() for e in endpoints
                if e.lower() in cls.ALL] if endpoints else []

    @classmethod
    def check_is_private(cls, endpoint):
        return endpoint in cls.private_endpoints


class Symbol:
    # todo "~"
    DELIMITER = "_"
    name = ""


class Currency(Symbol):
    # Deprecated
    # todo don't use, remove or rename to convert_symbol_or_symbols

    @classmethod
    def convert_symbols(cls, symbols):
        # Returns list, str or any empty
        # empty -> empty
        if not symbols:
            return symbols
        # str -> str
        if isinstance(symbols, str):
            if "," in symbols:
                symbols = symbols.split(",")
            return symbols.upper().replace("-", Currency.DELIMITER).replace(
                "/", Currency.DELIMITER)
        # list -> list
        return [
            s.upper().replace("-", Currency.DELIMITER).replace(
                "/", Currency.DELIMITER) for s in symbols if s
        ]

    @classmethod
    def convert_to_symbols(cls, symbols):
        # Always return list
        if isinstance(symbols, str):
            symbols = symbols.split(",")
        return cls.convert_symbols(symbols)


class CurrencyPair(Symbol):
    lot_size_min = None
    lot_size_max = None
    lot_size_step = None
    min_notional = None
    price_step = None
    quote = None
    base = None
    # as it comes from exchange
    name_in_platform = None

    def __repr__(self):
        return f"<{self.__class__.__name__}>: {self.get_canonical_name}"

    def to_string(self):
        return self.__str__()

    def __str__(self):
        return f"{self.base}{self.quote}"

    @property
    def get_canonical_name(self):
        return self.base and self.quote and "{}:{}{}".format(
            self.name_in_platform, self.base.upper(),
            self.quote.upper() or "")

    @property
    def lot_min(self):
        return self.lot_size_min if self.lot_size_min else self.lot_size_step

    @property
    def lot_max(self):
        return self.lot_size_max

    @property
    def lot_step(self):
        return self.lot_size_step


class ParamName:
    # Stores names which are used:
    # 1. in params of client.send() method;
    # 2. in value object classes!;
    # 3. field names in DB;
    # 4. in our REST APIs.

    ID = "id"
    ITEM_ID = "item_id"
    TRADE_ID = "trade_id"
    # Note: ORDER_ID is only used in MyTrade, not in Order!
    ORDER_ID = "order_id"
    USER_ORDER_ID = "user_order_id"

    SYMBOL = "symbol"
    SYMBOLS = "symbols"  # For our REST API only
    LIMIT = "limit"
    IS_USE_MAX_LIMIT = "is_use_max_limit"  # used in clients only
    LIMIT_SKIP = "limit_skip"
    PAGE = "page"  # instead of LIMIT_SKIP
    SORTING = "sorting"
    INTERVAL = "interval"
    INTERVALS = "intervals"  # For our REST API only
    DIRECTION = "direction"  # Sell/buy or ask/bid
    ORDER_TYPE = "order_type"
    ORDER_STATUS = "order_status"
    LOADING_STATUS = "loading_status"
    LEVEL = "level"  # For order book (WS)
    TRADES_COUNT = "trades_count"
    ORDERS_COUNT = "orders_count"
    TIMESTAMP = "timestamp"
    TIMESTAMP_CLOSE = "timestamp_close"  # For candles

    FROM_ITEM = "from_item"
    TO_ITEM = "to_item"
    FROM_TIME = "from_time"
    TO_TIME = "to_time"
    FROM_AMOUNT = "from_amount"
    TO_AMOUNT = "to_amount"
    FROM_PRICE = "from_price"
    TO_PRICE = "to_price"
    FROM_TRANSFER = "from_transfer"
    TO_TRANSFER = "to_transfer"

    AMOUNT_ORIGINAL = "amount_original"
    AMOUNT_EXECUTED = "amount_executed"
    AMOUNT_AVAILABLE = "amount_available"
    AMOUNT_RESERVED = "amount_reserved"
    AMOUNT_BORROWED = "amount_borrowed"
    MARGIN_BALANCE = 'margin_balance'
    AMOUNT = "amount"
    VOLUME = "volume"
    PRICE_OPEN = "price_open"
    PRICE_CLOSE = "price_close"
    PRICE_HIGH = "price_high"
    PRICE_LOW = "price_low"
    PRICE = "price"
    PRICE_STOP = "price_stop"
    PRICE_AVERAGE = "price_average"
    PRICE_MARGIN_CALL = "margincall_price"
    PRICE_LIMIT = "price_limit"

    TIME_IN_FORCE = "timeInForce"

    # Always in quote asset (YYY in XXXYYY). If not for some platform -
    # convert it using price in trade in converter while creating VO
    PNL = "pnl"
    MIN_NOTIONAL = "min_notional"
    FEE = "fee"
    REBATE = "rebate"
    BALANCES = "balances"
    ASKS = "asks"
    BIDS = "bids"
    BESTASK = "bestask"
    BESTBID = "bestbid"
    ITEMS = "items"
    ACCOUNT_TYPE = "account_type"
    TRANSACTION_TYPE = "transaction_type"

    # For our REST API only
    PLATFORM_ID = "platform_id"
    PLATFORM_IDS = "platform_ids"
    PLATFORM = "platform"  # (alternative)
    PLATFORMS = "platforms"  # (alternative)
    # PLATFORM_NAME = "platform_name"  # (completely different from PLATFORM_ID)
    ENDPOINT = "endpoint"
    # ENDPOINTS = "endpoints"
    ACTION = "action"  # for websocket api
    MARKET = "market"
    MARKETS = "markets"
    TIMEZONE = "timezone"
    LOCALE = "locale"
    INDICATOR = "indicator"  # for analytics
    DATETIME_FORMAT = "datetime_format"
    DATE_FORMAT = "date_format"
    TIME_FORMAT = "time_format"
    FIELD_SEPARATOR = "field_separator"
    FRACTIONAL_SEPARATOR = "fractional_separator"
    FILE_FORMAT = "file_format"

    IS_OPEN = "is_open"
    IS_SHORT = "is_short"
    IS_HQ_FORMAT = "is_hq_format"

    AVAILABLE = "available"  # ??? AMOUNT_AVAILABLE?
    RESERVED = "reserved"  # ??? AMOUNT_RESERVED?

    AGGREGATION_TYPE = "aggregation_type"

    # Symbol related
    LOT_SIZE_MIN = "lot_size_min"
    LOT_SIZE_MAX = "lot_size_max"
    LOT_SIZE_STEP = "lot_size_step"
    PRICE_STEP = "price_step"
    SYMBOL_QUOTE = "quote"
    SYMBOL_BASE = "base"
    PLATFORM_SYMBOL_NAME = "name_in_platform"
    PROFIT_N_LOSS = "profit_n_loss"
    LEVERAGE = "leverage"

    # For candles
    UPDATE_ON_TICK = "update_on_tick"

    ALL = [
        ID, ITEM_ID, TRADE_ID, ORDER_ID, USER_ORDER_ID, SYMBOL, SYMBOLS, LIMIT,
        DIRECTION, IS_USE_MAX_LIMIT, LIMIT_SKIP, PAGE, SORTING, INTERVAL,
        DIRECTION, ORDER_TYPE, ORDER_STATUS, LEVEL, TRADES_COUNT, ORDERS_COUNT,
        TIMESTAMP, FROM_ITEM, TO_ITEM, FROM_TIME, TO_TIME, FROM_AMOUNT,
        TO_AMOUNT, FROM_PRICE, TO_PRICE, FROM_TRANSFER, TO_TRANSFER,
        AMOUNT_ORIGINAL, AMOUNT_EXECUTED, AMOUNT_AVAILABLE, AMOUNT_RESERVED,
        AMOUNT_BORROWED, AMOUNT, VOLUME, PRICE_OPEN, PRICE_CLOSE, PRICE_HIGH,
        PRICE_LOW, PRICE, PRICE_STOP, FEE, REBATE, PNL, BALANCES, BIDS, ASKS,
        BESTBID, BESTASK, ITEMS, PLATFORM_ID, PLATFORM_IDS, PLATFORM,
        PLATFORMS, ENDPOINT, MARKET, MARKETS, TIMEZONE, DATETIME_FORMAT,
        DATE_FORMAT, FIELD_SEPARATOR, FRACTIONAL_SEPARATOR, FILE_FORMAT,
        LOCALE, IS_OPEN, IS_SHORT, IS_HQ_FORMAT, AVAILABLE, RESERVED,
        AGGREGATION_TYPE, LOT_SIZE_MIN, LOT_SIZE_MAX, LOT_SIZE_STEP,
        SYMBOL_QUOTE, SYMBOL_BASE, PLATFORM_SYMBOL_NAME, LOADING_STATUS,
        PRICE_AVERAGE, PRICE_MARGIN_CALL, PROFIT_N_LOSS, PRICE_LIMIT,
        UPDATE_ON_TICK, PRICE_STEP, MIN_NOTIONAL, LEVERAGE
    ]

    _timestamp_names = (TIMESTAMP, TIMESTAMP_CLOSE, FROM_TIME, TO_TIME)
    _decimal_names = (FROM_AMOUNT, TO_AMOUNT, FROM_PRICE, TO_PRICE,
                      FROM_TRANSFER, TO_TRANSFER, AMOUNT_ORIGINAL,
                      AMOUNT_EXECUTED, AMOUNT_AVAILABLE, AMOUNT_RESERVED,
                      AMOUNT_BORROWED, AMOUNT, VOLUME, PRICE_OPEN, PRICE_CLOSE,
                      PRICE_HIGH, PRICE_LOW, PRICE, PRICE_STOP, FEE, REBATE,
                      AVAILABLE, RESERVED, LOT_SIZE_MIN, LOT_SIZE_MAX,
                      LOT_SIZE_STEP, PRICE_AVERAGE, PRICE_MARGIN_CALL,
                      PROFIT_N_LOSS, PRICE_LIMIT, BESTASK, BESTBID, PRICE_STEP,
                      MIN_NOTIONAL, LEVERAGE)

    @classmethod
    def is_timestamp(cls, name):
        return name in cls._timestamp_names

    @classmethod
    def is_decimal(cls, name):
        return name in cls._decimal_names


class ParamValue:
    # todo remove sometimes
    # param_names = [ParamName.SORTING]

    # For limit
    MIN = "min"
    MAX = "max"

    ALL = "all"
    UNDEFINED = None


class Sorting:
    ASCENDING = "asc"  # Oldest first
    DESCENDING = "desc"  # Newest first, usually default
    DEFAULT_SORTING = "default_sorting"  # (For internal uses only)

    ALL = [ASCENDING, DESCENDING]


class CandleInterval:
    # For candles

    SEC_1 = "1s"  # for debug
    SEC_2 = "2s"  # for debug

    MIN_1 = "1m"
    MIN_3 = "3m"
    MIN_5 = "5m"
    MIN_15 = "15m"
    MIN_30 = "30m"
    HRS_1 = "1h"
    HRS_2 = "2h"
    HRS_4 = "4h"
    HRS_6 = "6h"
    HRS_8 = "8h"
    HRS_12 = "12h"
    DAY_1 = "1d"
    DAY_3 = "3d"
    WEEK_1 = "1w"
    MONTH_1 = "1M"

    ALL = [
        SEC_1,
        SEC_2,  # for debug
        MIN_1,
        MIN_3,
        MIN_5,
        MIN_15,
        MIN_30,
        HRS_1,
        HRS_2,
        HRS_4,
        HRS_6,
        HRS_8,
        HRS_12,
        DAY_1,
        DAY_3,
        WEEK_1,
        MONTH_1
    ]

    BASE_INTERVALS_BY_INTERVAL = {
        SEC_1: [],  # for debug
        SEC_2: [SEC_1],  # for debug
        MIN_1: [],
        MIN_3: [MIN_1],
        MIN_5: [MIN_1],
        MIN_15: [MIN_5, MIN_1],
        MIN_30: [MIN_15, MIN_5, MIN_1],
        HRS_1: [MIN_30, MIN_15, MIN_5, MIN_1],
        HRS_2: [HRS_1],
        HRS_4: [HRS_2, HRS_1],
        HRS_6: [HRS_2, HRS_1],
        HRS_8: [HRS_4, HRS_2, HRS_1],
        HRS_12: [HRS_6, HRS_4, HRS_2, HRS_1],
        DAY_1: [HRS_12, HRS_6, HRS_4, HRS_2, HRS_1],
        DAY_3: [DAY_1],
        WEEK_1: [DAY_1],
        MONTH_1: [DAY_1],
    }

    ENDPOINTS = [Endpoint.CANDLE]

    @classmethod
    def get_all_intervals_for_key(cls, candle_intervals, key):
        if not isinstance(candle_intervals, dict):
            return cls.sorted(candle_intervals)

        intervals = set()
        # Look for "key" on 1 and 2 nesting level
        for k, v in candle_intervals.items():
            if k == key:
                if isinstance(v, dict):
                    # (Merge all lists inside)
                    for subvalues in v.values():
                        intervals.update(set(subvalues))
                elif isinstance(v, list):
                    intervals.update(set(v))
            elif isinstance(v, dict) and key in v:
                intervals.update(cls.get_all_intervals_for_key(v, key))

        if not intervals and key not in candle_intervals:
            return cls.sorted(candle_intervals["default"])

        return cls.sorted(list(intervals))

    @classmethod
    def convert_to_intervals(cls, intervals, is_remove_wrong_values=True):
        # Always return list
        # (callable -> list|str|dict|tuple|set)
        if callable(intervals):
            intervals = intervals()
        # (Empty)
        if not intervals:
            return []

        # Convert (removing duplicates)
        # (str -> list)
        if isinstance(intervals, str):
            result = set(intervals.split(","))
        # (dict -> list)
        elif isinstance(intervals, dict):
            result = set()
            for v in intervals.values():
                result.update(
                    set(v if isinstance(v, (
                        list, tuple)) else cls.convert_to_intervals(v)))
        else:
            result = set(intervals)

        # Prepare result
        # todo remove remove_wrong_values at all (?+)
        # (Remove wrong values) (don't remove to be checked later to generate warnings (for mds candle api))
        if is_remove_wrong_values:
            result = [i for i in result if i in cls.ALL]
        # (Sort)
        return cls.sorted(result)

    # @classmethod
    # def is_week(cls, interval):
    #     return interval and interval.ends_with("w")
    #
    # @classmethod
    # def is_month(cls, interval):
    #     return interval and interval.ends_with("M")

    @classmethod
    def parse_interval(cls, interval, default_suffix="m"):
        # Empty
        if not interval:
            return 0, default_suffix
        # Wrong type
        if not isinstance(interval, str):
            if isinstance(interval, Iterable):
                return interval
            return interval, default_suffix
        # As number
        try:
            return float(interval), default_suffix
        except:
            pass

        number = interval[0:-1] or 0
        # if not number.isnumeric():
        #     return 0, interval
        try:
            number = float(number)
        except ValueError:
            return interval, None
        suffix = interval[-1]
        return number, suffix

    MINUTES_BY_SUFFIX = {
        "s": 1 / 60,
        "m": 1,
        "h": 60,
        "d": 60 * 24,
        "w": 60 * 24 * 7,
        "M": 60 * 24 * 30,
        "Y": 60 * 24 * 365,
    }
    SUFFIX_ORDER = ["s", "m", "h", "d", "w", "M", "Y"]

    @classmethod
    def convert_to_minutes(cls, interval):
        # Approximately, just for sorting

        number, suffix = cls.parse_interval(interval)

        # Parse suffix
        # suffix = interval[-1]
        minutes = cls.MINUTES_BY_SUFFIX.get(suffix)
        if not minutes:
            return interval

        # Parse number
        # number = interval[0:-1]
        # if not number:
        #     return 0
        try:
            number = float(number)
        except:
            return interval

        # Result
        return number * minutes

    @classmethod
    def sorted(cls, candle_intervals):
        if not candle_intervals:
            return candle_intervals
        return sorted(candle_intervals,
                      key=lambda i: cls.convert_to_minutes(i) or -1)

    PROPERTIES_BY_SUFFIX = {
        "s": ["second", "microsecond"],
        "m": ["minute", "second", "microsecond"],
        "h": ["hour", "minute", "second", "microsecond"],
        "d": ["day", "hour", "minute", "second", "microsecond"],
        "w": ["week", "hour", "minute", "second", "microsecond"],
        "M": ["month", "day", "hour", "minute", "second", "microsecond"],
        "Y":
        ["year", "month", "day", "hour", "minute", "second", "microsecond"],
    }
    # month = 1 .. 12
    # day = 1 ..
    # weekday = 0 (Monday) .. 6 (Sunday)
    # hour = 0 .. 23
    # minute = 0 .. 59
    # second = 0 .. 59
    MIN_VALUE_BY_PROPERTY = {
        "microsecond": 0,
        "second": 0,
        "minute": 0,
        "hour": 0,
        "day": 1,
        # "weekday": 0,
        "week": 1,
        "month": 1,
        "year": 1,
    }
    PERIOD_SUFFIX_BY_SUFFIX = {
        # "u": "s",
        # "i": "s",
        "s": "m",
        "m": "h",
        "h": "d",
        "d": "Y",
        "w": "Y",
        "M": "Y",
        "Y": None,
    }

    @classmethod
    # @lru_cache(maxsize=1024, typed=True) Wrong result on tests
    def break_time_period_on_intervals(cls,
                                       from_incl,
                                       to_excl,
                                       candle_interval,
                                       weekday_start=0,
                                       is_return_timestamps=True,
                                       is_narrow=True,
                                       is_milliseconds=False):
        """
        :param from_incl:
        :param to_excl: set same as from_incl and is_narrow=False to get interval around timestamp
        :param candle_interval:
        :param weekday_start:
        :param is_return_timestamps:
        :param is_narrow: True - excluding intervals which contain from and to timestamp in the middle of those intervals,
        :param is_milliseconds:
        False - always including
        :return:
        """
        if not from_incl and not to_excl or not candle_interval:  # or \
            # from_incl == to_excl:
            return []

        if not from_incl:
            from_incl = to_excl
        if not to_excl:
            to_excl = from_incl

        from_datetime = datetime.utcfromtimestamp(
            get_timestamp_s(from_incl)).replace(tzinfo=timezone.utc)
        to_datetime = datetime.utcfromtimestamp(
            get_timestamp_s(to_excl)).replace(tzinfo=timezone.utc)
        if from_datetime > to_datetime:
            from_datetime, to_datetime = to_datetime, from_datetime

        interval_value, interval_suffix = cls.parse_interval(candle_interval)

        from_datetime = cls._get_side_datetime_for_interval(
            from_datetime,
            interval_value,
            interval_suffix,
            weekday_start,
            is_before=not is_narrow)
        to_datetime = cls._get_side_datetime_for_interval(to_datetime,
                                                          interval_value,
                                                          interval_suffix,
                                                          weekday_start,
                                                          is_before=is_narrow)
        if not from_datetime or from_datetime == to_datetime:
            return []

        property = cls.PROPERTIES_BY_SUFFIX.get(interval_suffix)[0]
        interval_delta = relativedelta(**{property + "s": interval_value})

        # Build intervals between from_ and to_datetime
        result = []
        while True:
            # todo reset from_datetime on going through time zero point
            # current_to_datetime = from_datetime + interval_delta
            current_to_datetime = cls._get_side_datetime_for_interval(
                from_datetime + interval_delta,
                interval_value,
                interval_suffix,
                weekday_start,
                is_before=True)
            if current_to_datetime > to_datetime:
                break
            if is_return_timestamps:
                if is_milliseconds:
                    result.append([
                        from_datetime.timestamp() * 1000,
                        current_to_datetime.timestamp() * 1000
                    ])
                else:
                    result.append([
                        from_datetime.timestamp(),
                        current_to_datetime.timestamp()
                    ])
            else:
                result.append([from_datetime, current_to_datetime])
            from_datetime = current_to_datetime

        return result

    @classmethod
    def _reset_datetime_for_interval_suffix(cls, for_datetime,
                                            interval_suffix):
        properties = cls.PROPERTIES_BY_SUFFIX.get(interval_suffix, None)
        property, *zero_properties = properties
        side_datetime = for_datetime.replace(
            **{p: cls.MIN_VALUE_BY_PROPERTY.get(p)
               for p in zero_properties})
        return side_datetime

    @classmethod
    def _get_max_datetime(cls, for_datetime, interval_suffix):
        # max_datetime
        period_suffix = cls.PERIOD_SUFFIX_BY_SUFFIX.get(interval_suffix, None)
        properties = cls.PROPERTIES_BY_SUFFIX.get(period_suffix, None)
        if properties:
            # delta
            period_property, *_ = properties
            delta_property = "days" if period_property == "week" else period_property + "s"
            delta = relativedelta(**{delta_property: 1})
            # max_datetime result
            max_datetime = cls._reset_datetime_for_interval_suffix(
                for_datetime, period_suffix) + delta
            return max_datetime

    @classmethod
    def _get_side_datetime_for_interval(cls,
                                        for_datetime,
                                        interval_value,
                                        interval_suffix,
                                        weekday_start=0,
                                        is_before=False):

        properties = cls.PROPERTIES_BY_SUFFIX.get(interval_suffix, None)
        if not for_datetime or not interval_value or not properties:
            # Wrong interval suffix or lookup
            return None
        property, *zero_properties = properties

        current_value = for_datetime.weekday(
        ) if property == "week" else getattr(for_datetime, property)
        max_datetime = cls._get_max_datetime(for_datetime, interval_suffix)

        # side_datetime to initial start value
        side_datetime = cls._reset_datetime_for_interval_suffix(
            for_datetime, interval_suffix)
        if side_datetime != for_datetime:
            delta_property = "days" if property == "week" else property + "s"
            delta = relativedelta(**{delta_property: 1})
            if not is_before:
                current_value += 1

                side_datetime += delta

        # Consider weekday_start for week intervals
        if property == "week":
            delta_days = weekday_start - current_value
            if delta_days < 0:
                delta_days = 7 + delta_days
            if is_before and delta_days:
                delta_days -= 7
            delta = timedelta(days=delta_days)

            side_datetime += delta

        # Consider interval's starting point for 2x, 3x,... intervals
        if interval_value > 1:
            if property == "day":
                day_of_year = side_datetime.timetuple()[7]  # 1 .. 366
                current_value = day_of_year
            elif property == "week":
                week_of_year = side_datetime.isocalendar()[1]  # 1 .. 53
                current_value = week_of_year

            min_value = cls.MIN_VALUE_BY_PROPERTY.get(property)
            delta_value = (
                interval_value -
                (current_value - min_value) % interval_value) % interval_value
            if is_before and delta_value:
                delta_value -= interval_value
            delta = relativedelta(**{property + "s": delta_value})
            side_datetime += delta
        return min(side_datetime,
                   max_datetime) if max_datetime else side_datetime

    @classmethod
    def get_seconds_from_interval(self, candle_interval):
        interval_size, suffix = CandleInterval.parse_interval(
            candle_interval, 'm')
        return CandleInterval.MINUTES_BY_SUFFIX[suffix] * 60 * interval_size


class Direction:
    # (trade, order)

    SELL = 1
    BUY = 2
    # (for our REST API as alternative values)
    SELL_NAME = "sell"
    BUY_NAME = "buy"

    name_by_value = {
        SELL: SELL_NAME,
        BUY: BUY_NAME,
    }
    value_by_name = {v: k for k, v in name_by_value.items()}

    @classmethod
    def inverse(cls, direction):
        return Direction.SELL if direction == Direction.BUY else Direction.BUY

    @classmethod
    def get_direction_value(cls, direction, is_check_valid_id=True):
        return cls.value_by_name.get(
            str(direction).upper(), direction if not is_check_valid_id
            or direction in cls.name_by_value else None)


class OrderBookDepthLevel:
    # For ParamName.LEVEL

    LIGHT = "light"
    MEDIUM = "medium"
    DEEP = "deep"
    DEEPEST = "deepest"

    ALL = [LIGHT, MEDIUM, DEEP, DEEPEST]

    ENDPOINTS = [Endpoint.ORDER_BOOK, Endpoint.ORDER_BOOK_DIFF]


class OrderBookAggType:
    SIMPLE = "simple"
    ADVANCED = "advanced"
    WEIGHTED_ARITHMETIC = "arithmetic"
    WEIGHTED_GEOMETRIC = "geometric"
    WEIGHTED_HARMONIC = "harmonic"

    ALL = [
        SIMPLE, ADVANCED, WEIGHTED_ARITHMETIC, WEIGHTED_GEOMETRIC,
        WEIGHTED_HARMONIC
    ]
    WEIGHTED = [WEIGHTED_ARITHMETIC, WEIGHTED_GEOMETRIC, WEIGHTED_HARMONIC]


class OrderBookDirection:
    # Direction for order book (same as sell/buy but with different names)
    ASK = 1  # Same as sell
    BID = 2  # Same as buy
    # (for our REST API as alternative values)
    ASK_NAME = "ask"
    BID_NAME = "bid"

    name_by_value = {
        ASK: ASK_NAME,
        BID: BID_NAME,
    }
    value_by_name = {v: k for k, v in name_by_value.items()}


class OrderType:
    LIMIT = 1
    MARKET = 2
    VIRTUAL = 10
    STOP_MARKET = 3
    STOP_LIMIT = 11
    TAKE_PROFIT_LIMIT = 12
    TAKE_PROFIT_MARKET = 13

    # Virtual
    # STOP_MARKET = 11
    # STOP_LOSS = 12

    # (for our REST API)
    LIMIT_NAME = "limit"
    MARKET_NAME = "market"
    VIRTUAL_NAME = "virtual"
    STOP_NAME = "stop_market"
    STOP_LIMIT_NAME = "stop_limit"
    TAKE_PROFIT_LIMIT_NAME = "take_profit_limit"
    TAKE_PROFIT_MARKET_NAME = "take_profit_market"

    # Virtual
    # STOP_MARKET = "stop"
    # STOP_LOSS = "stop_loss"

    name_by_value = {
        LIMIT: LIMIT_NAME,
        MARKET: MARKET_NAME,
        VIRTUAL: VIRTUAL_NAME,
        STOP_MARKET: STOP_NAME,
        STOP_LIMIT: STOP_LIMIT_NAME,
        TAKE_PROFIT_LIMIT: TAKE_PROFIT_LIMIT_NAME,
        TAKE_PROFIT_MARKET: TAKE_PROFIT_MARKET_NAME,
        # Virtual
        # STOP_MARKET: STOP_MARKET,
        # STOP_LOSS: STOP_LOSS,
    }
    value_by_name = {v: k for k, v in name_by_value.items()}

    @classmethod
    def is_virtual(cls, value):
        return value >= 10

    @classmethod
    def is_limit_family(cls, value):
        return (value in [cls.LIMIT, cls.TAKE_PROFIT_LIMIT, cls.STOP_LIMIT]) or \
               (value in [cls.LIMIT_NAME, cls.TAKE_PROFIT_LIMIT_NAME, cls.STOP_LIMIT_NAME])

    @classmethod
    def is_market_family(cls, value):
        return not cls.is_limit_family(value)


class OrderStatus:
    # CLOSED = 0
    OPEN = 1  # When we cannot convert receiving status precisely to NEW or PARTIALLY_FILLED

    NEW = 2
    PARTIALLY_FILLED = 3
    # PENDING_CANCEL = 4
    FILLED = 5
    CANCELED = 6
    REJECTED = 7
    EXPIRED = 8

    # (for our REST API)
    OPEN_NAME = "open"
    CLOSED_NAME = "closed"

    NEW_NAME = "new"
    PARTIALLY_FILLED_NAME = "partially_filled"
    FILLED_NAME = "filled"
    # PENDING_CANCEL_NAME = "pending_cancel"
    CANCELED_NAME = "canceled"
    REJECTED_NAME = "rejected"
    EXPIRED_NAME = "expired"

    open = [NEW, PARTIALLY_FILLED, OPEN]  # , PENDING_CANCEL
    closed = [FILLED, CANCELED, REJECTED, EXPIRED]

    name_by_value = {
        OPEN: OPEN_NAME,
        # CLOSED: CLOSED_NAME,
        NEW: NEW_NAME,
        PARTIALLY_FILLED: PARTIALLY_FILLED_NAME,
        FILLED: FILLED_NAME,
        # PENDING_CANCEL: PENDING_CANCEL_NAME,
        CANCELED: CANCELED_NAME,
        REJECTED: REJECTED_NAME,
        EXPIRED: EXPIRED_NAME,
    }
    value_by_name = {v: k for k, v in name_by_value.items()}


class OrderTimeInForce:
    # https://www.reddit.com/r/BinanceExchange/comments/8odvs4/question_about_time_in_force_binance_api/
    GTC = 1  # GoodTillCancel
    IOC = 2  # ImmediateOrCancel
    FOK = 3  # FillOrKill
    DAY = 4


class ErrorCode:
    # Provides same error codes and messages for all trading platforms

    # Надо накопить достаточно типов ошибок, систематизировать их и дать им числовые коды,
    # которые будет легко мнемонически запомнить, чтобы поотм легко можно было определить ошибку по ее коду
    UNAUTHORIZED = "any1"
    RATE_LIMIT = "any:ratelim"
    IP_BAN = "any:ipban"
    WRONG_SYMBOL = "any:wrsymbol"
    WRONG_LIMIT = "any:wrlimit"
    WRONG_PARAM = "any:wrparval"
    APP_ERROR = "any:apperr"
    APP_DB_ERROR = "any:appdberr"
    MISS_REQ_PARAMS = "any:missreqparam"
    WRONG_URL = "wrurl"

    message_by_code = {
        UNAUTHORIZED:
        "Unauthorized. May be wrong api_key or api_secret or not defined at all.",
        RATE_LIMIT: "Rate limit reached. We must make a delay for a while.",
        WRONG_SYMBOL:
        "Wrong symbol. May be this symbol is not supported by platform or its name is wrong.",
        WRONG_LIMIT: "Wrong limit. May be too big.",
        WRONG_PARAM: "Wrong param value.",
        APP_ERROR: "App error!",
        APP_DB_ERROR:
        "App error! It's likely that app made wrong request to DB.",
        MISS_REQ_PARAMS: "Not all required parameters have been set.",
        WRONG_URL: "Not existing url requested",
    }

    @classmethod
    def get_message_by_code(cls, code, default=None, **kwargs):
        return cls.message_by_code[code].format_map(
            kwargs
        ) if code in cls.message_by_code else default or "(no message: todo)"


class AccountType:
    SPOT = 0
    MARGIN = 1
    FUTURES = 2
    WALLET = 3

    SPOT_NAME = 'spot'
    MARGIN_NAME = 'margin'
    FUTURES_NAME = 'futures'
    WALLET_NAME = 'wallet'

    name_by_value = {
        SPOT: SPOT_NAME,
        MARGIN: MARGIN_NAME,
        FUTURES: FUTURES_NAME,
        WALLET: WALLET_NAME,
    }
    value_by_name = {v: k for k, v in name_by_value.items()}


class DateTimeFormat:
    TIMESTAMP = "timestamp"
    DATETIME = "datetime"
    ISO = "iso"

    TIMESTAMP_NAME = "Timestamp"
    DATETIME_NAME = "Date and time"
    ISO_NAME = "ISO"

    name_by_value = {
        TIMESTAMP: TIMESTAMP_NAME,
        DATETIME: DATETIME_NAME,
        ISO: ISO_NAME,
    }
    value_by_name = {v: k for k, v in name_by_value.items()}


class DateFormat:
    DDMMYYYY = "%d%m%Y"
    DD_MM_YY = "%d/%m/%y"
    MM_DD_YY = "%m/%d/%y"
    YYYYMMDD = "%Y%m%d"
    YYMMDD = "%y%m%d"

    DDMMYYYY_NAME = "ddmmyyyy"
    DD_MM_YY_NAME = "dd/mm/yy"
    MM_DD_YY_NAME = "mm/dd/yy"
    YYYYMMDD_NAME = "yyyymmdd"
    YYMMDD_NAME = "yymmdd"

    name_by_value = {
        DDMMYYYY: DDMMYYYY_NAME,
        DD_MM_YY: DD_MM_YY_NAME,
        MM_DD_YY: MM_DD_YY_NAME,
        YYYYMMDD: YYYYMMDD_NAME,
        YYMMDD: YYMMDD_NAME,
    }
    value_by_name = {v: k for k, v in name_by_value.items()}


class TimeFormat:
    HH_MM_SS = "%H:%M:%S"
    HH_MM = "%H:%M"
    HHMMSS = "%H%M%S"
    HHMM = "%H%M"

    HH_MM_SS_NAME = "hh:mm:ss"
    HH_MM_NAME = "hh:mm"
    HHMMSS_NAME = "hhmmss"
    HHMM_NAME = "hhmm"

    name_by_value = {
        HH_MM_SS: HH_MM_SS_NAME,
        HH_MM: HH_MM_NAME,
        HHMMSS: HHMMSS_NAME,
        HHMM: HHMM_NAME,
    }
    value_by_name = {v: k for k, v in name_by_value.items()}


class FieldSeparator:
    COMMA = ","
    SPACE = " "
    SEMICOLON = ";"

    COMMA_NAME = ", (comma)"
    SPACE_NAME = "  (space)"
    SEMICOLON_NAME = "; (semicolon)"

    name_by_value = {
        COMMA: COMMA_NAME,
        SPACE: SPACE_NAME,
        SEMICOLON: SEMICOLON_NAME,
    }
    value_by_name = {v: k for k, v in name_by_value.items()}


class FractionalSeparator:
    DOT = "."
    COMMA = ","

    DOT_NAME = ". (dot)"
    COMMA_NAME = ", (comma)"

    name_by_value = {
        DOT: DOT_NAME,
        COMMA: COMMA_NAME,
    }
    value_by_name = {v: k for k, v in name_by_value.items()}


class FileFormat:
    CSV = "csv"
    TXT = "txt"  # Same as csv, but only extension in file is different
    JSON = "json"

    CSV_NAME = ".csv"
    TXT_NAME = ".txt"
    JSON_NAME = ".json"

    name_by_value = {
        CSV: CSV_NAME,
        TXT: TXT_NAME,
        JSON: JSON_NAME,
    }
    value_by_name = {v: k for k, v in name_by_value.items()}


class LoadingStatus:
    ACTIVE = 1
    STOPPED = 2
    ERROR = 3

    ACTIVE_NAME = "active"
    STOPPED_NAME = "stopped"
    ERROR_NAME = "error"

    name_by_value = {
        ACTIVE: ACTIVE_NAME,
        STOPPED: STOPPED_NAME,
        ERROR: ERROR_NAME,
    }
    value_by_name = {v: k for k, v in name_by_value.items()}


class TransactionType:
    DEPOSIT = 1
    WITHDRAWAL = 2
    REALISED_PNL = 3

    # _NAME = ""
    # _NAME = ""
    # _NAME = ""
    #
    # name_by_value = {
    #     : _NAME,
    #     : _NAME,
    #     : _NAME,
    # }
    # value_by_name = {v: k for k, v in name_by_value.items()}

    ALL = [DEPOSIT, WITHDRAWAL, REALISED_PNL]

    @classmethod
    def check_is_created_by_user(cls, type):
        return type in [cls.DEPOSIT, cls.WITHDRAWAL]


# For DB, REST API
item_format_by_endpoint = {
    # For saving in DB (ClickHouse) and using in REST API
    # Using in WS (Online MDS)
    Endpoint.TRADE: [
        ParamName.PLATFORM_ID, ParamName.SYMBOL, ParamName.TIMESTAMP,
        ParamName.ITEM_ID, ParamName.PRICE, ParamName.AMOUNT,
        ParamName.DIRECTION
    ],
    Endpoint.CANDLE: [
        ParamName.PLATFORM_ID, ParamName.SYMBOL, ParamName.INTERVAL,
        ParamName.TIMESTAMP, ParamName.TIMESTAMP_CLOSE, ParamName.PRICE_OPEN,
        ParamName.PRICE_CLOSE, ParamName.PRICE_HIGH, ParamName.PRICE_LOW,
        ParamName.VOLUME, ParamName.TRADES_COUNT
    ],
    # Using in WS (Online MDS)
    Endpoint.TRADE_MY: [
        ParamName.PLATFORM_ID, ParamName.SYMBOL, ParamName.TIMESTAMP,
        ParamName.ITEM_ID, ParamName.PRICE, ParamName.AMOUNT,
        ParamName.DIRECTION, ParamName.ORDER_ID, ParamName.FEE,
        ParamName.REBATE
    ],
    Endpoint.TICKER: [
        ParamName.PLATFORM_ID,
        ParamName.SYMBOL,
        ParamName.TIMESTAMP,  # ParamName.ITEM_ID,
        ParamName.PRICE,  # ParamName.AMOUNT, ParamName.DIRECTION
    ],
    Endpoint.QUOTE: [
        ParamName.PLATFORM_ID, ParamName.SYMBOL, ParamName.TIMESTAMP,
        ParamName.BESTASK, ParamName.BESTBID
    ],
    Endpoint.ORDER_BOOK: [
        ParamName.PLATFORM_ID, ParamName.SYMBOL, ParamName.TIMESTAMP,
        ParamName.ITEM_ID, ParamName.ASKS, ParamName.BIDS
    ],
    Endpoint.ORDER_BOOK_AGG: [
        ParamName.PLATFORM_ID, ParamName.SYMBOL, ParamName.TIMESTAMP,
        ParamName.ITEM_ID, ParamName.ASKS, ParamName.BIDS
    ],
    # OrderBookItem: [
    #     # ParamName.PLATFORM_ID, ParamName.SYMBOL, ParamName.TIMESTAMP, ParamName.ITEM_ID,
    #     ParamName.PRICE, ParamName.AMOUNT, ParamName.DIRECTION
    # ],
    Endpoint.ACCOUNT: [
        ParamName.PLATFORM_ID,
        ParamName.TIMESTAMP,
    ],
    Endpoint.BALANCE: [
        ParamName.PLATFORM_ID,
        ParamName.SYMBOL,  # ParamName.TIMESTAMP, ParamName.ITEM_ID,
        ParamName.AMOUNT_AVAILABLE,
        ParamName.AMOUNT_RESERVED,
        ParamName.AMOUNT_BORROWED,
        ParamName.PNL
    ],
    Endpoint.BALANCE_CONVERTED: [
        ParamName.PLATFORM_ID,
        ParamName.SYMBOL,  # ParamName.TIMESTAMP, ParamName.ITEM_ID,
        ParamName.AMOUNT_AVAILABLE,
        ParamName.AMOUNT_RESERVED,
        ParamName.AMOUNT_BORROWED,
        ParamName.PNL
    ],
    Endpoint.ORDER: [
        ParamName.PLATFORM_ID,
        ParamName.SYMBOL,
        ParamName.TIMESTAMP,
        ParamName.ITEM_ID,
        ParamName.USER_ORDER_ID,
        ParamName.ORDER_TYPE,
        ParamName.AMOUNT_ORIGINAL,
        ParamName.AMOUNT_EXECUTED,
        ParamName.PRICE,
        ParamName.DIRECTION,
        ParamName.ORDER_STATUS,
        ParamName.PRICE_STOP,
    ],
    Endpoint.POSITION: [
        ParamName.PLATFORM_ID,
        ParamName.SYMBOL,
        ParamName.TIMESTAMP,  # ParamName.ITEM_ID,
        ParamName.AMOUNT,
        ParamName.DIRECTION  # ParamName.PRICE,
    ],
}
name_by_value_by_param_name = {
    ParamName.PLATFORM_ID: Platform.name_by_id,
    ParamName.SORTING: Sorting.ALL,
    ParamName.INTERVAL: CandleInterval.ALL,
    ParamName.DIRECTION:
    Direction.name_by_value,  # OrderBookDirection.name_by_value],
    ParamName.LEVEL: OrderBookDepthLevel.ALL,
    ParamName.ORDER_TYPE: OrderType.value_by_name,
    ParamName.ORDER_STATUS: OrderStatus.value_by_name,
    ParamName.ACCOUNT_TYPE: AccountType.value_by_name,
    # ParamName.LOADING_STATUS: LoadingStatus.value_by_name,
}

# REST API:

# Parse request

parse_bool_info = "True, true, TRuE, t, 1, 123, y, yes - for true; " \
                  "False, false, FalsE, f, 0, n, no, any_string - for false. "


def parse_bool(value, default=None):
    if value is None:
        return default
    if not value:
        return False
    return bool(value.lower() in ("true", "t", "1", "y", "yes") or (value.isnumeric() and int(value))) \
        if isinstance(value, str) else bool(value)


def parse_list(params, *names):
    # None -> None
    # "xxxzzz,yyyZZZ" -> ["XXXZZZ", "YYYZZZ"]

    if not params or not names or not any(
            params.get(name, None)
            for name in names if params.get(name, None)):
        return None
    items = next(
        params.getlist(name) if hasattr(params, "getlist") else params.
        get(name, None) for name in names if params.get(name, None))
    if items is None:
        return None
    if isinstance(items, list) and len(items) == 1:
        # (For params.getlist(name) which always returns list)
        items = items[0]
    if isinstance(items, str):
        items = items.split(",")
    return items


def parse_platform_ids(params):
    platforms = parse_list(params, ParamName.PLATFORM_ID,
                           ParamName.PLATFORM_IDS, ParamName.PLATFORMS,
                           ParamName.PLATFORM)
    return [_convert_platform_id(p)
            for p in platforms] if isinstance(platforms, list) else None


def parse_platform_id(params):
    if not params:
        return None

    param_names = [
        ParamName.PLATFORM, ParamName.PLATFORMS, ParamName.PLATFORM_ID
    ]
    for name in param_names:
        value = params.get(name)
        if value:
            return _convert_platform_id(value)
    return None


def _convert_platform_id(platform):
    if platform is None:
        return None
    return int(platform) if isinstance(platform, str) and platform.isnumeric() \
        else Platform.get_platform_id_by_name(platform)


def parse_symbols(params, *param_names):
    # None -> None
    # "xxxzzz,yyyZZZ" -> ["XXXZZZ", "YYYZZZ"]
    # "xxxzzz,yyy-ZZZ,UUU/zzz" -> ["XXXZZZ", "YYY_ZZZ", "UUU_ZZZ"]
    # symbols = (params.get(ParamName.SYMBOLS) or params.get(ParamName.SYMBOL)) if params else None
    # if symbols is None:
    #     return None
    # if isinstance(symbols, str):
    #     symbols = symbols.upper().split(",")
    if not param_names:
        param_names = [ParamName.SYMBOL, ParamName.SYMBOLS]
    symbols = parse_list(params, *param_names)
    symbols = Currency.convert_symbols(symbols)
    return symbols


def parse_candle_intervals(params):
    intervals = parse_list(params, ParamName.INTERVALS, ParamName.INTERVAL)
    intervals = CandleInterval.convert_to_intervals(intervals)
    return intervals


def parse_direction(params, is_remove_wrong_value=True):
    # None -> None
    # "Sell" -> 1
    # "BUY" -> 2
    direction = params.get(ParamName.DIRECTION) if params else None
    if direction is None:
        return None
    direction = int(direction) if direction.isnumeric() else \
        Direction.value_by_name.get(direction.lower(), direction)
    return direction if not is_remove_wrong_value or direction in (
        Direction.SELL, Direction.BUY) else None


def parse_interval(params, is_remove_wrong_values=True):
    intervals = []
    if params:
        for param_name in [ParamName.INTERVALS, ParamName.INTERVAL]:
            param_value = params.get(param_name)
            if param_value:
                intervals.extend(
                    CandleInterval.convert_to_intervals(
                        param_value, is_remove_wrong_values))
    return intervals if len(intervals) > 0 else None


def parse_timestamp(params, name):
    # Any time value to Unix timestamp in seconds
    time = params.get(name) if params else None
    return time_util.get_timestamp_ms(time)


def parse_datetime(params, name):
    # Any time value to Unix timestamp in seconds
    time = params.get(name) if params else None
    timestamp_s = time_util.get_timestamp_s(time)
    result = datetime.utcfromtimestamp(
        timestamp_s) if timestamp_s is not None else None
    return result


def parse_decimal(params, name):
    value = params.get(name) if params else None
    return Decimal(str(value)) if value is not None else None


def parse_limit_and_limit_skip(params, DEFAULT_LIMIT, MIN_LIMIT, MAX_LIMIT):
    limit = params.get(ParamName.LIMIT,
                       DEFAULT_LIMIT) if params else DEFAULT_LIMIT
    if isinstance(limit, str) and not limit.isnumeric():
        return None
    limit = int(limit or DEFAULT_LIMIT)
    limit = min(MAX_LIMIT, max(MIN_LIMIT, limit))
    page = params.get(ParamName.PAGE) if params else None
    if not page or (isinstance(page, str) and not page.isnumeric()):
        page = 0
    else:
        page = int(page)
    limit_skip = params.get(ParamName.LIMIT_SKIP) if params else None
    if not limit_skip or (isinstance(limit_skip, str)
                          and not limit_skip.isnumeric()):
        limit_skip = 0
    else:
        limit_skip = int(limit_skip)
    limit_skip += int(page) * limit if page and limit else 0
    limit_skip = min(MAX_LIMIT, limit_skip) if limit_skip != 0 else None
    return limit, limit_skip


def parse_sorting(params, DEFAULT_SORTING):
    sorting = params.get(ParamName.SORTING,
                         DEFAULT_SORTING) if params else DEFAULT_SORTING
    return sorting
    # sorting = params.get(ParamName.SORTING)
    # # (Any wrong value treated as default)
    # is_descending = sorting == (Sorting.ASCENDING if DEFAULT_SORTING == Sorting.DESCENDING else Sorting.DESCENDING)
    # return Sorting.DESCENDING if is_descending else Sorting.ASCENDING


def sort_from_to_params(from_value, to_value):
    # Swap if from_value > to_value
    return (to_value, from_value) if from_value is not None and to_value is not None \
                                     and from_value > to_value else (from_value, to_value)



# def check_params(params, required_param_names=None):
#     # Create error response if wrong params or None if it's alright.
#     if params is None:
#         params = {}
#     else:
#         params = {k: v if v != "" else None for k, v in params.items()}
#
#     # required_param_names - means required with values.
#     # required_param_names: list of str|list|tuple|set, if item is list,
#     #  than at least one of param names in the list should be present
#     if required_param_names:
#         for param_name in required_param_names:
#             if isinstance(param_name, str):
#                 if params.get(param_name) is None:
#                     add_error_response_item(ErrorCode.MISS_REQ_PARAMS,
#                                             field=param_name)
#             elif isinstance(param_name, Iterable) and not any(
#                     params.get(name) is not None for name in param_name):
#                 add_error_response_item(
#                     ErrorCode.MISS_REQ_PARAMS,
#                     field=list(param_name),
#                     description=
#                     f"At least one of params: {param_name} should be defined!")
#
#     return finish_error_response()



_error_items = []


def add_error_response_item(error_code=None,
                            exception=None,
                            default_message=None,
                            description=None,
                            field=None,
                            **kwargs):
    global _error_items
    if not error_code and exception:
        # if isinstance(exception, ServerException):
        # Check for clickhouse_driver.errors.ServerException
        if exception and "clickhouse" in exception.__class__.__module__ and exception.__class__.__name__ == "ServerException":
            error_code = ErrorCode.APP_DB_ERROR
        else:
            error_code = ErrorCode.APP_ERROR
    description = ' ' + description if description else ''
    message = ErrorCode.get_message_by_code(
        error_code, default=default_message, **kwargs) + description
    error_item = {
        "code": error_code,
        "message": message,
        "field": field,  # for UI
    }
    _error_items.append(error_item)


def _make_format_json(endpoint, item_format, possible_values_by_name):
    possible_values_by_name = possible_values_by_name.copy()
    # (Fix direction str value)
    if endpoint == Endpoint.ORDER_BOOK:
        possible_values_by_name[
            ParamName.DIRECTION] = OrderBookDirection.name_by_value
    else:
        possible_values_by_name[ParamName.DIRECTION] = Direction.name_by_value

    return {
        # Needed to parse data from lists
        "item_format": item_format,
        # Can be used as info to decode ids to string
        "values":
        {k: v
         for k, v in possible_values_by_name.items() if k in item_format},
        # Just for info
        "example": {
            "data": [[name + "X" for name in item_format]],
            "warnings": ["Warning 1", "Warning 2"]
        },
        "example_error": {
            "errors": [{
                "code": 1,
                "message": "Error description."
            }]
        },
    }


# Utility:

# Convert items


def convert_items_obj_to_list(item_or_items, item_format):
    if not item_or_items:
        return item_or_items
    return _convert_item_or_items_with_fun(item_or_items, item_format,
                                           _convert_items_obj_to_list)


def convert_items_dict_to_list(item_or_items, item_format):
    if not item_or_items:
        return item_or_items
    return _convert_item_or_items_with_fun(item_or_items, item_format,
                                           _convert_items_dict_to_list)


def convert_items_to_list(item_or_items, item_format):
    if not item_or_items:
        return item_or_items
    return _convert_item_or_items_with_fun(item_or_items, item_format,
                                           _convert_items_to_list)


def convert_items_list_to_dict(item_or_items, item_format):
    if not item_or_items:
        return item_or_items
    return _convert_item_or_items_with_fun(item_or_items, item_format,
                                           _convert_items_list_to_dict)


def convert_items_obj_to_dict(item_or_items, item_format):
    if not item_or_items:
        return item_or_items
    return _convert_item_or_items_with_fun(item_or_items, item_format,
                                           _convert_items_obj_to_dict)


def convert_items_to_dict(item_or_items, item_format):
    if not item_or_items:
        return item_or_items
    return _convert_item_or_items_with_fun(item_or_items, item_format,
                                           _convert_items_to_dict)


def convert_items_to_obj(item_or_items, item_format, item_type):
    if not item_or_items:
        return item_or_items
    return _convert_item_or_items_with_fun(item_or_items, item_format,
                                           _convert_items_to_obj, item_type)


def _convert_item_or_items_with_fun(item_or_items, item_format, fun, *args):
    # Input item - output item,
    # input items - output items
    if not item_format:
        raise Exception("item_format cannot be None!")

    is_list = isinstance(item_or_items, (list, tuple))
    if is_list:
        for element in item_or_items:
            if element:
                # Check the first not None element is not an item
                # (list, dict (iterable but not a str) or object (has __dict__))
                if isinstance(element, str) or not isinstance(element, Iterable) and \
                        not hasattr(element, "__dict__"):
                    is_list = False
                break
    items = item_or_items if is_list else [item_or_items]
    # Convert
    result = fun(items, item_format, *args) if items else []
    return result if is_list else result[0]


def _convert_items_obj_to_list(items, item_format):
    return [[getattr(item, p) for p in item_format
             if hasattr(item, p)] if item is not None else None
            for item in items] if items else []


def _convert_items_dict_to_list(items, item_format):
    return [[item[p] for p in item_format
             if p in item] if item is not None else None
            for item in items] if items else []


def _convert_items_list_to_dict(items, item_format):
    index_property_list = list(enumerate(item_format))
    return [{p: item[i]
             for i, p in index_property_list
             if i < len(item)} if item is not None else None
            for item in items] if items else []


def _convert_items_obj_to_dict(items, item_format):
    return [{p: getattr(item, p)
             for p in item_format
             if hasattr(item, p)} if item is not None else None
            for item in items] if items else []


def _convert_items_list_to_obj(items, item_format, obj_type):
    result = []
    for item in items:
        obj = obj_type()
        for i, p in enumerate(item_format):
            if i >= len(item):
                break
            value = Decimal(item[i]) if ParamName.is_decimal(
                p) and item[i] is not None else item[i]
            setattr(obj, p, value)
        if hasattr(obj, "is_milliseconds") and hasattr(
                obj, ParamName.TIMESTAMP) and obj.timestamp:
            obj.is_milliseconds = obj.timestamp > 15000000000
        result.append(obj)

    return result


def _convert_items_dict_to_obj(items, item_format, obj_type):
    result = []
    for item in items:
        obj = obj_type()
        for p in item_format:
            if p in item:
                setattr(obj, p, item.get(p))
        if hasattr(obj, "is_milliseconds") and hasattr(
                obj, ParamName.TIMESTAMP) and obj.timestamp:
            obj.is_milliseconds = obj.timestamp > 15000000000
        result.append(obj)
    return result


def _convert_items_to_list(items, item_format):
    result = []
    for item in items:
        if isinstance(item, list):
            result.append(item)
        elif isinstance(item, dict):
            result += _convert_items_dict_to_list([item], item_format)
        elif hasattr(item, "__dict__"):
            result += _convert_items_obj_to_list([item], item_format)
        else:
            result.append(item)
    return result


def _convert_items_to_dict(items, item_format):
    result = []
    for item in items:
        if isinstance(item, list):
            result += _convert_items_list_to_dict([item], item_format)
        elif isinstance(item, dict):
            result.append(item)
        elif hasattr(item, "__dict__"):
            result += _convert_items_obj_to_dict([item], item_format)
        else:
            result.append(item)
    return result


def _convert_items_to_obj(items, item_format, obj_type):
    result = []
    for item in items:
        if isinstance(item, list):
            result += _convert_items_list_to_obj([item], item_format, obj_type)
        elif isinstance(item, dict):
            result += _convert_items_dict_to_obj([item], item_format, obj_type)
        elif hasattr(item, "__dict__"):
            result.append(item)
        else:
            result.append(item)
    return result


def apply_data_on_obj(obj, item, item_format):
    if isinstance(item, list):
        for i, p in enumerate(item_format):
            if i >= len(item):
                break
            setattr(obj, p, item[i])
    elif isinstance(item, dict):
        for p in item_format:
            if p in item:
                setattr(obj, p, item.get(p))
    if hasattr(obj, "is_milliseconds") and hasattr(
            obj, ParamName.TIMESTAMP) and obj.timestamp:
        obj.is_milliseconds = obj.timestamp > 15000000000
    return obj
