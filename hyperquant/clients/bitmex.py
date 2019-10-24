import datetime
import hashlib
import hmac
import json
import logging
import time
import urllib
from collections import OrderedDict
from decimal import Decimal
from operator import itemgetter

from hyperquant.api import (CandleInterval, Currency, CurrencyPair, Direction,
                            OrderBookDepthLevel, OrderStatus, OrderTimeInForce,
                            OrderType, Platform, Sorting, TransactionType)
from hyperquant.clients import (Account, Balance, BalanceTransaction, Candle,
                                Endpoint, Error, ErrorCode, ItemObject,
                                MyTrade, Order, OrderBook, OrderBookDiff,
                                OrderBookItem, ParamName, Position,
                                PrivatePlatformRESTClient, Quote,
                                RESTConverter, Ticker, Trade, WSClient,
                                WSConverter)

logger = logging.getLogger(__name__)

# TODO finish orderbook

# REST


class BitMEXRESTConverterV1(RESTConverter):
    """
    Go https://www.bitmex.com/api/v1/schema for whole API schema with param types keys
    which help to distinguish items from each other (for updates and removing).
    """
    SATOSHI_TO_XBT = Decimal("0.00000001")

    # Main params:
    base_url = "https://www.bitmex.com/api/v{version}/"

    IS_SORTING_ENABLED = True

    # Settings:

    # Converting info:
    # For converting to platform
    endpoint_lookup = {
        # Endpoint.PING: "",
        # Endpoint.SERVER_TIME: "",
        Endpoint.SYMBOLS:
        "instrument",
        Endpoint.SYMBOLS_ACTIVE:
        "instrument/active",
        Endpoint.CURRENCY_PAIRS:
        "instrument/active",
        Endpoint.TRADE:
        "trade",
        Endpoint.TRADE_HISTORY:
        "trade",
        Endpoint.TRADE_MY:
        "execution/tradeHistory",  # ?/user/executionHistory  # Private
        Endpoint.CANDLE:
        "trade/bucketed",
        Endpoint.TICKER:
        "instrument/active",
        Endpoint.TICKER_ALL:
        "instrument/active",
        Endpoint.ORDER_BOOK:
        "orderBook/L2",

        # https://github.com/BitMEX/api-connectors/issues/141#issuecomment-392629551
        Endpoint.QUOTE:
        "orderBook/L2",  # Private
        Endpoint.ACCOUNT:
        "user",
        Endpoint.BALANCE:
        "user/margin",  # ?"user/wallet" todo check which updates more frequently
        Endpoint.BALANCE_TRANSACTION:
        "user/walletHistory",
        Endpoint.ORDER:
        "order",
        Endpoint.ORDER_CREATE:
        "order",
        Endpoint.ORDER_CANCEL:
        "order",
        Endpoint.ORDERS_OPEN:
        "order",  # filter='{"open": true}'
        Endpoint.ORDERS_ALL:
        "order",
        Endpoint.ORDERS_ALL_CANCEL:
        "order/all",
        Endpoint.POSITION:
        "position",
        Endpoint.POSITION_CLOSE:
        "order",  # with execInst: "Close"
        Endpoint.LEVERAGE_SET:
        "position/leverage",
    }
    param_name_lookup = {
        ParamName.ORDER_ID: "orderID",  # !!! use userOrderID property!
        ParamName.SYMBOL: "symbol",
        ParamName.LIMIT: "count",
        ParamName.LIMIT_SKIP: "start",
        ParamName.SORTING: "reverse",
        ParamName.IS_USE_MAX_LIMIT: None,
        ParamName.INTERVAL: "binSize",
        ParamName.DIRECTION: "side",
        ParamName.ORDER_TYPE: "ordType",
        ParamName.AMOUNT: "orderQty",
        ParamName.TIMESTAMP: "timestamp",
        ParamName.FROM_ITEM: "startTime",
        ParamName.TO_ITEM: "endTime",
        ParamName.FROM_TIME: "startTime",
        ParamName.TO_TIME: "endTime",
        ParamName.VOLUME: "volume",
        ParamName.PRICE: "price",
        ParamName.TRADES_COUNT: "trades",
        ParamName.PRICE_STOP: "stopPx",
        ParamName.PRICE_LIMIT: "price",
        ParamName.TIME_IN_FORCE: "timeInForce",
        # ParamName.ASKS: "",
        # ParamName.BIDS: "",
    }
    param_value_lookup = {
        # Uncomment only if you use BTCUSD elsewhere
        # ParamName.SYMBOL: {
        #     "BTCUSD": "XBTUSD",
        #     "BTC": "XBT",
        # },
        # ParamName.AMOUNT_AVAILABLE: "amount",
        ParamName.SORTING: {
            Sorting.ASCENDING: "false",
            Sorting.DESCENDING: "true",
        },
        # (Should be out of ParamName.SORTING: {})
        Sorting.DEFAULT_SORTING:
        Sorting.ASCENDING,
        ParamName.INTERVAL: {
            CandleInterval.MIN_1: "1m",
            CandleInterval.MIN_5: "5m",
            CandleInterval.HRS_1: "1h",
            CandleInterval.DAY_1: "1d",
        },

        # By properties:
        # https://www.onixs.biz/fix-dictionary/5.0.SP2/tagNum_54.html
        ParamName.DIRECTION: {
            Direction.SELL: "Sell",
            Direction.BUY: "Buy",
        },
        # https://www.onixs.biz/fix-dictionary/5.0.SP2/tagNum_40.html
        ParamName.ORDER_TYPE: {
            OrderType.LIMIT: "Limit",
            OrderType.MARKET: "Market",
            OrderType.STOP_MARKET: "Stop",
            OrderType.STOP_LIMIT: "StopLimit",
            OrderType.TAKE_PROFIT_MARKET: "MarketIfTouched",
            OrderType.TAKE_PROFIT_LIMIT: "LimitIfTouched",
        },
        ParamName.TIME_IN_FORCE: {
            OrderTimeInForce.DAY: "Day",
            OrderTimeInForce.GTC: "GoodTillCancel",
            OrderTimeInForce.IOC: "ImmediateOrCancel",
            OrderTimeInForce.FOK: "FillOrKill",
        },
        # # https://www.onixs.biz/fix-dictionary/4.2/tagnum_39.html
        # ParamName.ORDER_STATUS: {
        #     # OrderStatus.NEW: 0,
        #     # OrderStatus.PARTIALLY_FILLED: 1,
        #     # OrderStatus.FILLED: 2,
        #     # # OrderStatus.PENDING_CANCEL: "PENDING_CANCEL",
        #     # OrderStatus.CANCELED: 4,
        #     # OrderStatus.REJECTED: 8,
        #     # OrderStatus.EXPIRED: "C",
        #     OrderStatus.NEW: "New",
        #     OrderStatus.PARTIALLY_FILLED: "PartiallyFilled",
        #     OrderStatus.FILLED: "Filled",
        #     # OrderStatus.PENDING_CANCEL: "PendingCancel",
        #     OrderStatus.CANCELED: "Canceled",
        #     OrderStatus.REJECTED: "Rejected",
        #     OrderStatus.EXPIRED: "Expired",
        # },
        ParamName.TRANSACTION_TYPE: {
            TransactionType.DEPOSIT: "Deposit",
            TransactionType.WITHDRAWAL: "Withdrawal",
            TransactionType.REALISED_PNL:
            "RealisedPNL",  # temp - for BitMEX, may be changed later
        },
    }
    param_value_reversed_lookup = {
        ParamName.ORDER_STATUS: {
            "New": OrderStatus.NEW,
            "PartiallyFilled": OrderStatus.PARTIALLY_FILLED,
            "Filled": OrderStatus.FILLED,
            "DoneForDay": OrderStatus.OPEN,  # ? "open",
            "Canceled": OrderStatus.CANCELED,
            "PendingCancel": OrderStatus.OPEN,  # ? "open",
            "PendingNew": OrderStatus.NEW,
            "Rejected": OrderStatus.REJECTED,
            "Expired": OrderStatus.EXPIRED,
            "Stopped": OrderStatus.OPEN,  # ? "open",
            "Untriggered": OrderStatus.OPEN,  # ? "open",
            "Triggered": OrderStatus.OPEN,  # ? "open",
        },
    }
    max_limit_by_endpoint = {
        Endpoint.TRADE: 500,
        Endpoint.TRADE_HISTORY: 500,
        # Endpoint.ORDER_BOOK: 25,
        # Endpoint.CANDLE: 500,
    }

    # For parsing
    # TODO check all property names are right
    param_lookup_by_class = {
        # Error
        Error: {
            "name": "code",
            "message": "message",
        },
        # Data
        Trade: {
            "trdMatchID": ParamName.ITEM_ID,
            "timestamp": ParamName.TIMESTAMP,
            "symbol": ParamName.SYMBOL,
            "size": ParamName.AMOUNT,
            "price": ParamName.PRICE,
            "side": ParamName.DIRECTION,
        },
        MyTrade: {
            "symbol": ParamName.SYMBOL,
            "timestamp": ParamName.TIMESTAMP,
            "orderID": ParamName.ORDER_ID,
            "execID": ParamName.ITEM_ID,  # ?
            "cumQty": ParamName.AMOUNT,  # ???
            "avgPx": ParamName.PRICE,
            "side": ParamName.DIRECTION,
            "commission": ParamName.FEE,
            # "": ParamName.FEE_SYMBOL,
            # "": ParamName.REBATE,
        },
        Candle: {
            "symbol": ParamName.SYMBOL,
            "timestamp": ParamName.TIMESTAMP,
            "high": ParamName.PRICE_HIGH,
            "low": ParamName.PRICE_LOW,
            "close": ParamName.PRICE_CLOSE,
            "open": ParamName.PRICE_OPEN,
            "volume": ParamName.VOLUME,
            "trades": ParamName.TRADES_COUNT,
        },
        Ticker: {
            "symbol": ParamName.SYMBOL,
            "lastPrice": ParamName.PRICE,
            # WS:
            "timestamp": ParamName.TIMESTAMP,
        },
        OrderBook: {
            "lastUpdateId": ParamName.ITEM_ID,
            "bids": ParamName.BIDS,
            "asks": ParamName.ASKS,
            "symbol": ParamName.SYMBOL,
        },
        Quote: {
            ParamName.SYMBOL: ParamName.SYMBOL,
            ParamName.ASKS: ParamName.BESTASK,
            ParamName.BIDS: ParamName.BESTBID,
            ParamName.TIMESTAMP: ParamName.TIMESTAMP,
            # "timestamp": ParamName.TIMESTAMP,
            # "askPrice": ParamName.BESTASK,
            # "bidPrice": ParamName.BESTBID,
            # "symbol": ParamName.SYMBOL,
        },
        # OrderBookItem: {
        #     "symbol": ParamName.SYMBOL,
        #     "id": ParamName.ITEM_ID,
        #     "size": ParamName.AMOUNT,
        #     "price": ParamName.PRICE,
        #     "side": ParamName.DIRECTION,
        # },
        OrderBookItem: [ParamName.PRICE, ParamName.AMOUNT],
        Account: {
            "lastUpdated": ParamName.TIMESTAMP,
            # "": ParamName.BALANCES,
        },
        Balance: {
            "currency": ParamName.SYMBOL,
            # -"amount": ParamName.AMOUNT_AVAILABLE,
            # TODO?
            # "availableMargin": ParamName.AMOUNT_AVAILABLE,
            # Equal values if no open positions:
            #   walletBalance, marginBalance, excessMargin, availableMargin, withdrawableMargin
            # Equal values if open positions: excessMargin, availableMargin, withdrawableMargin
            "availableMargin": ParamName.AMOUNT_AVAILABLE,
            "marginBalance": ParamName.MARGIN_BALANCE,
            # "": ParamName.AMOUNT_RESERVED,
            "unrealisedPnl": ParamName.PNL,
        },
        BalanceTransaction: {
            "currency": ParamName.SYMBOL,
            "transactTime": ParamName.TIMESTAMP,
            "transactID": ParamName.ITEM_ID,
            "transactType": ParamName.TRANSACTION_TYPE,
            "amount": ParamName.AMOUNT,
            "fee": ParamName.FEE,
            # todo
            # "transactStatus": ParamName.TRANSACTION_STATUS,
        },
        Order: {
            "symbol": ParamName.SYMBOL,
            # "transactTime": ParamName.TIMESTAMP,  # choose?
            "timestamp": ParamName.TIMESTAMP,
            "orderID": ParamName.ITEM_ID,
            "clOrdID": ParamName.USER_ORDER_ID,
            "ordType": ParamName.ORDER_TYPE,
            "orderQty": ParamName.AMOUNT_ORIGINAL,
            "cumQty": ParamName.AMOUNT_EXECUTED,  # ?
            # "leavesQty": ParamName.AMOUNT_LEFT,  # ? simpleOrderQty, orderQty, displayQty,
            # # simpleLeavesQty, leavesQty, simpleCumQty, cumQty
            "price": ParamName.PRICE,
            "stopPx": ParamName.PRICE_STOP,
            "side": ParamName.DIRECTION,
            "ordStatus": ParamName.ORDER_STATUS,
        },
        Position: {
            # Parsing position (Endpoint.POSITION)
            "symbol": ParamName.SYMBOL,
            "timestamp": ParamName.TIMESTAMP,
            "currentQty": ParamName.AMOUNT,
            "side": ParamName.DIRECTION,
            "isOpen": ParamName.IS_OPEN,
            "liquidationPrice": ParamName.PRICE_MARGIN_CALL,
            "avgEntryPrice": ParamName.PRICE_AVERAGE,
            "unrealisedPnl": ParamName.PROFIT_N_LOSS,

            # Parsing order (Endpoint.POSITION_CLOSE)
            "leavesQty": ParamName.AMOUNT,
            # "posMargin" - amount in XBt
        },
        CurrencyPair: {
            "maxOrderQty": ParamName.LOT_SIZE_MAX,
            "lotSize": ParamName.LOT_SIZE_STEP,
            "tickSize": ParamName.PRICE_STEP,
            "quoteCurrency": ParamName.SYMBOL_QUOTE,
            "underlying": ParamName.SYMBOL_BASE,
            "symbol": ParamName.PLATFORM_SYMBOL_NAME,
        },
    }

    error_code_by_platform_error_code = {
        # "": ErrorCode.UNAUTHORIZED,
        "Unknown symbol": ErrorCode.WRONG_SYMBOL,
        "RateLimitError": ErrorCode.RATE_LIMIT,
    }
    error_code_by_http_status = {
        400: ErrorCode.WRONG_PARAM,
        401: ErrorCode.UNAUTHORIZED,
        429: ErrorCode.RATE_LIMIT,  # ?
    }

    # For converting time
    is_source_in_timestring = True
    timestamp_platform_names = ["startTime", "endTime"]

    intervals_supported = [
        CandleInterval.MIN_1, CandleInterval.MIN_5, CandleInterval.HRS_1,
        CandleInterval.DAY_1
    ]

    # Convert to platform format

    @staticmethod
    def _inject_filter(keyword_from, keyword_to, params):
        if keyword_from in params and params[keyword_from]:
            if 'filter' not in params:
                params['filter'] = '{}'
            _filter = json.loads(params['filter'])
            _filter[keyword_to] = params[keyword_from]
            params.pop(keyword_from)
            params['filter'] = json.dumps(_filter)

    def _convert_params_to_platform(self, params, endpoint):
        if endpoint in [Endpoint.ORDERS_ALL, Endpoint.TRADE_MY]:
            if ParamName.FROM_ITEM in params and isinstance(
                    params[ParamName.FROM_ITEM], (MyTrade, Order)):
                ts = params[ParamName.FROM_ITEM].timestamp_s
                params[
                    ParamName.FROM_ITEM] = datetime.datetime.utcfromtimestamp(
                        ts).strftime("%Y-%m-%d %H:%M:%S.%f")
            self._inject_filter(ParamName.FROM_ITEM,
                                self.param_name_lookup[ParamName.FROM_TIME],
                                params)
        elif endpoint in [Endpoint.ORDER]:
            self._inject_filter(ParamName.ORDER_ID,
                                self.param_name_lookup[ParamName.ORDER_ID],
                                params)
        elif endpoint in [Endpoint.POSITION]:
            self._inject_filter(ParamName.SYMBOL,
                                self.param_name_lookup[ParamName.SYMBOL],
                                params)
        elif endpoint == Endpoint.ORDERS_OPEN:
            self._inject_filter(ParamName.IS_OPEN, "open", params)
        elif endpoint == Endpoint.ORDER_CREATE:
            required = tuple(params.keys())
            if params[ParamName.ORDER_TYPE] in (OrderType.STOP_MARKET,
                                                OrderType.TAKE_PROFIT_MARKET):
                required = (ParamName.AMOUNT, ParamName.PRICE_STOP,
                            ParamName.DIRECTION, ParamName.SYMBOL,
                            ParamName.ORDER_TYPE)
            params = {k: v for k, v in params.items() if k in required}
        return super()._convert_params_to_platform(params, endpoint)

    def _process_param_value(self, name, value):
        if name == ParamName.FROM_ITEM or name == ParamName.TO_ITEM:
            if isinstance(value, ItemObject):
                timestamp = value.timestamp
                if name == ParamName.TO_ITEM:
                    # Make to_item an including param (for BitMEX it's excluding)
                    # maybe multiply?
                    timestamp += (1000 if value.is_milliseconds else 1)
                return timestamp
        return super()._process_param_value(name, value)

    def _parse_item(self, endpoint, item_data):
        result = super()._parse_item(endpoint, item_data)

        # (For Trade)
        if endpoint == Endpoint.TRADE and hasattr(
                result, ParamName.SYMBOL) and result.symbol[0] == ".":
            # # ".ETHUSD" -> "ETHUSD"
            # result.symbol = result.symbol[1:]
            # https://www.bitmex.com/api/explorer/#!/Trade/Trade_get Please note
            # that indices (symbols starting with .) post trades at intervals to
            # the trade feed. These have a size of 0 and are used only to indicate
            # a changing price.
            return None

        if hasattr(result, ParamName.FEE) and result.fee and result.fee < 0:
            result.fee = -result.fee
        # Convert direction
        if result and hasattr(result,
                              ParamName.DIRECTION) and not result.direction:
            result.direction = None
            if isinstance(result, Position) and result.amount:
                result.direction = Direction.BUY if result.amount > 0 else Direction.SELL
                if result.amount < 0:
                    result.amount = -result.amount

        return result

    def parse(self, endpoint, data):
        if endpoint == Endpoint.CURRENCY_PAIRS:
            result = [
                self._parse_item(endpoint, item_data) for item_data in data
            ]
            return result
        elif endpoint in [Endpoint.SYMBOLS, Endpoint.SYMBOLS_ACTIVE]:
            result = [item_data[ParamName.SYMBOL] for item_data in data]
            return result
        elif endpoint == Endpoint.ORDER_BOOK:
            result = dict(asks=[(item['price'], item['size']) for item in data
                                if item['side'] == 'Sell'],
                          bids=[(item['price'], item['size']) for item in data
                                if item['side'] == 'Buy'])
            result[ParamName.SYMBOL] = data[0][ParamName.SYMBOL]
            data = result
        elif endpoint == Endpoint.QUOTE:
            result = {
                ParamName.ASKS if side['side'] == "Sell" else ParamName.BIDS:
                side[ParamName.PRICE]
                for side in data
            }
            data = result if result else None
        return super().parse(endpoint, data)

    def parse_error(self, error_data=None, response=None):
        if error_data and "error" in error_data:
            error_data = error_data["error"]
            if "Maximum result count is" in error_data["message"]:
                error_data["name"] = ErrorCode.WRONG_LIMIT
        if error_data == '[]':
            # implicit failure from bitmex
            response = None
            error_data = {"name": ErrorCode.APP_ERROR}
        result = super().parse_error(error_data, response)
        return result

    def _convert_satoshi_to_xbt(self, item, item_data=None):
        # https://www.bitmex.com/app/restAPI "Обратите внимание: все суммы в биткойнах при
        # возврате запроса указываются в Satoshi: 1 XBt (Satoshi) = 0.00000001 XBT (биткойн)."
        if isinstance(
                item, Balance
        ) and item and item.amount_available is not None and item_data:
            # marginBalance = walletBalance - unrealisedPnl
            total_amount = item_data.get("walletBalance")
            if total_amount is not None:
                item.amount_reserved = total_amount - item.amount_available

        if hasattr(item, ParamName.SYMBOL) and item.symbol == "XBt":
            for name in self.currency_param_names:
                if hasattr(item, name) and getattr(item, name):
                    setattr(item, name, getattr(item, name) / 100000000)
            setattr(item, ParamName.SYMBOL, "XBT")

    def _post_process_item(self, item, item_data=None):
        self._convert_satoshi_to_xbt(item, item_data)
        if hasattr(item, ParamName.BESTASK) and ParamName.ASKS in item_data:
            item.bestask = Decimal(item_data[ParamName.ASKS])
        if hasattr(item, ParamName.BESTBID) and ParamName.BIDS in item_data:
            item.bestbid = Decimal(item_data[ParamName.BIDS])
        return super()._post_process_item(item, item_data)

    def _generate_and_add_signature(self,
                                    method,
                                    url,
                                    endpoint,
                                    platform_params,
                                    headers,
                                    api_key,
                                    api_secret,
                                    passphrase=None):
        # Add secure headers
        expires = generate_expires()
        headers["api-expires"] = str(expires)
        headers["api-key"] = api_key
        platform_params = OrderedDict(platform_params.items(
        ))  # OrderedDict(sorted(platform_params.items(), key=lambda v: v[0]))
        headers["api-signature"] = generate_signature(method, url,
                                                      platform_params, expires,
                                                      api_secret)

        return platform_params, headers


class BitMEXRESTClient(PrivatePlatformRESTClient):
    platform_id = Platform.BITMEX
    version = "1"  # Default version

    is_futures = True
    is_balance_transactions_supported = True
    is_position_supported = True
    price_tick_size = 0.5

    IS_NONE_SYMBOL_FOR_ALL_SYMBOLS = True

    supported_order_types = (OrderType.MARKET, OrderType.LIMIT,
                             OrderType.STOP_LIMIT, OrderType.TAKE_PROFIT_LIMIT,
                             OrderType.STOP_MARKET,
                             OrderType.TAKE_PROFIT_MARKET)

    _converter_class_by_version = {
        "1": BitMEXRESTConverterV1,
    }

    wait_before_fetch_s = 2

    def _send(self, method, endpoint, params=None, version=None, **kwargs):
        if method == "GET" and endpoint in [
                Endpoint.ORDERS_OPEN, Endpoint.ORDERS_ALL
        ]:
            # The newest orders first
            kwargs["reverse"] = True

        return super()._send(method, endpoint, params, version, **kwargs)

    def _on_response(self, response, result):
        # super()._on_response(response)

        if not response.ok and "Retry-After" in response.headers:
            self.delay_before_next_request_sec = int(
                response.headers.get("Retry-After", 0))
        else:
            # "x-ratelimit-limit": 300
            # "x-ratelimit-remaining": 297
            # "x-ratelimit-reset": 1489791662
            try:
                ratelimit = int(response.headers.get("x-ratelimit-limit", -1))
                remaining_requests = float(
                    response.headers.get("x-ratelimit-remaining", -1))
                reset_ratelimit_timestamp = int(
                    response.headers.get("x-ratelimit-reset", -1))
                if ratelimit >= 0 and remaining_requests < ratelimit * 0.1 and reset_ratelimit_timestamp > 0:
                    precision_sec = 1  # Current machine time may not precise which can cause ratelimit error
                    self.delay_before_next_request_sec = reset_ratelimit_timestamp - time.time(
                    ) + precision_sec
                else:
                    self.delay_before_next_request_sec = 0
                self.logger.debug(
                    "Ratelimit info. remaining_requests: %s/%s delay: %s",
                    remaining_requests, ratelimit,
                    self.delay_before_next_request_sec)
            except Exception as error:
                self.logger.exception(
                    "Error while defining delay_before_next_request_sec.",
                    error)

    def fetch_quote(self, symbol: str = None, version: str = None, **kwargs):
        kwargs['depth'] = 1
        return super().fetch_quote(symbol=symbol, version=version, **kwargs)

    def fetch_symbols(self, version=None, **kwargs):
        endpoint = Endpoint.SYMBOLS_ACTIVE
        response = self._send("GET", endpoint, version=version, **kwargs)
        if not isinstance(response, Error):
            self._symbols = Currency.convert_to_symbols(response)
            return self._symbols
        else:
            return response

    def fetch_ticker(self, symbol=None, version=None, **kwargs):
        result = super().fetch_ticker(symbol=symbol, version=version, **kwargs)
        if isinstance(result, list) and symbol:
            for item in result:
                if item.symbol == symbol:
                    return item
            return Error(code=ErrorCode.WRONG_SYMBOL, message="Wrong symbol")
        return result

    def fetch_balance(self, version=None, **kwargs):
        result = super().fetch_balance(version, **kwargs)
        if not isinstance(result, list):
            result = [result]
        return result

    def cancel_order(self, order, symbol=None, version=None, **kwargs):
        if not order:
            return Error(
                ErrorCode.WRONG_PARAM,
                "Define order or order_id to be checked, "
                "or Use fetch_orders() to get all orders.")
        return super().cancel_order(order, symbol, version, **kwargs)

    def fetch_order(self, order_or_id, symbol=None, version=None, **kwargs):
        if not order_or_id:
            # For BitMEX, order=None returns not error, but all orders, which breaks our standard logic
            return Error(
                ErrorCode.WRONG_PARAM,
                "Define order or order_id to be checked, "
                "or Use fetch_orders() to get all orders.")
        result = super().fetch_order(order_or_id, symbol, version, **kwargs)
        return result[0] if isinstance(result,
                                       list) and len(result) else result

    def fetch_orders(self,
                     symbol=None,
                     limit=None,
                     from_item=None,
                     is_open_only=False,
                     version=None,
                     **kwargs):
        orders = super().fetch_orders(symbol=symbol,
                                      limit=limit,
                                      from_item=from_item,
                                      is_open_only=is_open_only,
                                      version=version,
                                      **kwargs)

        return orders

    def close_position(self, position_or_symbol, version=None, **kwargs):
        # Using POST "/order" with execInst: "Close"
        kwargs["execInst"] = "Close"

        result = super().close_position(position_or_symbol, version, **kwargs)
        return result

    def close_all_positions(self, symbol=None, version=None, **kwargs):
        # Using POST "/order" with execInst: "Close"
        kwargs["execInst"] = "Close"

        if not symbol:
            positions = self.get_positions()
            position_symbols = [p.symbol for p in positions if p.is_open
                                ] if isinstance(positions, list) else None
            self.logger.info(
                "close_all_positions -> Close position_symbols: %s",
                position_symbols)

            result = []
            if position_symbols:
                for symbol in position_symbols:
                    result += self.close_all_positions(symbol)
            return result

        return super().close_all_positions(symbol, version, **kwargs)


# WebSockets


class BitMEXWSConverterV1(WSConverter):
    # Main params:
    base_url = "wss://www.bitmex.com/realtime"

    IS_SUBSCRIPTION_COMMAND_SUPPORTED = True
    MAX_TABLE_LEN = 500

    # Private WS Part
    supported_endpoints = [
        Endpoint.TRADE,
        Endpoint.CANDLE,
        Endpoint.TICKER,
        Endpoint.TICKER_ALL,
        Endpoint.ORDER_BOOK,
        Endpoint.ORDER_BOOK_DIFF,
        Endpoint.BALANCE,
        Endpoint.POSITION,
        Endpoint.ORDER,
        Endpoint.TRADE_MY,
        Endpoint.QUOTE,
    ]
    symbol_endpoints = supported_endpoints

    # # symbol_endpoints = ["execution", "instrument", "order", "orderBookL2", "position", "quote", "trade"]
    # # supported_endpoints = symbolSubs + ["margin"]
    # supported_endpoints = [Endpoint.TRADE]
    # symbol_endpoints = [Endpoint.TRADE]

    # Settings:

    # Converting info:
    # For converting to platform
    endpoint_lookup = {
        Endpoint.TRADE:
        "trade:{symbol}",
        Endpoint.TRADE_MY:
        "execution:{symbol}",

        # Endpoint.TRADE: lambda params: "trade:" + params[Param.SYMBOL] if Param.SYMBOL in params else "trade",
        Endpoint.CANDLE:
        "tradeBin{interval}",
        # todo try "tradeBinXX" # "instrument" - Cannot be implemented properly - with interval property
        Endpoint.TICKER:
        "instrument",
        Endpoint.TICKER_ALL:
        "instrument",
        Endpoint.ORDER_BOOK:
        "orderBook{level}",  # orderBookL2 - full
        # Endpoint.ORDER_BOOK_DIFF: "orderBookL2_25",
        Endpoint.QUOTE:
        "quote:{symbol}",  # orderBookL2 - full
        Endpoint.ORDER_BOOK_DIFF:
        "orderBook{level}",
        # Private WS Part
        Endpoint.BALANCE:
        "margin",
        Endpoint.POSITION:
        "position",
    }

    # For parsing
    param_lookup_by_class = {
        **BitMEXRESTConverterV1.param_lookup_by_class,
        # Error
        Error: {
            "status": "code",
            "error": "message",
        },
        # Data
        Candle: {
            "symbol": ParamName.SYMBOL,
            "timestamp": ParamName.TIMESTAMP,  # "closingTimestamp"

            # "": ParamName.INTERVAL,
            "open": ParamName.PRICE_OPEN,
            "high": ParamName.PRICE_HIGH,
            "low": ParamName.PRICE_LOW,
            "close": ParamName.PRICE_CLOSE,
            "volume": ParamName.VOLUME,
            "trades": ParamName.TRADES_COUNT,
        },
        Balance: {
            "currency": ParamName.SYMBOL,
            # TODO
            # "availableMargin": ParamName.AMOUNT_AVAILABLE,
            "availableMargin": ParamName.AMOUNT_AVAILABLE,
            "marginBalance": ParamName.MARGIN_BALANCE,
            "unrealisedPnl": ParamName.PNL,
            # "": ParamName.AMOUNT_RESERVED,
        },
        # Same as in BitMEXRESTConverterV1.param_lookup_by_class
        Quote: {
            "timestamp": ParamName.TIMESTAMP,
            "askPrice": ParamName.BESTASK,
            "bidPrice": ParamName.BESTBID,
            "symbol": ParamName.SYMBOL,
        },
        OrderBook: {
            ParamName.SYMBOL:
            ParamName.SYMBOL,
            # "": ParamName.TIMESTAMP,
            # "": ParamName.ITEM_ID,
            ParamName.ASKS:
            ParamName.ASKS,
            ParamName.BIDS:
            ParamName.BIDS,
        },
        OrderBookDiff: {
            ParamName.SYMBOL:
            ParamName.SYMBOL,
            # "": ParamName.TIMESTAMP,
            # "": ParamName.ITEM_ID,
            ParamName.ASKS:
            ParamName.ASKS,
            ParamName.BIDS:
            ParamName.BIDS,
        },
        OrderBookItem: {
            "symbol": ParamName.SYMBOL,
            # "": ParamName.TIMESTAMP,
            "id": ParamName.ITEM_ID,
            "side": ParamName.DIRECTION,
            "size": ParamName.AMOUNT,
            "price": ParamName.PRICE,
        },
    }
    param_lookup_by_class[MyTrade]['orderQty'] = ParamName.AMOUNT
    param_value_lookup = BitMEXRESTConverterV1.param_value_lookup.copy()
    param_value_reversed_lookup = BitMEXRESTConverterV1.param_value_reversed_lookup.copy(
    )
    param_value_lookup[ParamName.LEVEL] = {
        OrderBookDepthLevel.LIGHT:
        "L2_25",  # "10", - has completely different format
        OrderBookDepthLevel.MEDIUM: "L2_25",
        OrderBookDepthLevel.DEEP: "L2_25",
        OrderBookDepthLevel.DEEPEST: "L2",
    }
    subscription_param = "table"
    event_type_param = "table"
    # (Skip parameterized platform endpoints)
    endpoint_by_event_type = {
        pl_e.split(":")[0]: e
        for e, pl_e in endpoint_lookup.items()
    }
    # endpoint_by_event_type = inverse_dict({k: v for k, v in endpoint_lookup.items() if "{" not in v})

    # error_code_by_platform_error_code = {
    #     # # "": ErrorCode.UNAUTHORIZED,
    #     # "Unknown symbol": ErrorCode.WRONG_SYMBOL,
    #     # # "ERR_RATE_LIMIT": ErrorCode.RATE_LIMIT,
    # }

    # For converting time
    is_source_in_timestring = True
    # timestamp_platform_names = []

    prev_order_book_data_by_subscription = None

    data = {}
    keys = {}

    def find_by_keys(self, keys, table, matchData):
        for item in table:
            if all(item[k] == matchData[k] for k in keys):
                return item

    def order_leaves_quantity(self, order):
        if order['leavesQty'] is None:
            return True
        return order['leavesQty'] > 0

    def handle_data_table(self, message):
        changed_items = []
        table = message["table"] if "table" in message else None
        action = message["action"] if "action" in message else None
        if "subscribe" in message:
            self.logger.debug("Subscribed to %s." % message["subscribe"])
        elif action:
            if table not in self.data:
                self.data[table] = []
            # There are four possible actions from the WS:
            # "partial" - full table image
            # "insert"  - new row
            # "update"  - update row
            # "delete"  - delete row
            if action == "partial":
                self.logger.debug("%s: partial" % table)
                self.data[table] += message["data"]
                # Keys are communicated on partials to let you know how to uniquely identify
                # an item. We use it for updates.
                self.keys[table] = message["keys"]
            elif action == "insert":
                self.logger.debug("%s: inserting %s" %
                                  (table, message["data"]))
                self.data[table] += message["data"]

                # Limit the max length of the table to avoid excessive memory usage.
                # Don't trim orders because we'll lose valuable state if we do.
                if table not in [
                        "order", "orderBookL2", "orderBookL2_25"
                ] and len(self.data[table]) > self.MAX_TABLE_LEN:
                    self.data[table] = self.data[table][int(self.
                                                            MAX_TABLE_LEN /
                                                            2):]
            elif action == 'update':
                self.logger.debug('%s: updating %s' % (table, message['data']))
                # Locate the item in the collection and update it.
                for updateData in message['data']:
                    item = self.find_by_keys(self.keys[table],
                                             self.data[table], updateData)
                    if not item:
                        return  # No item found to update. Could happen before push
                    item.update(updateData)
                    changed_items.append(item)
                    # Remove cancelled / filled orders
                    if table == 'order' and not self.order_leaves_quantity(
                            item):
                        self.data[table].remove(item)
            elif action == "delete":
                self.logger.debug("%s: deleting %s" % (table, message["data"]))
                # Locate the item in the collection and remove it.
                for deleteData in message["data"]:
                    item = self.find_by_keys(self.keys[table],
                                             self.data[table], deleteData)
                    self.data[table].remove(item)
            else:
                raise Exception("Unknown action: %s" % action)
        if changed_items:
            message['data'] = changed_items

        not_to_skip = ['Trade', 'Settlement', 'Funding']
        if table and action:
            for item in message['data']:
                if message['table'] == 'execution' and item[
                        'execType'] not in not_to_skip:
                    message['data'] = None
        return message

    def preprocess_data(self, data, subscription, endpoint, symbol, params):
        data = super().preprocess_data(data, subscription, endpoint, symbol,
                                       params)
        data = self.handle_data_table(data)
        if self.prev_order_book_data_by_subscription is None:
            self.prev_order_book_data_by_subscription = {}

        # Merge all changes with previous data and return full order books
        items_data = data.get("data")
        if items_data and endpoint in (Endpoint.ORDER_BOOK,
                                       Endpoint.ORDER_BOOK_DIFF):
            action = data.get("action")
            if action == "partial":
                self.prev_order_book_data_by_subscription[
                    subscription] = items_data
            else:
                prev_items_data = self.prev_order_book_data_by_subscription.get(
                    subscription)
                if prev_items_data is None:
                    self.logger.warning(
                        "It is impossible order book update cannot be before snapshot! "
                        "data: %s", data)
                    return None

                if action == "delete":
                    for item in items_data:
                        item["size"] = 0

                is_some_added = False
                for item in items_data:
                    id = item.get("id")
                    prev_item = next(
                        (pi for pi in prev_items_data if pi.get("id") == id),
                        None)
                    if prev_item:
                        # Apply changes
                        for k, v in item.items():
                            prev_item[k] = v
                    else:
                        # Insert item
                        prev_items_data.append(item)
                        is_some_added = True
                if is_some_added:
                    prev_items_data.sort(key=itemgetter("id"))

                # Convert full order book (action=="partial") for changed symbols
                data["action"] = "partial"
                # prevent sending non changed data
                symbols_changed = {i.get("symbol") for i in items_data}
                data["data"] = [
                    i for i in prev_items_data
                    if i.get("symbol") in symbols_changed
                ]

        return data

    def get_subscription_info(self, endpoint, data):
        subscription = data.get(
            self.subscription_param) if self.subscription_param else None
        if subscription in (Endpoint.QUOTE, ):
            endpoint = self.endpoint_by_event_type.get(subscription)
        return super().get_subscription_info(endpoint, data)

    # todo parse only for those symbols which user is subscribed on
    def parse(self, endpoint, data):
        # Skip unimportant messages
        if not data or "info" in data and "version" in data or "success" in data:
            return None

        action = data.get("action") if data else None
        if data:
            if "error" in data:
                result = self.parse_error(data)
                if "request" in data:
                    result.message += "request: " + json.dumps(data["request"])
                return result

            if not endpoint:
                endpoint = data.get(self.event_type_param)
            if endpoint in (Endpoint.ORDER_BOOK, ):
                if not action:
                    return None
                elif action != "partial":
                    endpoint = Endpoint.ORDER_BOOK_DIFF

            if "data" in data:
                data = data["data"]

        if data and endpoint in (Endpoint.ORDER_BOOK,
                                 Endpoint.ORDER_BOOK_DIFF):

            symbols = {i.get("symbol") for i in data}
            new_data = []
            for symbol in symbols:
                order_book_data = {
                    ParamName.SYMBOL:
                    symbol,
                    ParamName.ASKS: [
                        i for i in data
                        if i["symbol"] == symbol and i["side"] == "Sell"
                    ],
                    # sorted(key=itemgetter("price")),
                    ParamName.BIDS: [
                        i for i in data
                        if i["symbol"] == symbol and i["side"] == "Buy"
                    ],
                }
                new_data.append(order_book_data)
            data = new_data
        # elif data and endpoint in (Endpoint.QUOTE, ):
        #     symbols = {i.get("symbol") for i in data}
        #     for symbol in symbols:
        #         order_book_data = {
        #             ParamName.SYMBOL:
        #             symbol,
        #             ParamName.ASKS: [data[0]['askPrice']],
        #             # sorted(key=itemgetter("price")),
        #             ParamName.BIDS: [data[0]['bidPrice']],
        #         }
        #     data = [order_book_data]
        result = super().parse(endpoint, data)

        # if endpoint in (Endpoint.ORDER_BOOK, Endpoint.ORDER_BOOK_DIFF):
        #     result = list(result)
        # result = list(result)

        return result

    def _parse_item(self, endpoint, item_data):
        if endpoint == "pong":
            return None

        original_symbol = item_data.get("symbol")

        result = super()._parse_item(endpoint, item_data)

        if isinstance(result, MyTrade) and not result.subscription:
            result.subscription = self.endpoint_lookup.get(
                Endpoint.TRADE_MY).format(symbol=original_symbol)
        # For trade BitMEX doesn't return valid subscription, so we have to set it separately
        if isinstance(result, Trade) and not result.subscription:
            result.subscription = self.endpoint_lookup.get(
                Endpoint.TRADE).format(symbol=original_symbol)
        if endpoint in (Endpoint.QUOTE, ) and not result.subscription:
            result.subscription = self.endpoint_lookup.get(endpoint).format(
                symbol=original_symbol)

        # (For Trade)
        if hasattr(result, ParamName.SYMBOL) and result.symbol[0] == ".":
            # # ".ETHUSD" -> "ETHUSD"
            # result.symbol = result.symbol[1:]
            # https://www.bitmex.com/api/explorer/#!/Trade/Trade_get Please note
            # that indices (symbols starting with .) post trades at intervals to
            # the trade feed. These have a size of 0 and are used only to indicate
            # a changing price.
            return None

        # Convert direction
        if result and hasattr(result,
                              ParamName.DIRECTION) and not result.direction:
            result.direction = None
            if isinstance(result, Position) and result.amount:
                result.direction = Direction.BUY if result.amount > 0 else Direction.SELL
                if result.amount < 0:
                    result.amount = -result.amount

        # Skip not changed
        if isinstance(result, Ticker) and result.price is None:
            return None

        return result

    _convert_satoshi_to_xbt = BitMEXRESTConverterV1._convert_satoshi_to_xbt

    def _post_process_item(self, item, item_data=None):
        self._convert_satoshi_to_xbt(item, item_data)
        return super()._post_process_item(item, item_data)


class BitMEXWSClient(WSClient):
    platform_id = Platform.BITMEX
    version = "1"  # Default version

    ping_interval_sec = 5

    _converter_class_by_version = {
        "1": BitMEXWSConverterV1,
    }

    _subscription_limit_by_endpoint = {Endpoint.TRADE: 50}

    @property
    def url(self):
        self.is_subscribed_with_url = True
        params = {"subscribe": ",".join(self.current_subscriptions)}
        url, platform_params = self.converter.make_url_and_platform_params(
            params=params, is_join_get_params=True)
        return url

    @property
    def headers(self):
        result = super().headers or []
        # Return auth headers
        if self._api_key:
            self.logger.info("Authenticating with API Key.")
            # To auth to the WS using an API key, we generate
            # a signature of a nonce and the WS API endpoint.
            expire = generate_expires()
            result += [
                "api-expires: " + str(expire),
            ]
            if self._api_key and self._api_secret:
                signature = generate_signature("GET", "/realtime", "", expire,
                                               self._api_secret)
                result += [
                    "api-signature: " + signature,
                    "api-key: " + self._api_key,
                ]
        else:
            self.logger.info(
                "Not authenticating by headers because api_key is not set.")

        return result

    def _send_subscribe(self, subscriptions):
        self._send_command("subscribe", subscriptions)

    def _send_unsubscribe(self, subscriptions):
        self._send_command("unsubscribe", subscriptions)

    def _send_command(self, command, params=None):
        if params is None:
            params = []
        self._send({"op": command, "args": list(params)})

    def _send_ping(self):
        pass

    def on_item_received(self, item):
        if not item:
            return
        # if hasattr(item, "subscription") and self.current_subscriptions and item.subscription in self.current_subscriptions:
        # Note: Order book returns data for all symbols, so we have to filter them
        # symbols = self.symbols
        # (May be extracted to base class if one more platform needs it)
        endpoint = self.converter.endpoint_by_item_class.get(item.__class__)
        symbols = self.symbols_by_endpoint.get(endpoint)
        if endpoint == Endpoint.TICKER and Endpoint.TICKER_ALL in self.symbols_by_endpoint:
            symbols = None
        # if hasattr(item, "symbol"):
        #     item.symbol = item.symbol.replace('XBT', 'BTC')
        if not hasattr(item,
                       "symbol") or not symbols or item.symbol in symbols:
            super().on_item_received(item)
        # Balance have only part of symbol pair
        elif isinstance(item, Balance):
            super().on_item_received(item)

        # See https://www.bitmex.com/app/wsAPI
        self._prev_ping_timestamp = time.time()


def generate_expires(expires_after_s=3600):
    return int(round(time.time() + expires_after_s))


# todo test generate signature with params from https://www.bitmex.com/app/apiKeysUsage
def generate_signature(method, url, data, expires, api_secret):
    """
    Generates an API signature compatible with BitMEX..
    A signature is HMAC_SHA256(api_secret, method + path + expires + data), hex encoded.
    Verb must be uppercased, url is relative, expires must be an increasing 64-bit integer
    and the data, if present, must be JSON without whitespace between keys.

    For example, in pseudocode (and in real code below):
        method=POST
        url=/api/v1/order
        expires=1416993995705
        data={"symbol":"XBTZ14","quantity":1,"price":395.01}
        signature = HEX(HMAC_SHA256(api_secret, 'POST/api/v1/order1416993995705{"symbol":"XBTZ14","amount":1,"price":395.01}'))
    """

    if not isinstance(data, str):
        # # data = json.dumps(platform_params, separators=[",", ":"]) if platform_params else ""
        # data = json.dumps(platform_params) if platform_params else ""
        logger.debug("initial data: %s", data)
        data = urllib.parse.urlencode(
            data)  # if method.upper() != "GET" else ""
        logger.debug(" transformed data: %s", data)

    # Parse the url so we can remove the base and extract just the path.
    parsed_url = urllib.parse.urlparse(url)
    path = parsed_url.path
    if parsed_url.query:
        path = path + "?" + parsed_url.query
    if method.upper() == "GET" and data:
        # For GET requests data is used in query
        path += "&" + data if parsed_url.query else "?" + data
        data = ""

    # print "Computing HMAC: %s" % verb + path + str(expires) + data
    message = (method + path + str(expires) + data).encode("utf-8")

    signature = hmac.new(api_secret.encode("utf-8"),
                         message,
                         digestmod=hashlib.sha256).hexdigest()
    # TEMP (hide)
    logger.debug("\nGenerate signature: %s %s %s", api_secret.encode("utf-8"),
                 message, signature)
    return signature
