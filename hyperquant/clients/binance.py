import hashlib
import hmac
import itertools
import threading
from decimal import Decimal
from operator import itemgetter

import requests

from hyperquant.api import (CandleInterval, CurrencyPair, Direction,
                            OrderBookDepthLevel, OrderStatus, OrderType,
                            Platform, Sorting, OrderTimeInForce)
from hyperquant.clients import (Account, Balance, Candle, Endpoint, Error,
                                ErrorCode, ItemObject, MyTrade, Order,
                                OrderBook, OrderBookDiff, OrderBookItem,
                                ParamName, PrivatePlatformRESTClient,
                                Quote, RESTConverter,
                                Ticker, Trade, WSClient, WSConverter)


# REST
# TODO check getting trades history from_id=1
class BinanceRESTConverterV1(RESTConverter):
    # Main params:
    base_url = "https://api.binance.com/api/v{version}/"

    # Settings:
    is_delimiter_used = True

    # Converting info:
    # For converting to platform
    endpoint_lookup = {
        Endpoint.PING: "ping",
        Endpoint.SERVER_TIME: "time",
        Endpoint.SYMBOLS: "exchangeInfo",
        Endpoint.CURRENCY_PAIRS: "exchangeInfo",
        Endpoint.TRADE: "trades",
        Endpoint.TRADE_HISTORY: "historicalTrades",
        Endpoint.TRADE_MY: "myTrades",  # Private
        Endpoint.CANDLE: "klines",
        Endpoint.TICKER: "ticker/price",
        Endpoint.TICKER_ALL: "ticker/price",
        Endpoint.QUOTE: "ticker/bookTicker",
        Endpoint.ORDER_BOOK: "depth",
        # Private
        Endpoint.ACCOUNT: "account",
        Endpoint.BALANCE: "account",
        Endpoint.ORDER: "order",
        Endpoint.ORDER_CREATE: "order",
        Endpoint.ORDER_CANCEL: "order",
        Endpoint.ORDERS_OPEN: "openOrders",
        Endpoint.ORDERS_ALL: "allOrders",
        Endpoint.POSITION: "account",
        Endpoint.POSITION_CLOSE: "order",
    }
    param_name_lookup = {
        ParamName.ORDER_ID: "orderId",
        ParamName.SYMBOL: "symbol",
        ParamName.LIMIT: "limit",
        ParamName.IS_USE_MAX_LIMIT: None,
        # ParamName.SORTING: None,
        ParamName.INTERVAL: "interval",
        ParamName.DIRECTION: "side",
        ParamName.ORDER_TYPE: "type",
        ParamName.TIMESTAMP: "timestamp",
        ParamName.FROM_ITEM: "fromId",
        ParamName.TO_ITEM: None,
        ParamName.FROM_TIME: "startTime",
        ParamName.TO_TIME: "endTime",
        ParamName.AMOUNT: "quantity",
        ParamName.PRICE: "price",
        ParamName.PRICE_STOP: "stopPrice",
        ParamName.PRICE_LIMIT: "price",
        # -ParamName.ASKS: "asks",
        # ParamName.BIDS: "bids",
    }
    param_value_lookup = {
        # ParamName.SORTING: {
        #     Sorting.ASCENDING: None,
        #     Sorting.DESCENDING: None,
        # },
        Sorting.DEFAULT_SORTING:
        Sorting.ASCENDING,
        ParamName.INTERVAL: {
            CandleInterval.MIN_1: "1m",
            CandleInterval.MIN_3: "3m",
            CandleInterval.MIN_5: "5m",
            CandleInterval.MIN_15: "15m",
            CandleInterval.MIN_30: "30m",
            CandleInterval.HRS_1: "1h",
            CandleInterval.HRS_2: "2h",
            CandleInterval.HRS_4: "4h",
            CandleInterval.HRS_6: "6h",
            CandleInterval.HRS_8: "8h",
            CandleInterval.HRS_12: "12h",
            CandleInterval.DAY_1: "1d",
            CandleInterval.DAY_3: "3d",
            CandleInterval.WEEK_1: "1w",
            CandleInterval.MONTH_1: "1M",
        },

        # By properties:
        ParamName.DIRECTION: {
            Direction.SELL: "SELL",
            Direction.BUY: "BUY",
        },
        ParamName.ORDER_TYPE: {
            OrderType.LIMIT: "LIMIT",
            OrderType.MARKET: "MARKET",
            OrderType.STOP_MARKET: "STOP_LOSS",
            OrderType.STOP_LIMIT: "STOP_LOSS_LIMIT",
            OrderType.TAKE_PROFIT_MARKET: "TAKE_PROFIT",
            OrderType.TAKE_PROFIT_LIMIT: "TAKE_PROFIT_LIMIT",
        },
        ParamName.TIME_IN_FORCE: {
            OrderTimeInForce.GTC: "GTC",
            OrderTimeInForce.IOC: "IOC",
            OrderTimeInForce.FOK: "FOK",
        },
        ParamName.ORDER_STATUS: {
            OrderStatus.NEW: "NEW",
            OrderStatus.PARTIALLY_FILLED: "PARTIALLY_FILLED",
            OrderStatus.FILLED: "FILLED",
            # OrderStatus.PENDING_CANCEL: "PENDING_CANCEL",
            OrderStatus.CANCELED: "CANCELED",
            OrderStatus.REJECTED: "REJECTED",
            OrderStatus.EXPIRED: "EXPIRED",
        },
        ParamName.SYMBOL: {
            "BTCUSD": "BTCUSDT"
        },
    }

    max_limit_by_endpoint = {
        Endpoint.TRADE: 1000,
        Endpoint.TRADE_HISTORY: 1000,
        Endpoint.ORDER_BOOK: 1000,
        Endpoint.CANDLE: 1000,
    }

    # For parsing

    param_lookup_by_class = {
        # Error
        Error: {
            "code": "code",
            "msg": "message",
        },
        # Data
        Trade: {
            "time": ParamName.TIMESTAMP,
            "id": ParamName.ITEM_ID,
            "qty": ParamName.AMOUNT,
            "price": ParamName.PRICE,
            # "isBuyerMaker": "",  # https://github.com/ccxt/ccxt/issues/4618
            # "isBestMatch": "",
        },
        MyTrade: {
            "symbol": ParamName.SYMBOL,
            "time": ParamName.TIMESTAMP,
            "orderId": ParamName.ORDER_ID,
            "id": ParamName.ITEM_ID,
            "qty": ParamName.AMOUNT,
            "price": ParamName.PRICE,
            "isBuyer": ParamName.DIRECTION,
            "commission": ParamName.FEE,
            # "commissionAsset": ParamName.FEE_SYMBOL,
            # "": ParamName.REBATE,
        },
        Candle: [
            ParamName.TIMESTAMP,
            ParamName.PRICE_OPEN,
            ParamName.PRICE_HIGH,
            ParamName.PRICE_LOW,
            ParamName.PRICE_CLOSE,
            None,  # ParamName.AMOUNT,  # only volume present
            None,
            None,
            ParamName.TRADES_COUNT,
            # ParamName.INTERVAL,
        ],
        Ticker: {
            "symbol": ParamName.SYMBOL,
            "price": ParamName.PRICE,
        },
        Account: {
            "updateTime": ParamName.TIMESTAMP,
            # "balances": ParamName.BALANCES,
        },
        Balance: {
            "asset": ParamName.SYMBOL,
            "free": ParamName.AMOUNT_AVAILABLE,
            "locked": ParamName.AMOUNT_RESERVED,
        },
        CurrencyPair: {
            "minQty": ParamName.LOT_SIZE_MIN,
            "maxQty": ParamName.LOT_SIZE_MAX,
            "stepSize": ParamName.LOT_SIZE_STEP,
            "tickSize": ParamName.PRICE_STEP,
            "quoteAsset": ParamName.SYMBOL_QUOTE,
            "baseAsset": ParamName.SYMBOL_BASE,
            "symbol": ParamName.PLATFORM_SYMBOL_NAME,
            "minNotional": ParamName.MIN_NOTIONAL
        },
        Order: {
            "symbol": ParamName.SYMBOL,
            "transactTime": ParamName.TIMESTAMP,
            "time": ParamName.TIMESTAMP,  # check "time" or "updateTime"
            "updateTime": ParamName.TIMESTAMP,
            "orderId": ParamName.ITEM_ID,
            "clientOrderId": ParamName.USER_ORDER_ID,
            "type": ParamName.ORDER_TYPE,
            "origQty": ParamName.AMOUNT_ORIGINAL,
            "executedQty": ParamName.AMOUNT_EXECUTED,
            "price": ParamName.PRICE,
            "side": ParamName.DIRECTION,
            "status": ParamName.ORDER_STATUS,
        },
        Quote: {
            "symbol": ParamName.SYMBOL,
            "bidPrice": ParamName.BESTBID,
            "askPrice": ParamName.BESTASK,
        },
        OrderBook: {
            "lastUpdateId": ParamName.ITEM_ID,
            "bids": ParamName.BIDS,
            "asks": ParamName.ASKS,
        },
        OrderBookItem: [ParamName.PRICE, ParamName.AMOUNT],
    }

    error_code_by_platform_error_code = {
        -2014: ErrorCode.UNAUTHORIZED,
        -1121: ErrorCode.WRONG_SYMBOL,
        -1100: ErrorCode.WRONG_PARAM,
    }
    error_code_by_http_status = {
        429: ErrorCode.RATE_LIMIT,
        418: ErrorCode.IP_BAN,
        404: ErrorCode.WRONG_URL,
    }

    # For converting time
    is_source_in_milliseconds = True

    # timestamp_platform_names = [ParamName.TIMESTAMP]

    def __init__(self, platform_id=None, version=None):
        self.intervals_supported = self.param_value_lookup[
            ParamName.INTERVAL].keys()
        super().__init__(platform_id=platform_id, version=version)

    def _process_param_value(self, name, value):
        if name == ParamName.FROM_ITEM or name == ParamName.TO_ITEM:
            if isinstance(value, ItemObject):
                return value.item_id
        return super()._process_param_value(name, value)

    def _parse_item(self, endpoint, item_data):
        result = super()._parse_item(endpoint, item_data)

        # MyTrade specific direction param
        if result and isinstance(result, MyTrade) and hasattr(
                result, 'direction'):
            result.direction = Direction.BUY if result.direction is True else \
                (Direction.SELL if result.direction is False else None)

        return result

    def parse(self, endpoint, data):
        def expand_complex(data_in, data_out, path=None):
            if isinstance(data_in, dict):
                iterator = data_in.items()
            elif isinstance(data_in, (list, tuple)):
                iterator = enumerate(data_in)
            else:
                raise Exception("not complex type")
            for k, v in iterator:
                path = path or k
                if isinstance(v, (str, int)):
                    data_out[path] = v
                    path = None
                if isinstance(v, (tuple, list, dict)):
                    # path += "_%s_%s" % (k, type(v))
                    data_out[path] = expand_complex(v, data_out, path)

        if data:
            if endpoint == Endpoint.SERVER_TIME:
                timestamp_ms = data.get("serverTime")
                return timestamp_ms / 1000 if not self.use_milliseconds and timestamp_ms else timestamp_ms
            elif endpoint in [Endpoint.SYMBOLS, Endpoint.CURRENCY_PAIRS
                              ] and ParamName.SYMBOLS in data:
                exchange_info = data[ParamName.SYMBOLS]
                # (There are only 2 statuses: "TRADING" and "BREAK")
                # symbols = [item[ParamName.SYMBOL] for item in exchange_info if item["status"] == "TRADING"]
                symbols = []
                for item in exchange_info:
                    del item['filters'][
                        5]  # this leaf has duplicated and ==0 lot sizes
                    _item = {}
                    expand_complex(item, _item)
                    symbols.append(
                        super().parse(endpoint, _item) if endpoint ==
                        Endpoint.CURRENCY_PAIRS else _item[ParamName.SYMBOL])
                # if hasattr(self, "symbols"):
                #     del self.symbols
                return symbols
            elif endpoint == Endpoint.BALANCE:
                data = data.get("balances")
            elif endpoint == Endpoint.ORDER_BOOK:
                data = {
                    'asks': data['asks'],
                    'bids': data['bids'],
                }
        result = super().parse(endpoint, data)
        return result

    def preprocess_params(self, endpoint, params):
        if endpoint == Endpoint.ORDERS_OPEN and ParamName.IS_OPEN in params:
            params.pop(ParamName.IS_OPEN)
        if endpoint == Endpoint.ORDERS_ALL and ParamName.FROM_ITEM in params:
            params[ParamName.FROM_TIME] = params[ParamName.FROM_ITEM]
            params.pop(ParamName.FROM_ITEM)

        return super().preprocess_params(endpoint, params)

    def _generate_and_add_signature(self,
                                    method,
                                    url,
                                    endpoint,
                                    platform_params,
                                    headers,
                                    api_key,
                                    api_secret,
                                    passphrase=None):
        if not api_key or not api_secret:
            self.logger.error(
                "Empty api_key or api_secret. Cannot generate signature.")
            return None
        ordered_params_list = self._order_params(platform_params)
        # print("ordered_platform_params:", ordered_params_list)
        query_string = "&".join(
            ["{}={}".format(d[0], d[1]) for d in ordered_params_list])
        # print("query_string:", query_string)
        m = hmac.new(api_secret.encode("utf-8"), query_string.encode("utf-8"),
                     hashlib.sha256)
        signature = m.hexdigest()
        # Add
        # platform_params["signature"] = signature  # no need
        # if ordered_params_list and ordered_params_list[-1][0] != "signature":
        ordered_params_list.append(("signature", signature))
        return ordered_params_list, headers

    def _order_params(self, platform_params):
        # Convert params to sorted list with signature as last element.

        params_list = [(key, value) for key, value in platform_params.items()
                       if key != "signature"]
        # Sort parameters by key
        params_list.sort(key=itemgetter(0))
        # Append signature to the end if present
        if "signature" in platform_params:
            params_list.append(("signature", platform_params["signature"]))
        return params_list

    def post_process_result(self, result, method, endpoint, params):
        result = super().post_process_result(result, method, endpoint, params)
        if endpoint == Endpoint.ORDER_BOOK:
            for half in ('asks', 'bids'):
                for level in getattr(result, half):
                    level.symbol = params.get('symbol')
        return result


class BinanceRESTClient(PrivatePlatformRESTClient):
    # Settings:
    platform_id = Platform.BINANCE
    version = "1"  # Default version

    _converter_class_by_version = {
        "1": BinanceRESTConverterV1,
        "3":
        BinanceRESTConverterV1,  # Only for some methods (same converter used)
    }

    # State:
    ratelimit_error_in_row_count = 0
    wait_before_fetch_s = 1

    supported_order_types = (OrderType.MARKET, OrderType.LIMIT,
                             OrderType.STOP_LIMIT, OrderType.TAKE_PROFIT_LIMIT)

    def __init__(self,
                 api_key=None,
                 api_secret=None,
                 passphrase=None,
                 version=None,
                 credentials=None,
                 **kwargs) -> None:
        if any((api_key, api_secret, credentials)):
            if 'pivot_symbol' in kwargs:
                self.pivot_symbol = kwargs['pivot_symbol']
                kwargs.pop('pivot_symbol')
            else:
                raise Exception("pivot symbol is missing")
        super().__init__(api_key=api_key,
                         api_secret=api_secret,
                         passphrase=passphrase,
                         version=version,
                         credentials=credentials,
                         **kwargs)

    def fetch_candles(self,
                      symbol,
                      interval,
                      limit=None,
                      from_time=None,
                      to_time=None,
                      is_use_max_limit=False,
                      version=None,
                      **kwargs):
        return super().fetch_candles(symbol,
                                     interval,
                                     limit=limit,
                                     from_time=from_time,
                                     to_time=to_time,
                                     is_use_max_limit=is_use_max_limit,
                                     version=version or '1',
                                     **kwargs)

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
        return super().fetch_trades_history(symbol,
                                            limit=limit,
                                            from_item=from_item,
                                            to_item=to_item,
                                            sorting=sorting,
                                            is_use_max_limit=is_use_max_limit,
                                            from_time=from_time,
                                            to_time=to_time,
                                            version=version or '1',
                                            **kwargs)

    def fetch_trades(self, symbol, limit=None, version=None, **kwargs):
        return super().fetch_trades(symbol,
                                    limit=limit,
                                    version=version or '1',
                                    **kwargs)

    def get_server_timestamp(self,
                             force_from_server=False,
                             version=None,
                             **kwargs):
        return super().get_server_timestamp(
            force_from_server=force_from_server,
            version=version or '1',
            **kwargs)

    def ping(self, version=None, **kwargs):
        return super().ping(version=version or '1', **kwargs)

    @property
    def headers(self):
        result = super().headers
        result["X-MBX-APIKEY"] = self._api_key
        result["Content-Type"] = "application/x-www-form-urlencoded"
        return result

    def _on_response(self, response, result):
        # super()._on_response(response, result)

        self.delay_before_next_request_sec = 0
        if isinstance(result, Error):
            if result.code == ErrorCode.RATE_LIMIT:
                self.ratelimit_error_in_row_count += 1
                self.delay_before_next_request_sec = 60 * 2 * self.ratelimit_error_in_row_count  # some number - change
            elif result.code == ErrorCode.IP_BAN:
                self.ratelimit_error_in_row_count += 1
                self.delay_before_next_request_sec = 60 * 5 * self.ratelimit_error_in_row_count  # some number - change
            else:
                self.ratelimit_error_in_row_count = 0
        else:
            self.ratelimit_error_in_row_count = 0

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
        if from_item is None:
            from_item = 0
        return super().fetch_history(endpoint, symbol, limit, from_item,
                                     to_item, sorting, is_use_max_limit,
                                     from_time, to_time, **kwargs)

    def fetch_order_book(self,
                         symbol=None,
                         limit=None,
                         is_use_max_limit=False,
                         version=None,
                         **kwargs):
        LIMIT_VALUES = [5, 10, 20, 50, 100, 500, 1000]
        if limit not in LIMIT_VALUES:
            self.logger.warning("Limit value %s not in %s", limit,
                                LIMIT_VALUES)
            limit = 20
        return super().fetch_order_book(symbol,
                                        limit,
                                        is_use_max_limit,
                                        version="1",
                                        **kwargs)

    def fetch_ticker(self, symbol=None, version=None, **kwargs):
        return super().fetch_ticker(symbol=symbol,
                                    version=version or '3',
                                    **kwargs)

    def fetch_tickers(self, symbols=None, version=None, **kwargs):
        items = super().fetch_tickers(symbols, version or "3", **kwargs)

        # (Binance returns timestamp only for /api/v1/ticker/24hr which has weight of 40.
        # /api/v3/ticker/price - has weight 2.)
        timestamp = self.get_server_timestamp(version)
        for item in items:
            item.timestamp = timestamp
            item.use_milliseconds = self.use_milliseconds

        return items

    def get_account_info(self, version=None, **kwargs):
        return super().get_account_info(version or "3", **kwargs)

    def check_credentials(self, version=None, **kwargs):
        return super().check_credentials(version or "3", **kwargs)

    def fetch_balance(self, version=None, **kwargs):
        if 'symbol' in kwargs:
            kwargs.pop('symbol')
        return super().fetch_balance(version=version or '3', **kwargs)

    def fetch_my_trades(self,
                        symbol,
                        limit=None,
                        from_item=None,
                        version=None,
                        **kwargs):
        return super().fetch_my_trades(symbol, limit, from_item, version
                                       or "3", **kwargs)

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
        if OrderType.is_limit_family(order_type):
            # (About values:
            # https://www.reddit.com/r/BinanceExchange/comments/8odvs4/question_about_time_in_force_binance_api/)
            if not kwargs.get(ParamName.TIME_IN_FORCE):
                kwargs[ParamName.TIME_IN_FORCE] = OrderTimeInForce.GTC
        return super().create_order(symbol,
                                    order_type,
                                    direction,
                                    amount=amount,
                                    price=price,
                                    is_test=is_test,
                                    price_stop=price_stop,
                                    price_limit=price_limit,
                                    version=version or "3",
                                    **kwargs)

    def cancel_order(self, order, symbol=None, version=None, **kwargs):
        if hasattr(order, ParamName.SYMBOL) and order.symbol:
            symbol = order.symbol
        if order and not symbol:
            order_id = order if isinstance(order, str) else order.item_id
            orders = self.fetch_orders()
            symbol = [order for order in orders
                      if order.item_id == order_id][0].symbol
        return super().cancel_order(order, symbol, version or "3", **kwargs)

    def fetch_order(self, order_or_id, symbol=None, version=None, **kwargs):
        return super().fetch_order(order_or_id, symbol, version or "3",
                                   **kwargs)

    def fetch_orders(self,
                     symbol=None,
                     limit=None,
                     from_item=None,
                     is_open_only=False,
                     version=None,
                     **kwargs):
        if isinstance(from_item, Order):
            from_item = from_item.timestamp
        result = super().fetch_orders(symbol, limit, from_item, False, version
                                      or "3", **kwargs)
        if is_open_only and not isinstance(result, Error):
            result = [o for o in result if o.is_open]
        return result

    def get_positions(self, symbol=None, limit=None, version=None, **kwargs):
        return super().get_positions(symbol=symbol,
                                     limit=limit,
                                     version=version or '3',
                                     **kwargs)

    def _send(self, method, endpoint, params=None, version=None, **kwargs):
        if endpoint in self.converter.secured_endpoints:
            server_timestamp = self.get_server_timestamp()
            params[
                ParamName.
                TIMESTAMP] = server_timestamp if self.use_milliseconds else int(
                    server_timestamp * 1000)
        return super()._send(method, endpoint, params, version, **kwargs)


#  ____________________________  WebSocket ____________________________


class BinanceWSRestHelper(BinanceRESTClient):
    def __init__(self,
                 api_key=None,
                 api_secret=None,
                 passphrase=None,
                 version=None,
                 credentials=None,
                 **kwargs) -> None:
        super().__init__(api_key=api_key,
                         api_secret=api_secret,
                         passphrase=passphrase,
                         version=version,
                         credentials=credentials,
                         **kwargs)
        self.listen_key = None
        self.is_active = False

    @property
    def headers(self):
        result = super().headers
        result["X-MBX-APIKEY"] = self._api_key
        return result

    def get_listen_key(self):
        url = 'https://api.binance.com/api/v1/userDataStream'
        response = requests.post(url, headers=self.headers)
        data = response.json(parse_float=Decimal)
        self.active = True
        self._restart_reconnector()
        if 'listenKey' in data:
            self.listen_key = data['listenKey']
        else:
            self.logger.warning("Can't get listen key", data)

    def stream_keep_alive(self):
        params = {'listenKey': self.listen_key}
        url = 'https://api.binance.com/api/v1/userDataStream'
        requests.put(url, headers=self.headers, data=params)
        if self.is_active:
            self._restart_reconnector()

    def _restart_reconnector(self):
        self._user_timer = threading.Timer(30 * 60, self.stream_keep_alive)
        self._user_timer.setDaemon(True)
        self._user_timer.start()


class BinanceWSConverterV1(WSConverter):
    # Main params:
    base_url = "wss://stream.binance.com:9443/"

    IS_SUBSCRIPTION_COMMAND_SUPPORTED = False
    is_orderbook_snapshot_goes_first = False
    supported_endpoints = [
        Endpoint.TRADE, Endpoint.CANDLE, Endpoint.TICKER, Endpoint.TICKER_ALL,
        Endpoint.ORDER_BOOK, Endpoint.ORDER_BOOK_DIFF, Endpoint.BALANCE,
        Endpoint.POSITION, Endpoint.ORDER, Endpoint.QUOTE, Endpoint.TRADE_MY
    ]
    symbol_endpoints = supported_endpoints
    # supported_symbols = None

    # Settings:
    is_delimiter_used = True

    # Converting info:
    # For converting to platform

    endpoint_lookup = {
        Endpoint.TRADE: "{symbol}@trade",
        Endpoint.CANDLE: "{symbol}@kline_{interval}",
        Endpoint.TICKER: "{symbol}@miniTicker",
        Endpoint.TICKER_ALL: "!miniTicker@arr",
        Endpoint.ORDER_BOOK: "{symbol}@depth{level}",
        Endpoint.ORDER_BOOK_DIFF: "{symbol}@depth",
        Endpoint.QUOTE: "{symbol}@depth5",
        # Private endpoints below are virtual
        Endpoint.BALANCE: Endpoint.BALANCE,
        Endpoint.ORDER: Endpoint.ORDER,
        Endpoint.TRADE_MY: Endpoint.TRADE_MY,
    }

    # For parsing
    param_lookup_by_class = {
        # Error
        Error: {
            # "code": "code",
            # "msg": "message",
        },
        # Data
        Trade: {
            "s": ParamName.SYMBOL,
            "T": ParamName.TIMESTAMP,
            "t": ParamName.ITEM_ID,
            "p": ParamName.PRICE,
            "q": ParamName.AMOUNT,
            # "m": "",
        },
        Candle: {
            "s": ParamName.SYMBOL,
            "t": ParamName.TIMESTAMP,
            "i": ParamName.INTERVAL,
            "o": ParamName.PRICE_OPEN,
            "c": ParamName.PRICE_CLOSE,
            "h": ParamName.PRICE_HIGH,
            "l": ParamName.PRICE_LOW,
            "": ParamName.VOLUME,  # todo check
            "n": ParamName.TRADES_COUNT,
        },
        Ticker: {
            "s": ParamName.SYMBOL,
            "E": ParamName.TIMESTAMP,
            "c": ParamName.PRICE,  # todo check to know for sure
        },
        Quote: {
            "lastUpdateId": ParamName.ITEM_ID,
            # Diff. Depth Stream
            "s": ParamName.SYMBOL,
            "E": ParamName.TIMESTAMP,
            "u": ParamName.ITEM_ID,
        },
        OrderBook: {
            # Partial Book Depth Streams
            "lastUpdateId": ParamName.ITEM_ID,
            "asks": ParamName.ASKS,
            "bids": ParamName.BIDS,
            # Diff. Depth Stream
            "s": ParamName.SYMBOL,
            "E": ParamName.TIMESTAMP,
            "u": ParamName.ITEM_ID,
            "a": ParamName.ASKS,
            "b": ParamName.BIDS,
        },
        OrderBookDiff: {
            # Partial Book Depth Streams
            "lastUpdateId": ParamName.ITEM_ID,
            "asks": ParamName.ASKS,
            "bids": ParamName.BIDS,
            # Diff. Depth Stream
            "s": ParamName.SYMBOL,
            "E": ParamName.TIMESTAMP,
            "u": ParamName.ITEM_ID,
            "a": ParamName.ASKS,
            "b": ParamName.BIDS,
        },
        OrderBookItem: [ParamName.PRICE, ParamName.AMOUNT],
        # Private Data
        Balance: {
            "a": ParamName.SYMBOL,
            "f": ParamName.AMOUNT_AVAILABLE,
            "l": ParamName.AMOUNT_RESERVED,
        },
        Order: {
            "s": ParamName.SYMBOL,
            "T": ParamName.TIMESTAMP,
            "O": ParamName.TIMESTAMP,  # check "time" or "updateTime"
            "E": ParamName.TIMESTAMP,
            "i": ParamName.ITEM_ID,
            "c": ParamName.USER_ORDER_ID,
            "o": ParamName.ORDER_TYPE,
            "q": ParamName.AMOUNT_ORIGINAL,
            "z": ParamName.AMOUNT_EXECUTED,
            "p": ParamName.PRICE,
            "S": ParamName.DIRECTION,
            "X": ParamName.ORDER_STATUS,
        },
        MyTrade: {
            "s": ParamName.SYMBOL,
            "E": ParamName.TIMESTAMP,
            "t": ParamName.ITEM_ID,
            "l": ParamName.AMOUNT,
            "L": ParamName.PRICE,
            "S": ParamName.DIRECTION,
            "n": ParamName.FEE,
            "i": ParamName.ORDER_ID,
        },
    }

    param_value_lookup = BinanceRESTConverterV1.param_value_lookup.copy()
    param_value_lookup[ParamName.LEVEL] = {
        OrderBookDepthLevel.LIGHT: 5,
        OrderBookDepthLevel.MEDIUM: 10,
        OrderBookDepthLevel.DEEP: 20,
        OrderBookDepthLevel.DEEPEST: 20,
    }

    subscription_param = "stream"
    event_type_param = "e"
    endpoint_by_event_type = {
        "trade": Endpoint.TRADE,
        "kline": Endpoint.CANDLE,
        "24hrMiniTicker": Endpoint.TICKER,
        "24hrTicker": Endpoint.TICKER,
        # "depthUpdate": Endpoint.ORDER_BOOK,
        "depthUpdate": Endpoint.ORDER_BOOK_DIFF,
        "executionReport": Endpoint.ORDER,
        "outboundAccountInfo": Endpoint.BALANCE,
    }

    # https://github.com/binance-exchange/binance-official-api-docs/blob/master/errors.md
    error_code_by_platform_error_code = {
        # -2014: ErrorCode.UNAUTHORIZED,
        # -1121: ErrorCode.WRONG_SYMBOL,
        # -1100: ErrorCode.WRONG_PARAM,
    }
    error_code_by_http_status = {}

    # For converting time
    is_source_in_milliseconds = True

    # def _generate_subscription(self, endpoint, symbol=None, **params):
    #     return super()._generate_subscription(endpoint, symbol.lower() if symbol else symbol, **params)

    def _store_subscription(self, endpoints, symbols=None, **params):
        params = {
            k: v if isinstance(v, list) else [v]
            for k, v in params.items()
        }
        i_v = itertools.product(*list(params.values()))
        i_k = params.keys()
        _map = map(lambda a: dict(zip(i_k, a)), i_v)
        iterator = itertools.product(
            endpoints, symbols, _map) if symbols else (
                (e, None, p) for e, p in itertools.product(endpoints, _map))
        for endpoint, symbol, param in iterator:
            for subscription in self.generate_subscriptions([endpoint],
                                                            [symbol], **param):
                if not Endpoint.check_is_private(endpoint):
                    self.endpoint_symbol_params_by_subscription[
                        subscription] = (endpoint, symbol, param)
                else:
                    self.endpoint_symbol_params_by_subscription[
                        subscription] = (endpoint, symbol, {})

    def _generate_subscription(self, endpoint, symbol=None, **params):
        # ADD TO FIX THIS PROBLEM get_subscription_info self.platform_id == Platform.BINANCE and len(
        # self.endpoint_symbol_params_by_subscription) == 1
        params = {ParamName.SYMBOL: symbol, **params}
        subscription = self._get_platform_endpoint(endpoint, params)
        # Save to get endpoint and other params by subscription on parsing
        return subscription

    def post_process_result(self, result, method, endpoint, params):
        result = super().post_process_result(result, method, endpoint, params)
        if endpoint == Endpoint.ORDER_BOOK:
            for half in ('asks', 'bids'):
                for level in getattr(result, half):
                    level.symbol = params.get('symbol')
        return result

    def preprocess_data(self, data, subscription, endpoint, symbol, params):
        if params:
            params[ParamName.SYMBOL] = symbol
        if 'data' in data:
            data['data']['s'] = symbol
        return super().preprocess_data(data, subscription, endpoint, symbol,
                                       params)

    def parse(self, endpoint, data):
        if "data" in data:
            data = data["data"]
        if not endpoint and data and isinstance(
                data, dict) and self.event_type_param:
            endpoint = data.get(self.event_type_param, endpoint)

        endpoint = self.endpoint_by_event_type.get(endpoint, endpoint) \
            if self.endpoint_by_event_type else endpoint

        if endpoint == Endpoint.BALANCE or endpoint == Endpoint.POSITION:
            data = data['B']
        if endpoint == "outboundAccountPosition":
            return
        if endpoint == Endpoint.ORDER:
            endpoint = [Endpoint.ORDER, Endpoint.TRADE_MY]
        if isinstance(endpoint, list):
            endpoints = endpoint
            result = [
                super(WSConverter, self).parse(endpoint, data)
                for endpoint in endpoints
            ]
            result = [res for res in result if res]
        else:
            result = super(WSConverter, self).parse(endpoint, data)
        return result

    def _post_process_item(self, item, item_data=None):
        if isinstance(item, Quote):
            item.bestask = item_data['asks'][0][0]
            item.bestbid = item_data['bids'][0][0]
        if isinstance(item, MyTrade):
            if item.item_id == -1:
                return None
        return super()._post_process_item(item, item_data)

    def _parse_item(self, endpoint, item_data):
        if endpoint == Endpoint.CANDLE and "k" in item_data:
            item_data = item_data["k"]
        if endpoint == Endpoint.POSITION:
            return super()._parse_item(Endpoint.BALANCE, item_data)
        item = super()._parse_item(endpoint, item_data)
        # Hack to set subscription to created item, because subscription is virtual
        # and equal to endpoint name
        if item and Endpoint.check_is_private(endpoint):
            item.subscription = endpoint
        return item

    def _get_platform_param_value(self, value, name=None):
        value = super()._get_platform_param_value(value, name)
        if value and name == ParamName.SYMBOL:
            value = value.lower()
        return value

    def get_subscription_info(self, endpoint, data):
        # Get endpoint and other data by subscription name
        subscription = data.get(self.subscription_param)
        symbol, params = None, None
        if self.endpoint_symbol_params_by_subscription:
            prev_endpoint = endpoint

            # But it doesn't work by private subscription were we need to
            # distinguish different items from one channel

            # Note Binance not support simultaneously Private and Public subscriptions
            if 'e' in data and Endpoint.check_is_private(
                    list(self.endpoint_symbol_params_by_subscription.
                                 values())[0][0]):
                return None, None, None, None

            if len(self.endpoint_symbol_params_by_subscription) == 1:
                # If there is only one subscription to choose
                # (For Binance, if you subscribed only to one channel,
                # it returns messages without subscription (stream) name)
                subscription, (endpoint, symbol, params) = list(
                    self.endpoint_symbol_params_by_subscription.items())[0]
            else:
                if not subscription:
                    if isinstance(data, list):
                        event = data[0].get(self.event_type_param)
                    else:
                        event = data.get(self.event_type_param)
                    endpoint = self.endpoint_by_event_type.get(event)
                    if endpoint in [Endpoint.TRADE, Endpoint.CANDLE]:
                        params = {ParamName.SYMBOL: data.get('s')}
                        subscription = self._get_platform_endpoint(
                            endpoint, params)
                endpoint, symbol, params = self.endpoint_symbol_params_by_subscription.get(
                    subscription)

            if prev_endpoint and endpoint != prev_endpoint:
                self.logger.warning(
                    "Endpoint: %s changed to: %s for subscription: %s",
                    prev_endpoint, endpoint, subscription)
        return subscription, endpoint, symbol, params


class BinanceWSClient(WSClient):
    platform_id = Platform.BINANCE
    version = "1"  # Default version
    is_private = None

    _converter_class_by_version = {
        "1": BinanceWSConverterV1,
    }

    def __init__(self,
                 api_key=None,
                 api_secret=None,
                 version=None,
                 credentials=None,
                 **kwargs) -> None:
        self.rest_helper = None
        if any([api_key, api_secret, credentials]):
            if 'pivot_symbol' in kwargs:
                self.pivot_symbol = kwargs['pivot_symbol']
                kwargs.pop('pivot_symbol')
                self.rest_helper = BinanceWSRestHelper(
                    api_key,
                    api_secret,
                    credentials=credentials,
                    pivot_symbol=self.pivot_symbol)
                self.rest_helper.get_listen_key()
            else:
                raise Exception("pivot symbol is missing")
        super().__init__(api_key, api_secret, version, credentials, **kwargs)

    def _split_on_private_and_public(self, endpoints):
        private_endpoints = set()
        public_endpoints = []
        for endpoint in endpoints:
            if Endpoint.check_is_private(endpoint):
                private_endpoints.add(endpoint)
            else:
                public_endpoints.append(endpoint)
        return private_endpoints, public_endpoints

    def subscribe(self, endpoints=None, symbols=None, **params):
        """
        Небольшое отступление, чем подписка на приватные данные у бинанса, отличается от
        всех остальных подписок. При подписке мы не указываем конкретно на ЧТО мы подписываемся -
        мы отправляем только общий ключ, по которому нам будут приходить данные и которые затем
        мы распределяем на три эндпоинта - Balance, Position, Order.

        В current_subscriptions будет храниться одная общая подписка на приватные данные, но нам
        нужно еще где-то хранить на какие конечные эндпоинты подписался юзер и фильтровать лишние.
        Для этого мы будем пользоваться множеством user_private_subscriptions
        """
        private_endpoints, public_endpoints = self._split_on_private_and_public(
            endpoints)
        if private_endpoints:
            if self.is_private is False:
                raise Exception(
                    'Binance WS cant simultaneously subscribe to private and public endpoints'
                )
            self.is_private = True
            _, _, subscriptions = self.get_adding_subscriptions_for(
                endpoints, None, **params)
            self.converter._store_subscription(endpoints, None, **params)
            self.current_subscriptions.update(subscriptions)
            self.reconnect()
        if public_endpoints:
            if self.is_private is True:
                raise Exception(
                    'Binance WS cant simultaneously subscribe to private and public endpoints'
                )
            self.is_private = False
            super().subscribe(public_endpoints, symbols, **params)

    def unsubscribe(self, endpoints=None, symbols=None, **params):
        if endpoints:
            private_endpoints, public_endpoints = self._split_on_private_and_public(
                endpoints)
            if private_endpoints:
                _, _, subscriptions = self.get_removing_subscriptions_for(
                    endpoints, None, **params)
                self.current_subscriptions.difference_update(subscriptions)
                if not self.current_subscriptions:
                    self.rest_helper.is_active = False
                    self.reconnect()

            if public_endpoints:
                return super().unsubscribe(public_endpoints, symbols, **params)
        else:
            self.current_subscriptions = set()
            self.rest_helper.is_active = False
            self.reconnect()

    def _parse(self, endpoint, data):
        result = super()._parse(endpoint, data)
        if not result:
            return
        data_endpoint = result[0].endpoint if isinstance(
            result, list) else result.endpoint
        if Endpoint.check_is_private(data_endpoint):
            if isinstance(result, list):
                if data_endpoint == Endpoint.BALANCE:
                    if Endpoint.POSITION in self.current_subscriptions:
                        return self.make_positions_from_balance(result)
                for res in result:
                    if res.endpoint not in self.current_subscriptions:
                        result.remove(res)
                if result:
                    return result
            else:
                self.logger.exception(
                    'Unexpected behavior got not a list in private endpoint',
                    result)
        else:
            return result

    def make_positions_from_balance(self, balances):
        result = []
        for balance in balances:
            position = self.helper.create_position_if_exist(balance, self.pivot_symbol)
            if position:
                result.append(position)
        return result

    def _on_close(self):
        # If connection was disconnected maybe listen key was changed
        if self.rest_helper and self.is_started:
            self.rest_helper.get_listen_key()
        super()._on_close()

    @property
    def url(self):
        # Generate subscriptions
        if self.is_private:
            if self.rest_helper.listen_key:
                subscriptions = "ws/" + "".join(self.rest_helper.listen_key)
                return super().url + subscriptions
            else:
                self.logger.warning(
                    "Can't make subscription, there're no listen_key in client"
                )
        else:
            if not self.current_subscriptions:
                self.logger.warning(
                    "Making URL while current_subscriptions are empty. "
                    "There is no sense to connect without subscriptions.")
                subscriptions = ""
                # # There is no sense to connect without subscriptions
                # return None
            elif len(self.current_subscriptions) > 1:
                subscriptions = "stream?streams=" + "/".join(
                    self.current_subscriptions)
            else:
                subscriptions = "ws/" + "".join(self.current_subscriptions)

            self.is_subscribed_with_url = True
            return super().url + subscriptions

    # -
    # def subscribe(self, endpoints=None, symbols=None, **params):
    #     self._check_params(endpoints, symbols, **params)
    #
    #     super().subscribe(endpoints, symbols, **params)
    #
    # def unsubscribe(self, endpoints=None, symbols=None, **params):
    #     self._check_params(endpoints, symbols, **params)
    #
    #     super().unsubscribe(endpoints, symbols, **params)
    #
    # def _check_params(self, endpoints=None, symbols=None, **params):
    #     LEVELS_AVAILABLE = [5, 10, 20] + OrderBookDepthLevel.ALL
    #     if endpoints and Endpoint.ORDER_BOOK in endpoints and ParamName.LEVEL in params and \
    #             params.get(ParamName.LEVEL) not in LEVELS_AVAILABLE:
    #         self.logger.error("For %s endpoint %s param must be of values: %s, but set: %s",
    #                           Endpoint.ORDER_BOOK, ParamName.LEVEL, LEVELS_AVAILABLE,
    #                           params.get(ParamName.LEVEL))
