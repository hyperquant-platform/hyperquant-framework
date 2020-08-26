import time
from decimal import Decimal

from hyperquant.api import Platform, ParamName, ErrorCode, OrderStatus, OrderTimeInForce, OrderType, Direction, \
    CandleInterval, Sorting, CurrencyPair, OrderBookDepthLevel, TransactionType
from hyperquant.clients import Endpoint, OrderBookItem, OrderBook, Quote, Order, Balance, Account, Ticker, Candle, \
    MyTrade, Trade, Error, Position, WSConverter, BalanceTransaction, ItemObject
from hyperquant.clients.binance import BinanceRESTConverterV1, BinanceRESTClient, BinanceWSConverterV1, BinanceWSClient, \
    BinanceWSRestHelper


class BinanceFutureRESTConverterV1(BinanceRESTConverterV1):
    # Main params:
    base_url = "https://fapi.binance.com/fapi/v{version}/"

    # Settings:
    is_delimiter_used = True

    # Converting info:
    endpoint_lookup = {
        Endpoint.PING: "ping",
        Endpoint.SERVER_TIME: "time",
        Endpoint.SYMBOLS: "exchangeInfo",
        Endpoint.CURRENCY_PAIRS: "exchangeInfo",
        # We aren't using historicalTrades endpoint because WS doesn't support it
        # Also 'trades' endpoint disabled to prevent different data types for trades
        Endpoint.TRADE: "aggTrades",
        Endpoint.TRADE_HISTORY: "aggTrades",
        Endpoint.CANDLE: "klines",
        Endpoint.TICKER: "ticker/price",
        Endpoint.TICKER_ALL: "ticker/price",
        Endpoint.QUOTE: "ticker/bookTicker",
        Endpoint.ORDER_BOOK: "depth",
        # # Private
        Endpoint.ACCOUNT: "account",
        Endpoint.BALANCE: "account",
        Endpoint.ORDER: "order",
        Endpoint.ORDER_CREATE: "order",
        Endpoint.ORDER_CANCEL: "order",
        Endpoint.ORDERS_OPEN: "openOrders",
        Endpoint.ORDERS_ALL: "allOrders",
        Endpoint.POSITION: "positionRisk",
        Endpoint.POSITION_CLOSE: "order",
        Endpoint.TRADE_MY: "userTrades",
        Endpoint.LEVERAGE_SET: "leverage",
        Endpoint.BALANCE_TRANSACTION: "income",
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
            OrderType.STOP_LIMIT: "STOP",
            OrderType.TAKE_PROFIT_LIMIT: "TAKE_PROFIT",
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
        ParamName.LEVEL: {
            OrderBookDepthLevel.LIGHT: 20,
            OrderBookDepthLevel.MEDIUM: 100,
            OrderBookDepthLevel.DEEP: 500,
            OrderBookDepthLevel.DEEPEST: 1000,
        },
    }

    param_lookup_by_class = {
        # Error
        Error: {
            "code": "code",
            "msg": "message",
        },
        # Data
        Trade: {
            "T": ParamName.TIMESTAMP,
            "a": ParamName.ITEM_ID,
            "q": ParamName.AMOUNT,
            "p": ParamName.PRICE,
        },
        MyTrade: {
            "symbol": ParamName.SYMBOL,
            "time": ParamName.TIMESTAMP,
            "orderId": ParamName.ORDER_ID,
            "id": ParamName.ITEM_ID,
            "qty": ParamName.AMOUNT,
            "price": ParamName.PRICE,
            "side": ParamName.DIRECTION,
            "commission": ParamName.FEE,
            "commissionAsset": ParamName.FEE_CURRENCY,
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
            "maxWithdrawAmount": ParamName.AMOUNT_AVAILABLE,
            "marginBalance": ParamName.MARGIN_BALANCE,
            "unrealizedProfit": ParamName.PNL,
            # "walletBalance": , # Parse manually in post_process method
        },
        BalanceTransaction: {
            "asset": ParamName.SYMBOL,
            "symbol": ParamName.CURRENCY_PAIR,
            "time": ParamName.TIMESTAMP,
            "incomeType": ParamName.TRANSACTION_TYPE,
            "income": ParamName.AMOUNT,
        },
        CurrencyPair: {
            "minQty": ParamName.LOT_SIZE_MIN,
            "maxQty": ParamName.LOT_SIZE_MAX,
            "stepSize": ParamName.LOT_SIZE_STEP,
            "tickSize": ParamName.PRICE_STEP,
            "quoteAsset": ParamName.SYMBOL_QUOTE,
            "baseAsset": ParamName.SYMBOL_BASE,
            "symbol": ParamName.PLATFORM_SYMBOL_NAME,
            # "minQty": ParamName.MIN_NOTIONAL # No min notional parameter on future market
        },
        Order: {
            "symbol": ParamName.SYMBOL,
            "transactTime": ParamName.TIMESTAMP,
            # "time": ParamName.TIMESTAMP,
            # Rest API Order lookups now return updateTime which represents
            # the last time the order was updated; time is the order creation time.

            # But time parameter returned only by get orders request and not returned
            # by other requests like creation or getting open_orders
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
        Position: {
            "symbol": ParamName.SYMBOL,
            "positionAmt": ParamName.AMOUNT,
            "liquidationPrice": ParamName.PRICE_MARGIN_CALL,
            "entryPrice": ParamName.PRICE_AVERAGE,
            "unRealizedProfit": ParamName.PNL,
        },
        OrderBookItem: [ParamName.PRICE, ParamName.AMOUNT],
    }

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
                    _item = {}
                    expand_complex(item, _item)
                    symbols.append(
                        super().parse(endpoint, _item) if endpoint ==
                                                          Endpoint.CURRENCY_PAIRS else _item[ParamName.SYMBOL])
                # if hasattr(self, "symbols"):
                #     del self.symbols
                return symbols
            elif endpoint == Endpoint.ORDER_BOOK:
                data = {
                    'asks': data['asks'],
                    'bids': data['bids'],
                }
            elif endpoint == Endpoint.BALANCE:
                data = data['assets']
            elif endpoint == Endpoint.ORDERS_ALL_CANCEL:
                # This endpoint returns info message that cancel is successful
                data = []
        result = super(BinanceRESTConverterV1, self).parse(endpoint, data)
        return result

    error_code_by_platform_error_code = {
        -2014: ErrorCode.UNAUTHORIZED,
        -1121: ErrorCode.WRONG_SYMBOL,
        -1100: ErrorCode.WRONG_PARAM,
        -1130: ErrorCode.WRONG_LIMIT,
    }

    def post_process_result(self, result, method, endpoint, params):
        result = super().post_process_result(result, method, endpoint, params)
        if endpoint == Endpoint.ORDER_BOOK:
            for half in ('asks', 'bids'):
                for level in getattr(result, half):
                    level.symbol = params.get('symbol')
        if endpoint == Endpoint.POSITION:
            current_timestamp = time.time()
            if self.use_milliseconds:
                current_timestamp *= 1000
            for position in result:
                if position.amount < 0:
                    position.direction = Direction.SELL
                    position.amount = position.amount * (-1)
                elif position.amount > 0:
                    position.direction = Direction.BUY
                position.timestamp = current_timestamp

            symbol = params.get(ParamName.SYMBOL)
            if symbol:
                result = [p for p in result if p.symbol == symbol]

        if endpoint == Endpoint.BALANCE_TRANSACTION:
            if isinstance(result, list):
                result = list(sorted(result, key=lambda x: x.timestamp, reverse=True))
        return result

    def _parse_my_trade_direction(self, item):
        # Skip Binance specific parse
        pass

    def _parse_transaction_type(self, raw_transaction_type):
        if raw_transaction_type in ["TRANSFER", "WELCOME_BONUS", "CROSS_COLLATERAL_TRANSFER", "INSURANCE_CLEAR"]:
            return TransactionType.TRANSFER
        if raw_transaction_type == "FUNDING_FEE":
            return TransactionType.FUNDING
        if raw_transaction_type in ["REFERRAL_KICKBACK", "COMMISSION_REBATE"]:
            return TransactionType.REFERRAL_PAYOUT
        return TransactionType.REALISED_PNL

    def _post_process_item(self, item, item_data=None):
        if (isinstance(item, Balance) and item
                and item.amount_available is not None and item_data):
            total_amount = item_data.get("walletBalance")
            if total_amount is not None:
                item.amount_reserved = Decimal(total_amount) - item.amount_available

        item = super()._post_process_item(item, item_data)

        if isinstance(item, BalanceTransaction):
            item.transaction_type = self._parse_transaction_type(item.transaction_type)

        return item


class BinanceFutureRESTClient(BinanceRESTClient):

    platform_id = Platform.BINANCE_FUTURE
    version = "1"  # Default version
    is_futures = True
    is_position_supported = True
    is_balance_transactions_supported = True

    _converter_class_by_version = {
        "1": BinanceFutureRESTConverterV1,
    }

    def _send(self, method, endpoint, params=None, version=None, **kwargs):
        version = "1"
        # if params and 'limit' in params and params['limit'] and endpoint==Endpoint.TRADE_HISTORY:
        #     # Bug in Binance Future API it restrict smaller at 1 than requested
        #     if params['limit'] < 1000:
        #         params['limit'] += 1
        return super()._send(method, endpoint, params, version, **kwargs)

    def close_position(self, position_or_symbol=None, version=None, **kwargs):
        if isinstance(position_or_symbol, Position):
            position_or_symbol = position_or_symbol.symbol
        result = self.get_positions(position_or_symbol)
        if isinstance(result, list):
            for position in result:
                if not position_or_symbol or position.symbol == position_or_symbol:
                    if position.is_open:
                        result = self._close_position(position, version, **kwargs)
                        if isinstance(result, Error):
                            return result
                        result = self.get_positions(position.symbol)
                        if isinstance(result, Error):
                            return result
                        if isinstance(result, list):
                            if len(result) == 1:
                                return result[0]
                            else:
                                raise IndexError(f'Was returned more then 1 Position {result}')
                    else:
                        return position

    def _close_position(self, position, version=None, **kwargs):
        if position.direction == Direction.BUY:
            direction = Direction.SELL
        elif position.direction == Direction.SELL:
            direction = Direction.BUY
        else:
            raise ValueError(f'Position has wrong direction value {position.direction}')
        return self.create_order(position.symbol, OrderType.MARKET, direction, position.amount, 0)

    def close_all_positions(self, symbol=None, version=None, **kwargs):
        result = self.get_positions(symbol)
        if isinstance(result, list):
            for position in result:
                if not symbol or position.symbol == symbol:
                    if position.is_open:
                        result = self._close_position(position, version, **kwargs)
                        if isinstance(result, Error):
                            return result
        return self.get_positions()

    def fetch_balance_transactions(
        self, limit=None, page=None, from_time=None, to_time=None, is_direct=False, version=None, **kwargs
    ):
        if from_time is None and to_time is None:
            # Zero will fetch latest transactions, None will return empty list due bug in API
            from_time = 0
        return super().fetch_balance_transactions(
            limit=limit, page=page, from_time=from_time, to_time=to_time, is_direct=is_direct, version=version,
            **kwargs,
        )


class BinanceFutureWSRestHelper(BinanceWSRestHelper):
    base_url = "https://fapi.binance.com/fapi/v1/listenKey"


class BinanceFutureWSConverterV1(BinanceWSConverterV1):
    base_url = "wss://fstream.binance.com/"

    endpoint_lookup = {
        Endpoint.TRADE: "{symbol}@aggTrade",
        Endpoint.CANDLE: "{symbol}@kline_{interval}",
        Endpoint.TICKER: "{symbol}@miniTicker",
        # Endpoint.TICKER_ALL: "!miniTicker@arr",
        Endpoint.ORDER_BOOK: "{symbol}@depth{level}",
        Endpoint.ORDER_BOOK_DIFF: "{symbol}@depth",
        # Endpoint.QUOTE: "{symbol}@depth5",
        # Private endpoints below are virtual
        Endpoint.BALANCE: Endpoint.BALANCE,
        Endpoint.ORDER: Endpoint.ORDER,
        Endpoint.TRADE_MY: Endpoint.TRADE_MY,
    }

    param_value_lookup = BinanceFutureRESTConverterV1.param_value_lookup.copy()
    param_lookup_by_class = BinanceWSConverterV1.param_lookup_by_class.copy()

    param_lookup_by_class[Trade] = {
        "s": ParamName.SYMBOL,
        "T": ParamName.TIMESTAMP,
        # "f": ParamName.ITEM_ID,
        "l": ParamName.ITEM_ID,
        "p": ParamName.PRICE,
        "q": ParamName.AMOUNT,
    }

    param_lookup_by_class[Balance] = {
        "a": ParamName.SYMBOL,
        "wb": ParamName.AMOUNT_AVAILABLE,
    }

    param_lookup_by_class[Position] = {
        "s": ParamName.SYMBOL,
        "pa": ParamName.AMOUNT,
        "ep": ParamName.PRICE_AVERAGE,
        "up": ParamName.PNL,
    }

    param_lookup_by_class[MyTrade] = {
        "s": ParamName.SYMBOL,
        "T": ParamName.TIMESTAMP,
        "t": ParamName.ITEM_ID,
        "l": ParamName.AMOUNT,
        "L": ParamName.PRICE,
        "S": ParamName.DIRECTION,
        "n": ParamName.FEE,
        "i": ParamName.ORDER_ID,
    }

    endpoint_by_event_type = {
        "trade": Endpoint.TRADE,
        "kline": Endpoint.CANDLE,
        "24hrMiniTicker": Endpoint.TICKER,
        "24hrTicker": Endpoint.TICKER,
        # "depthUpdate": Endpoint.ORDER_BOOK,
        "depthUpdate": Endpoint.ORDER_BOOK_DIFF,
        "ORDER_TRADE_UPDATE": Endpoint.ORDER,
        "ACCOUNT_UPDATE": Endpoint.BALANCE,
    }

    def parse(self, endpoint, data):
        if "data" in data:
            data = data["data"]
        if not endpoint and data and isinstance(
                data, dict) and self.event_type_param:
            endpoint = data.get(self.event_type_param, endpoint)

        endpoint = self.endpoint_by_event_type.get(endpoint, endpoint) \
            if self.endpoint_by_event_type else endpoint

        if endpoint == Endpoint.BALANCE:
            result = super(WSConverter, self).parse(Endpoint.BALANCE, data['a']['B'])
            result += super(WSConverter, self).parse(Endpoint.POSITION, data['a']['P'])
        else:
            if endpoint == "outboundAccountPosition":
                return
            if endpoint == Endpoint.ORDER:
                data = data['o']
                if data['x'] != 'NEW':
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

    def _parse_item(self, endpoint, item_data):
        if endpoint == Endpoint.CANDLE and "k" in item_data:
            item_data = item_data["k"]
        item = super(BinanceWSConverterV1, self)._parse_item(endpoint, item_data)
        # Hack to set subscription to created item, because subscription is virtual
        # and equal to endpoint name
        if item and Endpoint.check_is_private(endpoint):
            item.subscription = endpoint
        return item

    def _post_process_item(self, item, item_data=None):
        result = super()._post_process_item(item, item_data=item_data)
        if isinstance(result, Position):
            current_timestamp = time.time()
            if self.use_milliseconds:
                current_timestamp *= 1000
            result.timestamp = current_timestamp
        return result


class BinanceFutureWSClient(BinanceWSClient):
    platform_id = Platform.BINANCE_FUTURE
    version = "1"  # Default version

    _converter_class_by_version = {
        "1": BinanceFutureWSConverterV1,
    }

    helper_class = BinanceFutureWSRestHelper

    def _parse(self, endpoint, data):
        result = super(BinanceWSClient, self)._parse(endpoint, data)
        if not result:
            return
        # If result was not parsed correctly, there will be raw message,
        # so we check for 'endpoint' attribute
        if isinstance(result, list):
            data_endpoint = getattr(result[0], 'endpoint', None)
        else:
            data_endpoint = getattr(result, 'endpoint', None)
        if Endpoint.check_is_private(data_endpoint):
            if isinstance(result, list):
                for res in list(result):
                    if res.endpoint not in self.current_subscriptions:
                        result.remove(res)
                if result:
                    return result
            else:
                if result.endpoint in self.current_subscriptions:
                    return result
        else:
            return result
