import itertools
import json
import logging
import time
from datetime import datetime, timezone
from decimal import Decimal
from unittest import TestCase

from unittest.mock import Mock
from websocket import ABNF

from hyperquant.api import (
    CandleInterval,
    CurrencyPair,
    Direction,
    OrderBookDepthLevel,
    OrderBookDirection,
    OrderStatus,
    OrderType,
    Platform,
    Sorting,
    TransactionType,
)
from hyperquant.clients import (
    Candle,
    DataObject,
    Endpoint,
    Error,
    ErrorCode,
    ItemObject,
    Order,
    OrderBook,
    OrderBookItem,
    ParamName,
    ProtocolConverter,
    RESTConverter,
    Trade,
    WSConverter,
    SingleDataAggregator)
from hyperquant.clients.tests.utils import (
    create_test_order,
    delete_all_test_orders,
    set_up_logging,
)
from hyperquant.clients.utils import create_rest_client, create_ws_client
from hyperquant.utils import time_util
from hyperquant.utils.test_util import APITestCase


set_up_logging()


class TestCandle(TestCase):
    def test_eq(self):
        candle1 = Candle(timestamp=1500000)
        candle2 = Candle(timestamp=1500000)
        candle3 = Candle(timestamp=1500001)

        self.assertEqual(candle1, candle2)
        self.assertNotEqual(candle1, candle3)

    def test_sorted(self):
        base_dt = datetime(
            year=2019, month=5, day=1, hour=0, minute=0, tzinfo=timezone.utc
        )

        # Normal
        candles = [
            Candle(
                interval=CandleInterval.MIN_5, timestamp=base_dt.replace().timestamp()
            ),
            Candle(
                interval=CandleInterval.MIN_5,
                timestamp=base_dt.replace(minute=10).timestamp(),
            ),
            Candle(
                interval=CandleInterval.MIN_1, timestamp=base_dt.replace().timestamp()
            ),
            Candle(
                interval=CandleInterval.MIN_5,
                timestamp=base_dt.replace(minute=5).timestamp(),
            ),
            Candle(
                interval=CandleInterval.MIN_5,
                timestamp=base_dt.replace(minute=15).timestamp(),
            ),
            Candle(
                interval=CandleInterval.MIN_1,
                timestamp=base_dt.replace(minute=3).timestamp(),
            ),
            Candle(
                interval=CandleInterval.MIN_1,
                timestamp=base_dt.replace(minute=2).timestamp(),
            ),
            Candle(
                interval=CandleInterval.MIN_15,
                timestamp=base_dt.replace(minute=15).timestamp(),
            ),
            Candle(
                interval=CandleInterval.MIN_15, timestamp=base_dt.replace().timestamp()
            ),
            Candle(
                interval=CandleInterval.HRS_1, timestamp=base_dt.replace().timestamp()
            ),
            Candle(
                interval=CandleInterval.MONTH_1, timestamp=base_dt.replace().timestamp()
            ),
            Candle(
                interval=CandleInterval.WEEK_1,
                timestamp=base_dt.replace(day=6).timestamp(),
            ),
            Candle(
                interval=CandleInterval.DAY_1, timestamp=base_dt.replace().timestamp()
            ),
        ]
        expected = [
            Candle(
                interval=CandleInterval.MIN_1, timestamp=base_dt.replace().timestamp()
            ),
            Candle(
                interval=CandleInterval.MIN_1,
                timestamp=base_dt.replace(minute=2).timestamp(),
            ),
            Candle(
                interval=CandleInterval.MIN_1,
                timestamp=base_dt.replace(minute=3).timestamp(),
            ),
            Candle(
                interval=CandleInterval.MIN_5, timestamp=base_dt.replace().timestamp()
            ),
            Candle(
                interval=CandleInterval.MIN_5,
                timestamp=base_dt.replace(minute=5).timestamp(),
            ),
            Candle(
                interval=CandleInterval.MIN_5,
                timestamp=base_dt.replace(minute=10).timestamp(),
            ),
            Candle(
                interval=CandleInterval.MIN_15, timestamp=base_dt.replace().timestamp()
            ),
            Candle(
                interval=CandleInterval.MIN_5,
                timestamp=base_dt.replace(minute=15).timestamp(),
            ),
            Candle(
                interval=CandleInterval.MIN_15,
                timestamp=base_dt.replace(minute=15).timestamp(),
            ),
            Candle(
                interval=CandleInterval.HRS_1, timestamp=base_dt.replace().timestamp()
            ),
            Candle(
                interval=CandleInterval.DAY_1, timestamp=base_dt.replace().timestamp()
            ),
            Candle(
                interval=CandleInterval.WEEK_1,
                timestamp=base_dt.replace(day=6).timestamp(),
            ),
            Candle(
                interval=CandleInterval.MONTH_1, timestamp=base_dt.replace().timestamp()
            ),
        ]

        # (timestamp only)
        result = Candle.sorted(candles)
        self.assertEqual(result, expected)

        # (timestamp + timestamp_close)
        for c in candles:
            c.timestamp_close = (
                c.timestamp + CandleInterval.convert_to_minutes(c.interval) * 60
            )
        result = Candle.sorted(candles)
        self.assertEqual(result, expected)

        # Empty
        candles = []
        expected = []

        result = Candle.sorted(candles)
        self.assertEqual(result, expected)

        candles = None
        expected = None

        result = Candle.sorted(candles)
        self.assertEqual(result, expected)


class TestOrderBook(TestCase):
    order_book_obj = OrderBook(
        1,
        "BTCUSD",
        15000001,
        "12345",
        False,
        asks=[
            OrderBookItem(amount=100, price=12, direction=OrderBookDirection.ASK),
            OrderBookItem(amount=150, price=14, direction=OrderBookDirection.ASK),
        ],
        bids=[
            OrderBookItem(amount=200, price=22, direction=OrderBookDirection.BID),
            OrderBookItem(amount=250, price=24, direction=OrderBookDirection.BID),
        ],
    )
    order_book_list = [
        1,
        "BTCUSD",
        15000001,
        "12345",
        [[100, 12, OrderBookDirection.ASK], [150, 14, OrderBookDirection.ASK]],
        [[200, 22, OrderBookDirection.BID], [250, 24, OrderBookDirection.BID]],
    ]
    order_book_dict = {
        ParamName.PLATFORM_ID: 1,
        ParamName.SYMBOL: "BTCUSD",
        ParamName.TIMESTAMP: 15000001,
        ParamName.ITEM_ID: "12345",
        ParamName.ASKS: [
            {
                ParamName.AMOUNT: 100,
                ParamName.PRICE: 12,
                ParamName.DIRECTION: OrderBookDirection.ASK,
            },
            {
                ParamName.AMOUNT: 150,
                ParamName.PRICE: 14,
                ParamName.DIRECTION: OrderBookDirection.ASK,
            },
        ],
        ParamName.BIDS: [
            {
                ParamName.AMOUNT: 200,
                ParamName.PRICE: 22,
                ParamName.DIRECTION: OrderBookDirection.BID,
            },
            {
                ParamName.AMOUNT: 250,
                ParamName.PRICE: 24,
                ParamName.DIRECTION: OrderBookDirection.BID,
            },
        ],
    }

    def test_from_json(self):
        for data in [
            self.order_book_list,
            self.order_book_dict,
            json.dumps(self.order_book_list),
            json.dumps(self.order_book_dict),
        ]:
            order_book = OrderBook()
            order_book.from_json(data)
            self.assertEqual(order_book, self.order_book_obj)

    def test_to_json(self):
        self.assertEqual(self.order_book_obj.to_json(), self.order_book_dict)
        self.assertEqual(self.order_book_obj.to_json(True), self.order_book_list)
        self.assertEqual(
            self.order_book_obj.to_json(False, True), json.dumps(self.order_book_dict)
        )
        self.assertEqual(
            self.order_book_obj.to_json(True, True), json.dumps(self.order_book_list)
        )


# Converter


class TestProtocolConverter(TestCase):
    converter_class = ProtocolConverter

    converter = None

    def setUp(self):
        super().setUp()
        self.converter = self.converter_class()

    # def test_(self):
    #     pass


# Common client


class TestClient(TestCase):
    is_rest = None
    platform_id = None
    version = None

    is_sorting_supported = False
    pivot_symbol = "BTC"
    testing_symbol = "EOSETH"
    testing_order_symbol = "EOSETH"
    testing_symbol2 = "bnbbtc"
    testing_symbols = ["EOSETH", "BNBBTC"]
    # TODO Add tests for: (todo make common behavior for all clients)
    testing_symbol_none = None
    testing_symbol_empty = ""
    # testing_symbol_wrong = ""
    wrong_symbol = "XXXYYY"  # todo rename to testing_symbol_wrong
    testing_symbol_not_existing = "ETHEOS"
    testing_symbols_none = None  # means all
    testing_symbols_empty = []
    testing_symbols_empty2 = [""]
    testing_symbols_with_wrong = testing_symbols + [wrong_symbol]
    testing_symbols_with_not_existing = testing_symbols + [wrong_symbol]

    testing_interval = CandleInterval.DAY_1

    client = None
    client_authed = None

    # To prevent rate limit
    sleep_between_tests_sec = 0

    def setUp(self):
        self.skipIfBase()
        super().setUp()

        if self.is_rest:
            self.client = create_rest_client(self.platform_id, version=self.version)
            self.client_authed = create_rest_client(
                self.platform_id, True, self.version, pivot_symbol=self.pivot_symbol
            )
        else:
            self.client = create_ws_client(self.platform_id, version=self.version)
            self.client_authed = create_ws_client(
                self.platform_id, True, self.version, pivot_symbol=self.pivot_symbol
            )

        time.sleep(self.sleep_between_tests_sec)

    def tearDown(self):
        self.client.close()
        super().tearDown()

    def skipIfBase(self):
        if self.platform_id is None:
            self.skipTest("Skip base class")

    # Utility

    def _result_info(self, result, sorting):
        is_asc_sorting = sorting == Sorting.ASCENDING
        items_info = "%s first: %s last: %s sort-ok: %s " % (
            "ASC" if is_asc_sorting else "DESC",
            time_util.get_timestamp_iso(result[0]),
            time_util.get_timestamp_iso(result[-1]),
            (
                result[0].timestamp < result[-1].timestamp
                if is_asc_sorting
                else result[0].timestamp > result[-1].timestamp
            )
            if result
            else "-",
        )
        return items_info + "count: %s" % (len(result) if result else "-")

    def assertRightSymbols(self, items):
        if self.testing_symbol:
            for item in items:
                # was: item.symbol = self.testing_symbol
                self.assertEqual(item.symbol, item.symbol.upper())
                self.assertEqual(item.symbol, self.testing_symbol)
        else:
            # For Trades in BitMEX
            symbols = set([item.symbol for item in items])
            self.assertGreater(len(symbols), 1)
            # self.assertGreater(len(symbols), 10)

    # (Assert items)

    def assertItemIsValid(
        self, trade, testing_symbol_or_symbols=None, is_with_timestamp=True
    ):
        if not testing_symbol_or_symbols:
            testing_symbol_or_symbols = self.testing_symbol

        APITestCase.assertItemIsValid(
            self,
            trade,
            testing_symbol_or_symbols,
            self.platform_id,
            is_with_timestamp=is_with_timestamp,
        )

    def assertTradeIsValid(self, trade, testing_symbol_or_symbols=None):
        if not testing_symbol_or_symbols:
            testing_symbol_or_symbols = self.testing_symbol

        APITestCase.assertTradeIsValid(
            self, trade, testing_symbol_or_symbols, self.platform_id
        )

    def assertMyTradeIsValid(self, my_trade, testing_symbol_or_symbols=None):
        if not testing_symbol_or_symbols:
            testing_symbol_or_symbols = self.testing_symbol

        APITestCase.assertMyTradeIsValid(
            self, my_trade, testing_symbol_or_symbols, self.platform_id
        )

    def assertCandleIsValid(self, candle, testing_symbol_or_symbols=None):
        if not testing_symbol_or_symbols:
            testing_symbol_or_symbols = self.testing_symbol

        APITestCase.assertCandleIsValid(
            self, candle, testing_symbol_or_symbols, self.platform_id
        )

    def assertTickerIsValid(self, ticker, testing_symbol_or_symbols=None):
        # if not testing_symbol_or_symbols:
        #     testing_symbol_or_symbols = self.testing_symbol

        APITestCase.assertTickerIsValid(
            self, ticker, testing_symbol_or_symbols, self.platform_id
        )

    def assertOrderBookIsValid(self, order_book, testing_symbol_or_symbols=None):
        if not testing_symbol_or_symbols:
            testing_symbol_or_symbols = self.testing_symbol

        APITestCase.assertOrderBookIsValid(
            self, order_book, testing_symbol_or_symbols, self.platform_id
        )

    def assertQuoteIsValid(self, quote, testing_symbol_or_symbols=None):
        testing_symbol_or_symbols = (
            testing_symbol_or_symbols
            if testing_symbol_or_symbols
            else self.testing_symbol
        )
        APITestCase.assertQuoteIsValid(
            self, quote, testing_symbol_or_symbols, self.platform_id
        )

    assertAnyQuote = APITestCase.assertAnyQuote

    def assertOrderBookDiffIsValid(self, order_book, testing_symbol_or_symbols=None):
        if not testing_symbol_or_symbols:
            testing_symbol_or_symbols = self.testing_symbol

        APITestCase.assertOrderBookDiffIsValid(
            self, order_book, testing_symbol_or_symbols, self.platform_id
        )

    def assertAggOrderBookIsValid(
        self, order_book, platform_ids=None, testing_symbol_or_symbols=None
    ):
        if not testing_symbol_or_symbols:
            testing_symbol_or_symbols = self.testing_symbols
        if not platform_ids:
            platform_ids = self.platform_ids

        APITestCase.assertOrderBookIsValid(
            self, order_book, testing_symbol_or_symbols, platform_ids
        )

    assertAnyOrderBookHasAsksAndBids = APITestCase.assertAnyOrderBookHasAsksAndBids

    # def assertOrderBookItemIsValid(self, order_book_item, testing_symbol_or_symbols=None):
    #     if not testing_symbol_or_symbols:
    #         testing_symbol_or_symbols = self.testing_symbol
    #
    #     APITestCase.assertOrderBookItemIsValid(self, order_book_item, testing_symbol_or_symbols, self.platform_id)

    def assertAccountIsValid(self, account):
        APITestCase.assertAccountIsValid(self, account, self.platform_id)

    def assertBalanceIsValid(self, balance):
        APITestCase.assertBalanceIsValid(self, balance, self.platform_id)

    def assertBalanceTransactionIsValid(self, transaction):
        APITestCase.assertBalanceTransactionIsValid(self, transaction, self.platform_id)

    def assertOrderIsValid(self, order, testing_symbol_or_symbols=None):
        # if not testing_symbol_or_symbols:
        #     testing_symbol_or_symbols = self.testing_order_symbol

        APITestCase.assertOrderIsValid(
            self, order, testing_symbol_or_symbols, self.platform_id
        )

    def assertPositionIsValid(self, position, testing_symbol_or_symbols=None):
        # if not testing_symbol_or_symbols:
        #     # (As position creating by order)
        #     testing_symbol_or_symbols = self.testing_order_symbol

        APITestCase.assertPositionIsValid(
            self, position, testing_symbol_or_symbols, self.platform_id
        )

    def assertOrdersEqual(self, first, second):
        order_fields = [
            "platform_id",
            "item_id",
            "symbol",
            "user_order_id",
            "order_type",
            "amount_original",
            "amount_executed",
            "price",
            "direction",
        ]
        for field in order_fields:
            self.assertEqual(getattr(first, field), getattr(second, field))


# REST


class TestRESTConverter(TestProtocolConverter):
    converter_class = RESTConverter

    def test_filter_result(self):
        timestamp_ms = 1500000000000
        item_dicts = [
            {
                ParamName.ITEM_ID: i,
                ParamName.TIMESTAMP: timestamp_ms,
                "is_milliseconds": True,
            }
            for i in range(10)
        ]
        item_trades = [Trade(**item) for item in item_dicts]
        from_item = ItemObject(**item_dicts[2])
        to_item = ItemObject(**item_dicts[8])

        result = self.converter._filter_result(
            item_trades, from_item, to_item, timestamp_ms, timestamp_ms
        )

        self.assertEqual(result, item_trades[2:9])  # 9 or 8?
        self.assertEqual(len(result), 7)  # 7 or 6?

        result = self.converter._filter_result(item_trades, from_item, to_item)

        self.assertEqual(result, item_trades[2:9])  # 9 or 8?
        self.assertEqual(len(result), 7)  # 7 or 6?

        result = self.converter._filter_result(
            item_trades, None, to_item, timestamp_ms, timestamp_ms
        )

        self.assertEqual(result, item_trades[:9])  # 9 or 8?
        self.assertEqual(len(result), 9)  # 9 or 8?

        result = self.converter._filter_result(
            item_trades, from_item, None, timestamp_ms, timestamp_ms
        )

        self.assertEqual(result, item_trades[2:])
        self.assertEqual(len(result), 8)

        result = self.converter._filter_result(
            item_trades, None, None, timestamp_ms, timestamp_ms
        )

        self.assertEqual(result, item_trades)
        self.assertEqual(len(result), 10)

        result = self.converter._filter_result(item_trades)

        self.assertEqual(result, item_trades)
        self.assertEqual(len(result), 10)

        result = self.converter._filter_result([item_trades[1]])

        self.assertEqual(result, [item_trades[1]])
        self.assertEqual(len(result), 1)

        # From and to_item are same
        result = self.converter._filter_result(
            [item_trades[2]], from_item, from_item, timestamp_ms, timestamp_ms
        )

        self.assertEqual(result, [item_trades[2]])
        self.assertEqual(len(result), 1)


class BaseTestRESTClient(TestClient):
    is_rest = True

    # (If False then platform supposed to use its max_limit instead
    # of returning error when we send too big limit)
    has_limit_error = False
    has_default_interval = False
    is_symbol_case_sensitive = True  # todo remove
    # todo remove and make common behavior for all platforms
    is_possible_fetch_my_trades_without_symbols = False

    is_rate_limit_error = False

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.is_rate_limit_error = False
        # TODO cancel all orders, close positions, etc

    def setUp(self):
        self.skipIfRateLimit()
        super().setUp()

    def assertGoodResult(self, result, is_iterable=True, message=None):
        if isinstance(result, Error) and result.code in (
            ErrorCode.RATE_LIMIT,
            ErrorCode.IP_BAN,
        ):
            self.__class__.is_rate_limit_error = True
            self.skipIfRateLimit()

        self.assertIsNotNone(result, message)
        self.assertNotIsInstance(result, Error, message or result)
        if is_iterable:
            self.assertIsInstance(result, list)
            self.assertGreater(len(result), 0, message)

    def assertErrorResult(self, result, error_code_expected=None):
        if isinstance(result, Error) and result.code in (
            ErrorCode.RATE_LIMIT,
            ErrorCode.IP_BAN,
        ):
            self.__class__.is_rate_limit_error = True
            self.skipIfRateLimit()

        self.assertIsNotNone(result)
        self.assertIsInstance(result, Error)
        if error_code_expected is not None:
            self.assertEqual(result.code, error_code_expected)

    def skipIfRateLimit(self):
        if self.__class__.is_rate_limit_error:
            self.skipTest("Rate limit reached for this platform. Try again later.")


class TestPlatformRESTClientCommon(BaseTestRESTClient):
    # Test all common methods except history and private methods

    # Simple methods

    def test_ping(self, is_auth=False):
        client = self.client_authed if is_auth else self.client

        result = client.ping()

        self.assertGoodResult(result, False)

    def test_get_server_timestamp(self, is_auth=False):
        client = self.client_authed if is_auth else self.client

        # With request
        client.use_milliseconds = True

        result0_ms = result = client.get_server_timestamp(force_from_server=True)

        self.assertGoodResult(result, False)
        self.assertGreater(result, (time.time() - 60) * 1000)
        self.assertIsInstance(result, int)

        client.use_milliseconds = False

        result0_s = result = client.get_server_timestamp(force_from_server=True)

        self.assertGoodResult(result, False)
        self.assertGreater(result, (time.time() - 60))
        self.assertLess(result, (time.time() + 60))
        self.assertIsInstance(result, (int, float))

        # Cached
        client.use_milliseconds = True

        result = client.get_server_timestamp(force_from_server=False)

        self.assertGoodResult(result, False)
        self.assertGreater(result, (time.time() - 60) * 1000)
        self.assertIsInstance(result, int)
        self.assertGreater(result, result0_ms)

        client.use_milliseconds = False

        result = client.get_server_timestamp(force_from_server=False)

        self.assertGoodResult(result, False)
        self.assertGreater(result, (time.time() - 60))
        self.assertLess(result, (time.time() + 60))
        self.assertIsInstance(result, (int, float))
        self.assertGreater(result, result0_s)

    def test_get_symbols(self, is_auth=False):
        client = self.client_authed if is_auth else self.client

        result = client.get_symbols()

        # as far as it returns dict of Symbol objects
        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 1)
        # (For BitMEX there are only 13 active instruments)
        # self.assertGreater(len(result), 50)
        self.assertGreater(len(result), 10)
        self.assertIsInstance(result[0], str)
        self.assertEqual(result, [symbol.upper() for symbol in result])
        if self.testing_symbol:
            self.assertIn(self.testing_symbol, result)

    def test_get_currecy_pairs(self, is_auth=False):
        client = self.client_authed if is_auth else self.client
        result = client.helper.get_currency_pair(self.platform_id, self.testing_symbol)
        self.assertIsInstance(result, CurrencyPair)

    # fetch_trades

    def test_fetch_trades(self, method_name="fetch_trades", is_auth=False):
        client = self.client_authed if is_auth else self.client

        result = getattr(client, method_name)(self.testing_symbol)

        self.assertGoodResult(result)
        self.assertGreater(len(result), 1)
        self.assertGreater(len(result), 20)
        self.assertTradeIsValid(result[0])
        for item in result:
            self.assertTradeIsValid(item)
        self.assertRightSymbols(result)

    def test_fetch_trades_errors(self, method_name="fetch_trades", is_auth=False):
        client = self.client_authed if is_auth else self.client

        # Wrong symbol
        result = getattr(client, method_name)(self.wrong_symbol)

        self.assertIsNotNone(result)
        self.assertIsInstance(result, Error)
        self.assertEqual(result.code, ErrorCode.WRONG_SYMBOL)

        if self.is_symbol_case_sensitive:
            # Symbol in lower case as wrong symbol
            result = getattr(client, method_name)(self.testing_symbol.lower())

            self.assertIsNotNone(result)
            self.assertIsInstance(result, Error)
            self.assertTrue(
                result.code == ErrorCode.WRONG_SYMBOL
                or result.code == ErrorCode.WRONG_PARAM
            )

    def test_fetch_trades_limit(self, method_name="fetch_trades", is_auth=False):
        client = self.client_authed if is_auth else self.client

        self.assertFalse(client.converter.is_use_max_limit)

        # Test limit
        # self.assertTrue(client.use_milliseconds)
        client.use_milliseconds = False
        result = getattr(client, method_name)(self.testing_symbol, 2)

        self.assertGoodResult(result)
        self.assertEqual(len(result), 2)
        # (Test use_milliseconds)
        # time.time() return the time in seconds since the epoch as a floating point number.
        self.assertLess(result[0].timestamp, time.time() * 1000)

        # Test is_use_max_limit (with limit param)
        client.use_milliseconds = True
        client.converter.is_use_max_limit = True
        result = getattr(client, method_name)(self.testing_symbol, 2)

        self.assertGoodResult(result)
        self.assertEqual(len(result), 2)
        # (Test use_milliseconds)
        self.assertGreater(result[0].timestamp, time.time())

        # (Get default item count)
        result = getattr(client, method_name)(self.testing_symbol)
        self.assertGoodResult(result)
        default_item_count = len(result)

        # Test is_use_max_limit (without limit param)
        client.converter.is_use_max_limit = True
        result = getattr(client, method_name)(self.testing_symbol)

        self.assertGoodResult(result)
        self.assertGreaterEqual(
            len(result),
            default_item_count,
            "Sometimes needs retry (for BitMEX, for example)",
        )
        for item in result:
            self.assertTradeIsValid(item)
        self.assertRightSymbols(result)

    def test_fetch_trades_limit_is_too_big(
        self, method_name="fetch_trades", is_auth=False
    ):
        client = self.client_authed if is_auth else self.client

        # Test limit is too big
        too_big_limit = 1000000
        result = getattr(client, method_name)(self.testing_symbol, too_big_limit)

        self.assertIsNotNone(result)
        if self.has_limit_error:
            self.assertIsInstance(result, Error)
            self.assertErrorResult(result, ErrorCode.WRONG_LIMIT)
        else:
            self.assertGoodResult(result)
            self.assertGreater(len(result), 10)
            self.assertLess(len(result), too_big_limit)
            for item in result:
                self.assertTradeIsValid(item)
            self.assertRightSymbols(result)
            max_limit_count = len(result)

            # Test is_use_max_limit uses the maximum possible limit
            client.converter.is_use_max_limit = True
            result = getattr(client, method_name)(self.testing_symbol)

            self.assertEqual(
                len(result), max_limit_count, "is_use_max_limit doesn't work"
            )

    def test_fetch_trades_sorting(self, method_name="fetch_trades", is_auth=False):
        if not self.is_sorting_supported:
            self.skipTest("Sorting is not supported by platform.")

        client = self.client_authed if is_auth else self.client

        self.assertEqual(client.converter.sorting, Sorting.DESCENDING)

        # Test descending (default) sorting
        result = getattr(client, method_name)(self.testing_symbol)

        self.assertGoodResult(result)
        self.assertGreater(len(result), 2)
        self.assertGreater(result[0].timestamp, result[-1].timestamp)

        # Test ascending sorting
        client.converter.sorting = Sorting.ASCENDING
        result2 = getattr(client, method_name)(self.testing_symbol)

        self.assertGoodResult(result2)
        self.assertGreater(len(result2), 2)
        self.assertLess(result2[0].timestamp, result2[-1].timestamp)

        # (not necessary)
        # logging.info("TEMP timestamps: %s %s", result[0].timestamp, result[-1].timestamp)
        # logging.info("TEMP timestamps: %s %s", result2[0].timestamp, result2[-1].timestamp)
        # # Test that it is the same items for both sorting types
        # self.assertGreaterEqual(result2[0].timestamp, result[-1].timestamp)
        # self.assertGreaterEqual(result[0].timestamp, result2[-1].timestamp)
        # Test that interval of items sorted ascending is far before the interval of descending

        self.assertLess(result2[0].timestamp, result[-1].timestamp)
        self.assertLess(result2[0].timestamp, result[0].timestamp)

    # Other public methods

    def test_fetch_candles(self):
        client = self.client

        # Error
        result = client.fetch_candles(None, None)

        self.assertErrorResult(result)

        if not self.has_default_interval:
            result = client.fetch_candles(self.testing_symbol, None)
            self.assertErrorResult(result)

        # Good
        result = client.fetch_candles(self.testing_symbol, self.testing_interval)

        self.assertGoodResult(result)
        for item in result:
            self.assertCandleIsValid(item, self.testing_symbol)
            self.assertEqual(item.interval, self.testing_interval)

        # todo test from_, to_, and limit

    # todo check on all platforms
    def test_fetch_candles__with_all_intervals(self):
        client = self.client
        converter = client.converter

        # Good
        intervals = converter.intervals_supported

        for interval in intervals:
            result = client.fetch_candles(self.testing_symbol, interval)

            self.assertGoodResult(result)
            for item in result:
                self.assertCandleIsValid(item, self.testing_symbol)
                self.assertEqual(item.interval, interval)

                # Seems doesn't have sense as it's solely an exchange based response. For example, return oldest
                # available, which of course won't be necessary aligned with interval start

                # Assert converter.weekday_start is set right
                # if item != result[0] and item != result[-1]:
                #     if client.use_milliseconds:
                #         start_datetime = datetime.fromtimestamp(item.timestamp / 1000)
                #     else:
                #         start_datetime = datetime.fromtimestamp(item.timestamp)
                #     # todo check other intervals
                #     if interval == CandleInterval.WEEK_1:
                #         self.assertEqual(start_datetime.weekday(), converter.weekday_start,
                #                          "Check and fix weekday_start for current platform. "
                #                          "Or this is the first candle for a given symbol: %s" % item)

    def test_fetch_ticker(self):
        client = self.client

        # Error

        # Good

        # Empty params
        # result = client.fetch_ticker(None)
        #
        # self.assertGoodResult(result)
        # self.assertGreater(len(result), 2)
        # for item in result:
        #     self.assertTickerIsValid(item)

        # Full params
        result = client.fetch_ticker(self.testing_symbol)

        logging.info(f"Result: {result}")
        self.assertGoodResult(result, False)
        self.assertTickerIsValid(result, self.testing_symbol)

        # Wrong symbol
        result = client.fetch_ticker("WRONG_SYM")

        logging.info(f"Result: {result}")
        self.assertIsInstance(result, Error)

    def test_fetch_tickers(self):
        client = self.client

        # Error

        # Good

        # Empty params
        result = client.fetch_tickers()

        self.assertGoodResult(result)
        self.assertGreater(len(result), 2)
        for item in result:
            self.assertTickerIsValid(item)

        # Full params
        result = client.fetch_tickers(self.testing_symbols)

        self.assertGoodResult(result)
        self.assertEqual(len(result), len(self.testing_symbols))
        for item in result:
            self.assertTickerIsValid(item, self.testing_symbols)

    def test_fetch_order_book(self):
        client = self.client

        # Error

        # Empty params
        result = client.fetch_order_book()

        self.assertErrorResult(result)

        # Good

        # Full params
        result = client.fetch_order_book(self.testing_symbol)

        self.assertGoodResult(result, False)
        self.assertOrderBookIsValid(result)

        # todo test limit and is_use_max_limit

    def test_fetch_quote(self):
        client = self.client

        # Error

        # Empty params
        result = client.fetch_quote(symbol="shitcoin")

        if self.platform_id in (Platform.BITMEX,):
            self.assertIsNone(result)
        else:
            self.assertErrorResult(result)

        # Good

        # Full params
        result = client.fetch_quote(self.testing_symbol)
        self.assertGoodResult(result, False)
        self.assertQuoteIsValid(result)


# TODO check that all open orders were created by this tests, otherwise log warning that
# somebody else does tests at the same time from the same account
class TestPlatformRESTClientPrivate(BaseTestRESTClient):
    # Test all methods except history methods

    # Define your params in subclasses
    testing_order_symbol = None
    testing_order_symbol2 = None
    testing_position_symbol = None
    current_market_price_by_symbol = {
        # For Binance
        "EOSETH": 0.02,
        # For BitMEX
        "XBTUSD": 3500,
        "ETHUSD": 108,
    }
    buy_sell_amount_by_symbol = None
    current_market_price_for_buying = 0
    current_market_price_for_selling = 100000000000000
    # -is_limit_price_for_market_orders_available = False
    buy_sell_amount = 0.000001
    buy_sell_amount_for_second_symbol = 0.000001
    # -is_create_market_orders_by_default = False
    is_support_fetch_orders_without_symbol = True
    is_show_closed_positions = True
    is_market_orders_not_supported = False
    # Some platforms (OKEx) has temporary symbols for futures
    is_temporary_symbols = False

    @property
    def order_buy_stop_limit_params(self):
        result = self.get_order_buy_stop_market_params(self.testing_order_symbol)
        price_limit = self.orderbook[self.testing_order_symbol].bids[-1].price
        result[ParamName.PRICE_LIMIT] = price_limit
        result[ParamName.ORDER_TYPE] = OrderType.STOP_LIMIT
        return result

    @property
    def order_buy_stop_market_params(self):
        return self.get_order_buy_stop_market_params(self.testing_order_symbol)

    def get_order_buy_stop_market_params(self, symbol=None, amount=None):
        if not amount:
            amount = SingleDataAggregator().get_symbol_min_amount(
                self.platform_id, symbol
            )
        price_stop = self.orderbook[symbol].asks[-1].price
        return {
            ParamName.ORDER_TYPE: OrderType.STOP_MARKET,
            ParamName.DIRECTION: Direction.BUY,
            ParamName.PRICE_STOP: price_stop,
            ParamName.PRICE_LIMIT: None,
            ParamName.AMOUNT: amount,
        }

    @property
    def order_buy_take_profit_limit_params(self):
        result = self.get_order_buy_take_profit_market_params(self.testing_order_symbol)
        price_limit = self.orderbook[self.testing_order_symbol].asks[-1].price
        result[ParamName.PRICE_LIMIT] = price_limit
        result[ParamName.ORDER_TYPE] = OrderType.TAKE_PROFIT_LIMIT
        return result

    @property
    def order_buy_take_profit_market_params(self):
        return self.get_order_buy_take_profit_market_params(self.testing_order_symbol)

    def get_order_buy_take_profit_market_params(self, symbol=None, amount=None):
        if not amount:
            amount = SingleDataAggregator().get_symbol_min_amount(
                self.platform_id, symbol
            )
        price_stop = self.orderbook[symbol].bids[-1].price
        return {
            ParamName.ORDER_TYPE: OrderType.TAKE_PROFIT_MARKET,
            ParamName.DIRECTION: Direction.BUY,
            ParamName.PRICE_STOP: price_stop,
            ParamName.PRICE_LIMIT: None,
            ParamName.AMOUNT: amount,
        }

    @property
    def order_buy_limit_params(self):
        return self.get_order_buy_limit_params(self.testing_order_symbol)

    def get_order_buy_limit_params(self, symbol=None, amount=None):
        current_market_price = (
            self.current_market_price_by_symbol.get(symbol)
            if self.current_market_price_by_symbol
            else None
        )
        current_market_price = self.orderbook[symbol].bids[-1].price
        if not amount:
            amount = SingleDataAggregator().get_symbol_min_amount(
                self.platform_id,
                symbol,
                price=current_market_price,
            )
        return {
            ParamName.ORDER_TYPE: OrderType.LIMIT,
            ParamName.DIRECTION: Direction.BUY,
            # Set minimal price for current testing_order_symbol and platform_id
            ParamName.PRICE: current_market_price
            or self.current_market_price_for_buying,
            ParamName.AMOUNT: amount,
        }

    @property
    def order_sell_limit_params(self):
        return self.get_order_sell_limit_params(self.testing_order_symbol)

    def get_order_sell_limit_params(self, symbol=None, amount=None):
        current_market_price = (
            self.current_market_price_by_symbol.get(symbol)
            if self.current_market_price_by_symbol
            else None
        )
        current_market_price = self.orderbook[symbol].asks[-1].price
        if not amount:
            amount = SingleDataAggregator().get_symbol_min_amount(
                self.platform_id,
                symbol,
                price=current_market_price,
            )
        return {
            ParamName.ORDER_TYPE: OrderType.LIMIT,
            ParamName.DIRECTION: Direction.SELL,
            # Set maximum price for current testing_order_symbol and platform_id
            ParamName.PRICE: current_market_price
            or self.current_market_price_for_selling,
            ParamName.AMOUNT: amount,
        }

    def get_order_buy_market_params(self, symbol):
        amount = SingleDataAggregator().get_symbol_min_amount(
            self.platform_id, symbol
        )
        return {
            ParamName.ORDER_TYPE: OrderType.MARKET,
            ParamName.DIRECTION: Direction.BUY,
            ParamName.AMOUNT: amount,
            ParamName.PRICE: 0,
        }

    def get_order_sell_market_params(self, symbol):
        amount = SingleDataAggregator().get_symbol_min_amount(
            self.platform_id, symbol
        )
        return {
            ParamName.ORDER_TYPE: OrderType.MARKET,
            ParamName.DIRECTION: Direction.SELL,
            ParamName.AMOUNT: amount,
            ParamName.PRICE: 0,
        }

    all_symbols = None
    created_orders = None
    # (Stop tests if price is bad)
    # is_stop_order_placing = False
    order_with_bad_price = None
    account = None
    balances = None
    orderbook = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # (Reset)
        # TestPlatformRESTClientPrivate.is_stop_order_placing = False
        cls.order_with_bad_price = None
        if cls.is_temporary_symbols:
            # Find the first symbol from list which begins with initially defined symbol
            cls.testing_symbol = cls._get_real_symbol(cls.testing_symbol)
            cls.testing_symbol2 = cls._get_real_symbol(
                cls.testing_symbol2, [cls.testing_symbol]
            )
            cls.testing_order_symbol = cls._get_real_symbol(cls.testing_order_symbol)
            cls.testing_order_symbol2 = cls._get_real_symbol(
                cls.testing_order_symbol2, [cls.testing_order_symbol]
            )
            if cls.testing_symbols:
                for i, testing_symbol in enumerate(cls.testing_symbols):
                    cls.testing_symbols[i] = cls._get_real_symbol(testing_symbol)
            if cls.current_market_price_by_symbol:
                for testing_symbol, v in cls.current_market_price_by_symbol.items():
                    del cls.current_market_price_by_symbol[testing_symbol]
                    cls.current_market_price_by_symbol[
                        cls._get_real_symbol(testing_symbol)
                    ] = v
            if cls.buy_sell_amount_by_symbol:
                for testing_symbol, v in cls.buy_sell_amount_by_symbol.items():
                    del cls.buy_sell_amount_by_symbol[testing_symbol]
                    cls.buy_sell_amount_by_symbol[
                        cls._get_real_symbol(testing_symbol)
                    ] = v

    @classmethod
    def _get_real_symbol(cls, symbol, except_symbols=None):
        if not symbol:
            return symbol
        if not cls.all_symbols:
            if not cls.client:
                cls.client = create_rest_client(cls.platform_id, version=cls.version)
            cls.all_symbols = cls.client.get_symbols()
        if symbol not in cls.all_symbols:
            real_symbols = (
                s
                for s in cls.all_symbols
                if s.startswith(symbol)
                and (not except_symbols or s not in except_symbols)
            )
            if any(real_symbols):
                symbol = next(real_symbols)
        return symbol

    def setUp(self):
        super().setUp()

        self.wait_before_fetch_s = self.client_authed.wait_before_fetch_s

        self.account = self.client_authed.get_account_info()
        self.balances = self.client_authed.fetch_balance()
        self.orderbook = {
            self.testing_order_symbol: self.client_authed.fetch_order_book(
                self.testing_order_symbol
            ),
            self.testing_order_symbol2: self.client_authed.fetch_order_book(
                self.testing_order_symbol2
            ),
        }
        logging.info(f"\n\nAccount: {self.account}")
        logging.info(f"Balance (10 first): {self.balances[:10]}")
        if self.is_support_fetch_orders_without_symbol:
            all_orders = self.client_authed.fetch_orders(is_open_only=False)
        else:
            all_orders = self.client_authed.fetch_orders(
                symbol=self.testing_order_symbol, is_open_only=False
            )
            if not isinstance(all_orders, Error):
                all_orders += self.client_authed.fetch_orders(
                    symbol=self.testing_order_symbol2, is_open_only=False
                )
        if isinstance(all_orders, Error):
            logging.info(
                "All orders: failed. %s\t%s", all_orders.code, all_orders.message
            )
        else:
            logging.info(
                "Last 10 orders: %s\n   open_orders: %s",
                all_orders[-10:],
                [o for o in all_orders if o.is_open],
            )
        # print("\n\n")

    def tearDown(self):
        # Cancel all created orders
        logging.info(
            "\n\nCanceling created orders on tear down %s", self.created_orders
        )
        if self.created_orders:
            for item in self.created_orders:
                result = self.client_authed.cancel_order(item)
                self.assertTrue(
                    result.is_closed,
                    "WARNING! Order was created during a test but not closed! "
                    "Fix close method or close manually through web interface of current platform."
                    "created_order: %s canceled_order: %s" % (item, result),
                )
        else:
            if self.is_support_fetch_orders_without_symbol:
                orders_canceled = self.client_authed.cancel_all_orders()
            else:
                orders_canceled = self.client_authed.cancel_all_orders(
                    symbol=self.testing_symbol
                )
                if not isinstance(orders_canceled, Error):
                    orders_canceled2 = self.client_authed.cancel_all_orders(
                        symbol=self.testing_symbol2
                    )
                    if not isinstance(orders_canceled2, Error):
                        orders_canceled += orders_canceled2

        self.wait_before_fetch()
        self.wait_before_fetch()
        # Check all orders canceled
        client = self.client_authed
        if self.is_support_fetch_orders_without_symbol:
            result = client.fetch_orders()
        else:
            result = client.fetch_orders(self.testing_order_symbol)
            if not isinstance(result, Error):
                result2 = client.fetch_orders(self.testing_order_symbol2)
                if not isinstance(result2, Error):
                    result += result2
        # print("All orders:", result)
        self.assertGoodResult(result, False)
        for item in result:
            self.assertTrue(
                item.is_closed,
                "Order: %s is not closed. Maybe wait_before_fetch_s=%s is too small"
                % (item, self.wait_before_fetch_s),
            )
        # Close all positions
        positions = client.close_all_positions()
        logging.info(
            "Positions closed: %s %s",
            len(positions) if isinstance(positions, list) else None,
            positions,
        )
        super().tearDown()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        # cls._cancel_all_orders_and_close_all_positions()

    # the same exist in tearDown
    # @classmethod
    # def _cancel_all_orders_and_close_all_positions(cls):
    #     client = create_rest_client(cls.platform_id, True, cls.version, pivot_symbol=cls.pivot_symbol)
    #     if not client:
    #         return
    #
    #     # Show info before
    #     balances = client.fetch_balance()
    #     positions = client.get_positions()
    #     logging.info("\n\nReverting orders tests for platform_id: %s", cls.platform_id)
    #     logging.info("Balances: %s", balances)
    #     logging.info("Positions: %s", positions)
    #     # my_trades = client.fetch_my_trades(cls.testing_order_symbol)
    #     # logging.info("MyTrades: %s", my_trades)
    #     if cls.is_support_fetch_orders_without_symbol:
    #         all_orders = client.fetch_orders(is_open_only=False)
    #     else:
    #         all_orders = client.fetch_orders(symbol=cls.testing_order_symbol, is_open_only=False)
    #         all_orders += client.fetch_orders(symbol=cls.testing_order_symbol2, is_open_only=False)
    #     logging.info("All orders: %s", all_orders)
    #     # self.assertGoodResult(all_orders)
    #
    #     # Cancel all open orders
    #     if cls.is_support_fetch_orders_without_symbol:
    #         orders_canceled = client.cancel_all_orders()
    #     else:
    #         orders_canceled = client.cancel_all_orders(symbol=cls.testing_symbol)
    #         orders_canceled += client.cancel_all_orders(symbol=cls.testing_symbol2)
    #
    #     logging.info("Orders canceled: %s %s", len(orders_canceled), orders_canceled)
    #
    #     # Close all positions
    #     positions = client.close_all_positions()
    #     logging.info("Positions closed: %s %s", len(positions) if isinstance(positions, list) else None, positions)
    #
    #     # Check
    #     balances = client.fetch_balance()
    #     # orders = client.fetch_orders(is_open_only=True)
    #     if cls.is_support_fetch_orders_without_symbol:
    #         orders = client.fetch_orders()
    #     else:
    #         orders = client.fetch_orders(symbol=cls.testing_order_symbol)
    #         orders += client.fetch_orders(symbol=cls.testing_order_symbol2)
    #     positions = client.get_positions()
    #     logging.info("After tests. All balances: %s orders: %s positions: %s", balances, orders, positions)

    def wait_before_fetch(self):
        # (Wait some time after changes applied and before fetch)
        if self.client_authed.wait_before_fetch_s > 0:
            time.sleep(self.client_authed.wait_before_fetch_s)

    def _create_order(self, symbol=None, is_limit=True, is_buy=True, order_params=None):
        if not symbol:
            symbol = self.testing_order_symbol

        client = self.client_authed
        if self.__class__.order_with_bad_price:
            self.skipTest(
                "Some order %s was filled at once! Set lower price!"
                % self.__class__.order_with_bad_price
            )

        if not order_params:
            if is_buy:
                order_params = (
                    self.get_order_buy_limit_params(symbol)
                    if is_limit
                    else self.get_order_buy_market_params(symbol)
                )
            else:
                order_params = (
                    self.get_order_sell_limit_params(symbol)
                    if is_limit
                    else self.get_order_sell_market_params(symbol)
                )
        while True:
            order = client.create_order(symbol, **order_params, is_test=False)

            if isinstance(order, Error):
                if "The system is currently overloaded" in order.message:
                    logging.warning(
                        "Can't place test order, RETRY after 30 sec", str(order)
                    )
                    time.sleep(30)
                else:
                    raise Exception("Can't place test order", str(order))
            else:
                self.assertOrderIsValid(order, symbol)
                if order.is_closed and order.is_limit:
                    self.__class__.order_with_bad_price = order
                if is_limit:
                    self.assertTrue(
                        order.is_open,
                        "It seems that order filled at once! Set %s price!"
                        % ("lower" if order.is_buy else "higher"),
                    )
                # Add for canceling in tearDown
                if not self.created_orders:
                    self.created_orders = []
                self.created_orders.append(order)

                return order

    def _order_canceled(self, order):
        if order in self.created_orders:
            self.created_orders.remove(order)

    def assertGoodResultForCanceledOrder(self, cancel_result):
        self.assertGoodResult(
            cancel_result,
            False,
            "IMPORTANT! Order was created during tests, but not canceled!",
        )

    def assertCanceledOrder(self, order, symbol, item_id):
        self.assertIsInstance(order, Order)
        self.assertEqual(order.item_id, item_id)
        self.assertItemIsValid(order, symbol, is_with_timestamp=False)

    def assertOrdersCount(self, expected_count, is_open_only=True, is_wait=True):
        if is_wait:
            self.wait_before_fetch()

        client = self.client_authed
        if self.is_support_fetch_orders_without_symbol:
            result = client.fetch_orders(is_open_only=is_open_only)
        else:
            result = client.fetch_orders(self.testing_symbol, is_open_only=is_open_only)
            result += client.fetch_orders(
                self.testing_symbol2, is_open_only=is_open_only
            )
        self.assertGoodResult(
            result, is_iterable=expected_count > 0
        )  # (Don't check len(result) if expecting 0 orders)
        self.assertGreaterEqual(len(result), expected_count)
        return result

    def assertPositionsCount(self, expected_count, is_wait=True, can_be_empty=False):
        if is_wait:
            self.wait_before_fetch()

        client = self.client_authed
        result = client.get_positions()
        if can_be_empty or expected_count == 0:
            # okex: position can be empty []
            self.assertEqual(result, [])
        else:
            self.assertGoodResult(result)
            self.assertGreaterEqual(
                len(result),
                expected_count,
                "Maybe wait_before_fetch_s: %s is not enough. result: %s"
                % (self.wait_before_fetch_s, result),
            )

            open_positions = [p for p in result if p.is_open]
            self.assertEqual(len(open_positions), expected_count, result)

        return result

    # Private API methods

    def test_create_stop_orders(self):
        client = self.client_authed
        with self.subTest("STOP_MARKET LIMIT"):
            if OrderType.STOP_LIMIT in client.supported_order_types:
                result = client.create_order(
                    self.testing_order_symbol, **self.order_buy_stop_limit_params
                )
                self.assertGoodResult(result, is_iterable=False)
                cancel_result = client.cancel_order(result)
                if self.platform_id not in [Platform.BINANCE]:
                    # return quite poor info as response of these type of orders
                    self.assertOrderIsValid(result, self.testing_order_symbol)
        with self.subTest("STOP_MARKET MARKET"):
            if OrderType.STOP_MARKET in client.supported_order_types:
                result = client.create_order(
                    self.testing_order_symbol, **self.order_buy_stop_market_params
                )
                self.assertGoodResult(result, is_iterable=False)
                cancel_result = client.cancel_order(result)
                self.assertOrderIsValid(result, self.testing_order_symbol)
        with self.subTest("TAKE PROFIT LIMIT"):
            if OrderType.TAKE_PROFIT_LIMIT in client.supported_order_types:
                result = client.create_order(
                    self.testing_order_symbol, **self.order_buy_take_profit_limit_params
                )
                self.assertGoodResult(result, is_iterable=False)
                cancel_result = client.cancel_order(result)
                if self.platform_id not in [Platform.BINANCE]:
                    # return quite poor info as response of these type of orders
                    self.assertOrderIsValid(result, self.testing_order_symbol)
        with self.subTest("TAKE PROFIT MARKET"):
            if OrderType.TAKE_PROFIT_MARKET in client.supported_order_types:
                result = client.create_order(
                    self.testing_order_symbol,
                    **self.order_buy_take_profit_market_params,
                )
                self.assertGoodResult(result, is_iterable=False)
                cancel_result = client.cancel_order(result)
                self.assertOrderIsValid(result, self.testing_order_symbol)

    def test_set_leverage(self):
        leverages = [1, Decimal(0.6)]
        client = self.client_authed
        for leverage in leverages:
            result = client.set_leverage(leverage, self.testing_symbol)
            self.assertGoodResult(result, is_iterable=False)
            self.assertAlmostEqual(result["leverage"], leverage)
        client.set_leverage(0, self.testing_symbol)

    def test_get_account_info(self):
        client = self.client_authed

        # Error

        # Good

        # Empty params  # Full params
        result = client.get_account_info()

        self.assertGoodResult(result, is_iterable=False)
        self.assertAccountIsValid(result)

    def test_check_credentials(self):
        client = self.client_authed

        result = client.check_credentials()
        self.assertTrue(result)

    def test_fetch_balance(self):
        client = self.client_authed

        # Error

        # Good

        # Empty params
        # Full params
        result = client.fetch_balance()

        self.assertGoodResult(result, message="Maybe account has no funds.")
        for balance in result:
            self.assertBalanceIsValid(balance)
        # Check repetitions
        symbols_returned = [balance.symbol for balance in result]
        self.assertEqual(
            len(set(symbols_returned)),
            len(symbols_returned),
            "Several balances for same symbols. %s" % result,
        )

    def test_fetch_balance_transactions(self):
        client = self.client_authed

        if self.platform_id != Platform.BITMEX:
            self.skipTest("Implemented only for BitMEX")

        # Error

        # Good

        # Empty params
        result = client.fetch_balance_transactions()

        self.assertGoodResult(result, message="Maybe account has no funds.")
        for item in result:
            self.assertBalanceTransactionIsValid(item)
        self.assertGreater(
            result[0].timestamp,
            result[-1].timestamp,
            "Maybe too few transactions. Make some changes for balance",
        )

        # Same by fetch()
        result2 = client.fetch(Endpoint.BALANCE_TRANSACTION)

        self.assertEqual(result, result2)

        # Paging
        result10_2 = client.fetch_balance_transactions(10, 2)
        result10_3 = client.fetch_balance_transactions(10, 3)
        result20_1 = client.fetch_balance_transactions(20, 1)

        self.assertEqual(result20_1, result10_2 + result10_3)

        # Test that last page returns []
        page = 0
        # result = 1
        while result:
            result = client.fetch_balance_transactions(1000, page)
            if not isinstance(result, list):
                break
            page += 1
        self.assertEqual(result, [])

        # (Error)
        # result = client.fetch_balance_transactions(10000000, 10000000)
        #
        # self.assertEqual(result20_1, result10_2 + result10_3)

        # is_only_by_user
        result = client.fetch_balance_transactions()
        transaction_types = [item.transaction_type for item in result]
        self.assertIn(TransactionType.REALISED_PNL, transaction_types)

        result = client.fetch_balance_transactions(is_only_by_user=True)
        transaction_types = [item.transaction_type for item in result]
        self.assertNotIn(TransactionType.REALISED_PNL, transaction_types)

    def test_fetch_my_trades(self):
        client = self.client_authed

        # Error

        # Empty params
        result = client.fetch_my_trades(None)

        if not self.is_possible_fetch_my_trades_without_symbols:
            self.assertErrorResult(result)
        else:
            self.assertGoodResult(result, is_iterable=True)

        # Good

        # Full params
        result = client.fetch_my_trades(self.testing_symbol)
        result2 = client.fetch_my_trades("ETHUSD")

        NO_ITEMS_FOR_ACCOUNT = True
        self.assertGoodResult(result, not NO_ITEMS_FOR_ACCOUNT)
        for item in result:
            self.assertMyTradeIsValid(item, self.testing_symbols)

        # Limit
        result = client.fetch_my_trades(self.testing_symbol, 1)

        self.assertGoodResult(result, not NO_ITEMS_FOR_ACCOUNT)
        self.assertLessEqual(len(result), 1)

        result = client.fetch_my_trades(self.testing_symbol, 7)

        self.assertGoodResult(result, not NO_ITEMS_FOR_ACCOUNT)
        self.assertLessEqual(len(result), 7)
        if len(result) < 7:
            logging.warning("You have not enough my trades to test limit for sure.")
        for item in result:
            self.assertMyTradeIsValid(item, self.testing_symbols)

    def test_create_order(self, is_test=False):
        if self.__class__.order_with_bad_price:
            self.skipTest(
                "Some order %s was filled at once! Set lower price!"
                % self.__class__.order_with_bad_price
            )
        client = self.client_authed

        # Error

        # Empty params
        result = client.create_order(None, None, None, None, None)

        self.assertErrorResult(result)

        # Good

        # Full params
        # Buy, Market
        if not self.is_market_orders_not_supported:
            result = client.create_order(
                self.testing_order_symbol,
                **self.get_order_buy_market_params(self.testing_symbol),
                is_test=is_test,
            )

            self.assertGoodResult(result, is_iterable=False)
            cancel_result = client.cancel_order(
                result
            )  # May be already filled (then "result" will be returned)

            self.assertOrderIsValid(result, self.testing_order_symbol)
            if self.platform_id not in [Platform.OKEX, Platform.BILAXY]:
                # Okex, Bilaxy does not send order_type!!!
                self.assertEqual(
                    result.order_type,
                    self.get_order_buy_market_params(self.testing_order_symbol).get(ParamName.ORDER_TYPE),
                )
            self.assertEqual(
                result.direction, self.get_order_buy_market_params(self.testing_order_symbol).get(ParamName.DIRECTION)
            )
            # (None for market type)
            # self.assertEqual(float(result.price), float(self.order_buy_market_params.get(ParamName.PRICE)))
            # can't check dynamic value
            # self.assertEqual(
            #     float(result.amount_original),
            #     float(self.order_buy_market_params.get(ParamName.AMOUNT)))
            # todo
            # self.assertEqual(float(result.amount_executed), float(self.order_buy_market_params.get(ParamName.AMOUNT)))
            self.assertGoodResultForCanceledOrder(cancel_result)

        # Buy, Limit
        result = client.create_order(
            self.testing_order_symbol, **self.order_buy_limit_params, is_test=is_test
        )

        self.assertGoodResult(result, is_iterable=False)
        cancel_result = client.cancel_order(result)

        self.assertOrderIsValid(result, self.testing_order_symbol)
        if self.platform_id not in [Platform.OKEX, Platform.BILAXY]:
            # Okex, Bilaxy does not send order_type!!!
            self.assertEqual(
                result.order_type, self.order_buy_limit_params.get(ParamName.ORDER_TYPE)
            )
        self.assertEqual(
            result.direction, self.order_buy_limit_params.get(ParamName.DIRECTION)
        )
        self.assertEqual(
            float(result.price), float(self.order_buy_limit_params.get(ParamName.PRICE))
        )
        # if result.amount_original is not None:
        #     self.assertEqual(float(result.amount_original),
        #                      float(self.order_buy_limit_params.get(ParamName.AMOUNT_ORIGINAL)))
        # if result.amount_executed is not None:
        #     self.assertEqual(float(result.amount_executed),
        #                      float(self.order_buy_limit_params.get(ParamName.AMOUNT_EXECUTED)))
        self.assertGoodResultForCanceledOrder(cancel_result)

        # (May be insufficient funds because of no EOS or other shit coin on balance)
        # Sell, Limit
        result = client.create_order(
            self.testing_order_symbol, **self.order_sell_limit_params, is_test=is_test
        )

        self.assertGoodResult(result, is_iterable=False)
        cancel_result = client.cancel_order(result)

        self.assertOrderIsValid(result, self.testing_order_symbol)
        if self.platform_id not in [Platform.OKEX, Platform.BILAXY]:
            # Okex, Bilaxy does not send order_type!!!
            self.assertEqual(
                result.order_type,
                self.order_sell_limit_params.get(ParamName.ORDER_TYPE),
            )
        self.assertEqual(
            result.direction, self.order_sell_limit_params.get(ParamName.DIRECTION)
        )
        self.assertEqual(
            float(result.price),
            float(self.order_sell_limit_params.get(ParamName.PRICE)),
        )
        self.assertEqual(
            float(result.amount_original),
            float(self.order_sell_limit_params.get(ParamName.AMOUNT)),
        )
        self.assertGoodResultForCanceledOrder(cancel_result)

        if not self.is_market_orders_not_supported:
            # Sell, Market - to revert buy-market order
            result = client.create_order(
                self.testing_order_symbol,
                **self.get_order_sell_market_params(self.testing_symbol),
                is_test=is_test,
            )

            self.assertGoodResult(result, is_iterable=False)
            cancel_result = client.cancel_order(result)

            self.assertOrderIsValid(result, self.testing_order_symbol)
            if self.platform_id not in [Platform.OKEX, Platform.BILAXY]:
                # Okex, Bilaxy does not send order_type!!!
                self.assertEqual(
                    result.order_type,
                    self.get_order_sell_market_params(self.testing_order_symbol).get(ParamName.ORDER_TYPE),
                )
            self.assertEqual(
                result.direction, self.get_order_sell_market_params(self.testing_order_symbol).get(ParamName.DIRECTION)
            )
            # (None for market type)
            # self.assertEqual(float(result.price), float(self.order_sell_market_params.get(ParamName.PRICE)))
            # can't check dynamic values
            # self.assertEqual(
            #     float(result.amount_original),
            #     float(self.order_sell_market_params.get(ParamName.AMOUNT)))
            # todo
            # self.assertEqual(float(result.amount_executed), float(self.order_buy_market_params.get(ParamName.AMOUNT)))
            self.assertGoodResultForCanceledOrder(cancel_result)

    def test_cancel_order(self):
        client = self.client_authed

        # Error

        # Empty params
        logging.info("\n\nEmpty params - error")
        result = client.cancel_order(None)

        self.assertErrorResult(result)

        # Good

        # Full params
        logging.info("\n\nFull params - by order")
        order = self._create_order()

        result = client.cancel_order(order, "some")
        logging.info("Cancelled order: %s", result)
        self.assertGoodResultForCanceledOrder(result)
        # self.assertGoodResult(result)
        self.assertIn(result.order_status, OrderStatus.closed)
        self.assertEqual(
            result.order_status,
            OrderStatus.CANCELED,
            "Check CANCELED status is supported for the platform.",
        )
        self.assertNotEqual(result.order_status, order.order_status)
        self.assertCanceledOrder(result, order.symbol, order.item_id)
        self._order_canceled(order)

        # Same by item_id and symbol
        logging.info("\n\nFull params - by order_id and symbol")
        order = self._create_order()

        result = client.cancel_order(order.item_id, order.symbol)
        self.assertGoodResultForCanceledOrder(result)
        # self.assertGoodResult(result)
        self.assertIn(result.order_status, OrderStatus.closed)
        self.assertEqual(
            result.order_status,
            OrderStatus.CANCELED,
            "Check CANCELED status is supported for the platform.",
        )
        self.assertIsNot(result, order)
        self.assertEqual(result.item_id, order.item_id)
        self.assertCanceledOrder(result, order.symbol, order.item_id)
        self._order_canceled(order)

    def _create_several_orders(self):
        order1 = self._create_order(self.testing_order_symbol, is_buy=True)
        # in order to sell something useless, we have to buy something useless
        order_pre2 = self._create_order(
            self.testing_order_symbol, is_buy=True, is_limit=False
        )
        order2 = self._create_order(self.testing_order_symbol, is_buy=False)
        # we have to cancel order opened for sell order previously
        self._order_canceled(order_pre2)
        self.client_authed.close_all_positions()
        order3_params = self.get_order_buy_limit_params(
            symbol=self.testing_order_symbol2,
            amount=self.buy_sell_amount_for_second_symbol,
        )
        order3 = self._create_order(
            self.testing_order_symbol2, order_params=order3_params
        )
        return order1, order2, order3

    def test_cancel_all_orders(self, is_buy=True):
        client = self.client_authed

        # Error

        # Good

        # Nothing to close
        if self.is_support_fetch_orders_without_symbol:
            orders = client.fetch_orders(is_open_only=True)
        else:
            orders = client.fetch_orders(
                symbol=self.testing_order_symbol, is_open_only=True
            )
            if not isinstance(orders, Error):
                orders += client.fetch_orders(
                    symbol=self.testing_order_symbol2, is_open_only=True
                )

        self.assertGoodResult(orders, False)
        self.assertEqual(orders, [])

        if self.is_support_fetch_orders_without_symbol:
            result = client.cancel_all_orders()
        else:
            result = client.cancel_all_orders(self.testing_symbol)
            result += client.cancel_all_orders(self.testing_symbol2)

        self.assertGoodResult(result, False)
        self.assertEqual(result, [])
        # todo check testing_order_symbol and testing_order_symbol2 use same base symbol
        # to fix failing creating order because of insufficient funds

        # Full params
        self._create_several_orders()

        self.assertOrdersCount(3)
        for item in result:
            self.assertFalse(item.is_closed)

        result = client.cancel_all_orders(self.testing_order_symbol)
        # print("Canceled orders:", result)
        list(
            map(
                lambda o: o.symbol == self.testing_order_symbol
                and self._order_canceled(o),
                self.created_orders[:],
            )
        )

        self.assertGoodResultForCanceledOrder(result)
        for item in result:
            self.assertEqual(item.symbol, self.testing_order_symbol)
            self.assertTrue(
                item.is_closed, "Check CANCELED status is supported for the platform."
            )
            self.assertIn(item.order_status, OrderStatus.closed)
            self.assertEqual(item.order_status, OrderStatus.CANCELED)

        self.assertOrdersCount(1)

        # Same by item_id and symbol
        # if self.platform_id == Platform.OKEX:
        order1 = self._create_order(
            self.testing_order_symbol, is_limit=True, is_buy=is_buy
        )
        # else:
        #     order1 = self._create_order(self.testing_order_symbol, is_limit=False, is_buy=True)
        self.assertOrdersCount(2)  # Includes waiting for order added

        if self.is_support_fetch_orders_without_symbol:
            orders = client.fetch_orders(is_open_only=True)
        else:
            orders = client.fetch_orders(
                symbol=self.testing_order_symbol, is_open_only=True
            )
            orders += client.fetch_orders(
                symbol=self.testing_order_symbol2, is_open_only=True
            )
        self.assertGreaterEqual(len({o.symbol for o in orders}), 2)

        if self.is_support_fetch_orders_without_symbol:
            result = client.cancel_all_orders()
        else:
            result = client.cancel_all_orders(self.testing_symbol)
            result += client.cancel_all_orders(self.testing_symbol2)

        self.assertGoodResultForCanceledOrder(result)
        # self.assertGoodResult(result)
        for order in orders:
            self._order_canceled(order)
        for item in result:
            self.assertTrue(item.is_closed)

        orders = self.assertOrdersCount(0)  # Includes waiting for order added
        # orders = client.fetch_orders(is_open_only=True)
        # # self.assertGoodResult(orders)     # If orders == [], test failed
        self.assertEqual(orders, [])

    def test_fetch_order(self):
        client = self.client_authed

        # Error

        # Empty params
        result = client.fetch_order(None)

        self.assertErrorResult(result)

        # # temp
        # result = client.fetch_order("someid", "somesymb")
        # Good

        # Full params (check get new)
        order = self._create_order()
        if self.platform_id == Platform.BITMEX:
            self.wait_before_fetch()

        # Test symbol is got from order instance
        result = client.fetch_order(order, "some_ignored")

        self.assertGoodResult(result, False)
        self.assertOrdersEqual(order, result)
        self.assertOrderIsValid(result, self.testing_order_symbol)
        self.assertTrue(result.is_open)

        # (Cancel)
        cancel_result = client.cancel_order(order)
        self.assertGoodResultForCanceledOrder(cancel_result)

        if self.platform_id == Platform.BITMEX:
            self.wait_before_fetch()
        # Same by item_id and symbol (check get canceled)
        result = client.fetch_order(order.item_id, order.symbol)

        self.assertGoodResult(result, False)
        self.assertOrdersEqual(order, result)
        self.assertOrderIsValid(result, order.symbol)
        self.assertTrue(result.is_closed)
        # Clear self.created_orders
        self._order_canceled(order)

    def test_fetch_orders(self):
        client = self.client_authed

        # Error

        # Good
        # TODO: выяснить всем ли нужен тут именно маркет
        # Как правило у меня маркет сразу заканчивается сделкой!
        if self.platform_id in [
            Platform.BITMEX,
            Platform.BINANCE,
            Platform.OKEX,
            Platform.BITTREX,
            Platform.COINSUPER,
            Platform.BILAXY,
        ]:
            order = self._create_order(is_limit=True)
        else:
            order = self._create_order(is_limit=False)
        # Empty params
        # (For Binance it has weight 40)
        # if self.platform_id != Platform.BINANCE:
        #     result = client.fetch_orders()
        #
        #     self.assertGoodResult(result)
        #     self.assertGreater(len(result), 0)
        #     for item in result:
        #         self.assertOrderIsValid(item)

        # All
        self.wait_before_fetch()

        if self.is_support_fetch_orders_without_symbol:
            result = client.fetch_orders(is_open_only=False)
        else:
            result = client.fetch_orders(
                symbol=self.testing_order_symbol, is_open_only=False
            )
            result += client.fetch_orders(
                symbol=self.testing_order_symbol2, is_open_only=False
            )
        # logging.info("All orders: %s", result)

        self.assertGoodResult(result)
        self.assertGreater(len(result), 0)
        # -is_any_open = is_any_closed = False
        for item in result:
            self.assertOrderIsValid(item)

            # -if item.is_open:
            #     is_any_open = True
            # elif item.is_closed:
            #     is_any_closed = True
        self.assertTrue(
            any(item.is_open for item in result),
            "(May be blocked for too many orders created or "
            "wait_before_fetch_s=%s is too small)" % self.wait_before_fetch_s,
        )
        if (
            self.platform_id != Platform.BILAXY
        ):  # Bilaxy не отдает отмененные ордера в fetch_orders
            self.assertTrue(any(item for item in result if item.is_closed))

        # (Assert new items come first)
        self.assertGreater(len(result), 1)
        # self.assertGreater(result[0].timestamp, result[-1].timestamp)

        # Full params
        result = client.fetch_orders(self.testing_order_symbol, is_open_only=True)
        # logging.info("Open orders: %s", result)

        self.assertGoodResult(result)
        # self.assertGreater(len(result), 0)
        for item in result:
            self.assertOrderIsValid(item, self.testing_order_symbol)
            self.assertTrue(item.is_open)

        cancel_result = client.cancel_order(order)
        self._order_canceled(order)
        self.assertGoodResultForCanceledOrder(cancel_result)
        self.wait_before_fetch()

        # All (all open are closed)
        result = client.fetch_orders(self.testing_order_symbol, is_open_only=False)

        if (
            self.platform_id != Platform.BILAXY
        ):  # Bilaxy не отдает отмененные ордера в fetch_orders
            self.assertGoodResult(result)
            self.assertGreater(len(result), 0)
            for item in result:
                self.assertOrderIsValid(item, self.testing_order_symbol)
                # TODO check closed
                # self.assertTrue(item.is_closed, item)
                # Clear self.created_orders
                self._order_canceled(item)

        # todo test also limit and from_item (and to_item? - for binance) for is_open_only=false

    def _create_position(self, symbol=None):
        if not symbol:
            symbol = self.testing_order_symbol
        order = self._create_order(symbol=symbol, is_limit=False, is_buy=True)
        return order

    def test_get_positions(self):
        client = self.client_authed

        self.assertIsNotNone(self.testing_order_symbol)
        self.assertIsNotNone(self.testing_order_symbol2)
        self.assertNotEqual(self.testing_order_symbol, self.testing_order_symbol2)

        # (All positions must be closed after tearDownClass)
        result = client.get_positions()

        if self.is_show_closed_positions:
            self.assertGoodResult(result)
            self.assertGreaterEqual(len(result), 0)
            for item in result:
                self.assertPositionIsValid(item, item.symbol)
                self.assertFalse(item.is_open)
        else:
            self.assertEqual(result, [])

        # Create buy position
        order1 = self._create_position()

        self._test_get_positions(
            symbol=None, is_buy_present=True, is_sell_present=False
        )
        testing_position_symbol = self.testing_order_symbol
        if self.testing_position_symbol:
            testing_position_symbol = self.testing_position_symbol
        self._test_get_positions(
            symbol=testing_position_symbol, is_buy_present=True, is_sell_present=False
        )

        # Test also for sell position if possible for current platform (for BitMEX)
        if client.is_position_supported:
            # Create sell position
            order2 = self._create_order(
                symbol=self.testing_order_symbol2, is_limit=False, is_buy=False
            )

            self._test_get_positions(
                symbol=None, is_buy_present=True, is_sell_present=True
            )
            self._test_get_positions(
                symbol=self.testing_order_symbol2,
                is_buy_present=False,
                is_sell_present=True,
            )

        # (Tear down before next test)
        client.close_all_positions()

    def _test_get_positions(
        self, symbol=None, is_buy_present=False, is_sell_present=False
    ):
        client = self.client_authed

        result = client.get_positions(symbol)

        self.assertGoodResult(result)
        self.assertGreaterEqual(len(result), is_buy_present + is_sell_present)
        for item in result:
            self.assertPositionIsValid(item)

        if any([p for p in result if p.direction]):
            if is_buy_present:
                self.assertGreater(
                    len([p for p in result if p.is_buy and p.is_open]), 0
                )
            if is_sell_present:
                self.assertGreater(
                    len([p for p in result if p.is_sell and p.is_open]), 0
                )

    def test_close_position(self, is_buy=True):
        client = self.client_authed

        self.assertIsNotNone(self.testing_order_symbol)
        self.assertIsNotNone(self.testing_order_symbol2)
        self.assertNotEqual(self.testing_order_symbol, self.testing_order_symbol2)

        # (Create positions)
        order1 = self._create_order(
            symbol=self.testing_order_symbol, is_limit=False, is_buy=is_buy
        )
        order2 = self._create_order(
            symbol=self.testing_order_symbol2, is_limit=False, is_buy=is_buy
        )

        result0 = self.assertPositionsCount(2)

        # Empty params
        result = client.close_position(None)

        self.assertErrorResult(result)  # , ErrorCode.WRONG_SYMBOL)
        self.assertPositionsCount(2)

        # Close position
        self.assertTrue(result0[0].is_open)

        result = client.close_position(result0[0])

        self.assertGoodResult(result, False)
        if self.is_show_closed_positions:
            self.assertPositionIsValid(result)
            self.assertFalse(result.is_open)
        self.assertPositionsCount(1)

        # Close if there is nothing to close
        result = client.close_position(result0[0])

        if self.is_show_closed_positions:
            self.assertGoodResult(result, False)
            self.assertPositionIsValid(result)  # self.assertIsNone(result)
            self.assertFalse(result.is_open)
        self.assertPositionsCount(1)
        client.close_position(result0[1])

    def test_close_position__sell_positions(self):
        client = self.client_authed
        if not client.is_position_supported:
            self.skipTest(
                "Positions are not supported for current platform, so we cannot create sell position."
            )

        self.test_close_position(False)

    def test_close_all_positions(self, is_buy=True):

        client = self.client_authed

        self.assertIsNotNone(self.testing_order_symbol)
        self.assertIsNotNone(self.testing_order_symbol2)
        # self.assertIsNotNone(self.testing_order_symbol3)
        self.assertNotEqual(self.testing_order_symbol, self.testing_order_symbol2)
        # self.assertNotEqual(self.testing_order_symbol, self.testing_order_symbol3)

        # (Create position)
        logging.info("\n\nCreating position 1 %s", self.testing_order_symbol)
        order1 = self._create_order(
            symbol=self.testing_order_symbol, is_limit=False, is_buy=is_buy
        )
        logging.info("\n\nCreating position 2 %s", self.testing_order_symbol2)
        order2 = self._create_order(
            symbol=self.testing_order_symbol2, is_limit=False, is_buy=is_buy
        )

        result0 = self.assertPositionsCount(2)

        # Wrong params
        result = client.close_all_positions(
            "some"
        )  # OR self.testing_order_symbol3 if error returned

        self.assertErrorResult(result)  # , ErrorCode.WRONG_SYMBOL)

        # Close selected positions
        result = client.close_all_positions(self.testing_order_symbol)

        self.assertGoodResult(result)
        for item in result:
            self.assertPositionIsValid(item)
        self.assertPositionsCount(1)

        # (Create position back)
        order1 = self._create_order(
            symbol=self.testing_order_symbol, is_limit=False, is_buy=is_buy
        )
        self.assertPositionsCount(2)

        # Close all position
        result = client.close_all_positions()

        self.assertGoodResult(result)
        for item in result:
            self.assertPositionIsValid(item)
            self.assertFalse(item.is_open)
        if not self.is_show_closed_positions:
            self.assertPositionsCount(0)

        # Close if there is nothing to close
        result_x = client.close_all_positions()
        if self.is_show_closed_positions:
            self.assertGoodResult(result_x)
            if self.platform_id != Platform.OKEX:
                # Okex return all closed position
                self.assertGreaterEqual(
                    len(result_x),
                    2,
                    "Platform should return recently closed positions too.",
                )
            self.assertEqual(len([p for p in result_x if p.is_open]), 0)
            for item in result_x:
                self.assertPositionIsValid(item)
                self.assertFalse(item.is_open)
        else:
            self.assertPositionsCount(0)

    def test_close_all_positions__sell_positions(self):
        client = self.client_authed
        if not client.is_position_supported:
            self.skipTest(
                "Positions are not supported for current platform, so we cannot create sell position."
            )

        self.test_close_all_positions(False)


class TestPlatformRESTClientHistory(BaseTestRESTClient):
    # Test only history methods

    is_pagination_supported = True
    is_to_item_supported = True
    is_to_item_by_id = False

    # fetch_history
    #
    # FIXME: Doesn't work corretc way because of from_item and to_item ordering.
    # e.g. Bitfinex wants from_item always be less then to_item, but such ordering causes wrong behavior
    # in _filter_result method in case of descending sorting. So with desc sorting test will fall almost always
    def test_fetch_history_from_and_to_item(
        self, endpoint=Endpoint.TRADE, is_auth=True, timestamp_param=ParamName.TIMESTAMP
    ):
        client = self.client_authed if is_auth else self.client

        # Limit must be greater than max items with same timestamp (greater than 10 at least)
        limit = 50

        # (Get items to be used to set from_item, to_item params)
        result0 = result = client.fetch_history(
            endpoint, self.testing_symbol, sorting=Sorting.DESCENDING, limit=limit
        )

        self.assertGoodResult(result)
        logging.info("\n#0 %s %s", len(result), result)
        self.assertGreater(len(result), 2)
        if client.converter.IS_SORTING_ENABLED:
            self.assertGreater(result[0].timestamp, result[-1].timestamp)

        # Test FROM_ITEM and TO_ITEM
        result = client.fetch_history(
            endpoint,
            self.testing_symbol,
            sorting=Sorting.DESCENDING,  # limit=limit,
            from_item=result0[0],
            to_item=result0[-1],
        )

        # logging.info("\n#1 %s %s", len(result), result)
        self.assertGoodResult(result)
        self.assertGreater(len(result), 2)
        self.assertIn(result[0], result0, "Try restart tests.")
        # self.assertIn(result[-10], result0, "Try restart tests.")
        if self.is_to_item_supported:
            self.assertIn(result[-1], result0, "Try restart tests.")
        # self.assertEqual(len(result), len(result0))
        # self.assertEqual(result, result0)

        # Test FROM_ITEM and TO_ITEM in wrong order
        result = client.fetch_history(
            endpoint,
            self.testing_symbol,
            sorting=Sorting.DESCENDING,  # limit=limit,
            from_item=result0[-1],
            to_item=result0[0],
        )

        # logging.info("\n#2 %s %s", len(result), result)
        self.assertGoodResult(result)
        self.assertGreater(len(result), 2)
        self.assertIn(result[0], result0, "Try restart tests.")
        # self.assertIn(result[-10], result0, "Try restart tests.")
        if self.is_to_item_supported:
            self.assertIn(result[-1], result0, "Try restart tests.")
        # self.assertEqual(len(result), len(result0))
        # self.assertEqual(result, result0)

        # Test FROM_ITEM and TO_ITEM in wrong order and sorted differently
        result = client.fetch_history(
            endpoint,
            self.testing_symbol,
            sorting=Sorting.ASCENDING,  # limit=limit,
            from_item=result0[-1],
            to_item=result0[0],
        )

        # logging.info("\n#3 %s %s", len(result), result)
        self.assertGoodResult(result)
        self.assertGreater(len(result), 2)
        self.assertIn(result[0], result0, "Try restart tests.")
        # self.assertIn(result[-10], result0, "Try restart tests.")
        if self.is_to_item_supported:
            self.assertIn(result[-1], result0, "Try restart tests.")
        # self.assertEqual(len(result), len(result0))
        # self.assertEqual(result, result0)

    def test_fetch_history_with_all_params(
        self, endpoint=Endpoint.TRADE, is_auth=True, timestamp_param=ParamName.TIMESTAMP
    ):
        client = self.client_authed if is_auth else self.client

        # (Get items to be used to set from_item, to_item params)
        # Test SYMBOL and LIMIT
        self.assertEqual(client.converter.sorting, Sorting.DESCENDING)
        limit = 10
        result = client.fetch_history(endpoint, self.testing_symbol, limit)

        self.assertGoodResult(result)
        self.assertEqual(len(result), limit)
        if client.converter.IS_SORTING_ENABLED:
            self.assertGreater(result[0].timestamp, result[-1].timestamp)
        # logging.info("TEMP result %s", result)

        # Test FROM_ITEM and TO_ITEM
        from_item = result[1]
        to_item = result[-2]
        logging.info("Get history from_item: %s to_item: %s", from_item, to_item)
        result = client.fetch_history(
            endpoint, self.testing_symbol, from_item=from_item, to_item=to_item
        )

        # logging.info("TEMP result: %s", result)
        self.assertGoodResult(result)
        if self.is_to_item_supported:
            if self.is_to_item_by_id:
                self.assertEqual(len(result), limit - 2)
            self.assertEqual(result[-1].timestamp, to_item.timestamp)

        # Test SORTING, get default_result_len
        result = client.fetch_history(
            endpoint, self.testing_symbol, sorting=Sorting.ASCENDING
        )

        self.assertGoodResult(result)
        self.assertGreater(len(result), limit)
        if client.converter.IS_SORTING_ENABLED:
            self.assertLess(result[0].timestamp, result[-1].timestamp)
        default_result_len = len(result)

        # Test IS_USE_MAX_LIMIT
        result = client.fetch_history(
            endpoint, self.testing_symbol, is_use_max_limit=True
        )

        self.assertGoodResult(result)
        self.assertGreaterEqual(len(result), default_result_len)

        # Test SYMBOL param as a list
        if self.testing_symbol:
            # (Note: for Binance fetch_history(endpoint, ["some", "some"])
            # sends request without 2 SYMBOL get params which cases error.)
            # (Note: for BitMEX fetch_history(endpoint, [None, None])
            # sends request without SYMBOL get param which is usual request - so skip here.)
            result = client.fetch_history(
                endpoint, [self.testing_symbol, self.testing_symbol]
            )

            self.assertIsNotNone(result)
            # (Bitfinex returns [] on such error)
            if result:
                self.assertErrorResult(result)

    # fetch_trades

    test_fetch_trades = TestPlatformRESTClientCommon.test_fetch_trades
    test_fetch_trades_errors = TestPlatformRESTClientCommon.test_fetch_trades_errors
    test_fetch_trades_limit = TestPlatformRESTClientCommon.test_fetch_trades_limit
    test_fetch_trades_limit_is_too_big = (
        TestPlatformRESTClientCommon.test_fetch_trades_limit_is_too_big
    )
    test_fetch_trades_sorting = TestPlatformRESTClientCommon.test_fetch_trades_sorting

    # fetch_trades_history

    def test_fetch_trades_history(self):
        self.test_fetch_trades("fetch_trades_history")

    def test_fetch_trades_history_errors(self):
        self.test_fetch_trades_errors("fetch_trades_history")

    def test_fetch_trades_history_limit(self):
        self.test_fetch_trades_limit("fetch_trades_history")

    def test_fetch_trades_history_limit_is_too_big(self):
        self.test_fetch_trades_limit_is_too_big("fetch_trades_history")

    def test_fetch_trades_history_sorting(self):
        self.test_fetch_trades_sorting("fetch_trades_history")

    def test_fetch_trades_is_same_as_first_history(self):
        result = self.client_authed.fetch_trades(self.testing_symbol)
        result_history = self.client_authed.fetch_trades_history(self.testing_symbol)

        self.assertNotIsInstance(result, Error)
        self.assertGreater(len(result), 10)
        # self.assertIn(result_history[0], result, "Try restart")
        self.assertIn(result_history[10], result, "Try restart")
        self.assertIn(result[-1], result_history)
        self.assertEqual(
            result,
            result_history,
            "Can fail sometimes due to item added between requests",
        )

    # todo add tests for from_item is ItemObject
    def test_fetch_trades_history_over_and_over(self, sorting=None):
        if not self.is_pagination_supported:
            self.skipTest("Pagination is not supported by current platform version.")

        if self.is_sorting_supported and not sorting:
            self.test_fetch_trades_history_over_and_over(Sorting.DESCENDING)
            self.test_fetch_trades_history_over_and_over(Sorting.ASCENDING)
            return

        client = self.client_authed
        client.converter.is_use_max_limit = True

        logging.info(
            "Test trade paging with %s",
            "sorting: " + sorting
            if sorting
            else "default_sorting: " + client.converter.default_sorting,
        )
        if not sorting:
            sorting = client.converter.default_sorting

        # result = client.fetch_trades(self.testing_symbol, sorting=sorting)
        result = client.fetch_trades_history(self.testing_symbol)
        self.assertGoodResult(result)
        page_count = 1
        logging.info("Page: %s %s", page_count, self._result_info(result, sorting))

        while result and not isinstance(result, Error):
            prev_result = result
            from_item = result[-1]
            result = client.fetch_trades_history(
                self.testing_symbol, sorting=sorting, from_item=from_item
            )
            page_count += 1
            self.assertGoodResult(result)
            if isinstance(result, Error):
                # Rate limit error!
                logging.info("Page: %s error: %s", page_count, result)
            else:
                # Check next page
                logging.info(
                    "Page: %s %s", page_count, self._result_info(result, sorting)
                )
                self.assertGreater(len(result), 2)
                for item in result:
                    self.assertTradeIsValid(item)
                self.assertRightSymbols(result)
                if sorting == Sorting.ASCENDING:
                    # Oldest first
                    self.assertLess(
                        prev_result[0].timestamp,
                        prev_result[-1].timestamp,
                        "Error in sorting",
                    )  # Check sorting is ok
                    self.assertLess(
                        result[0].timestamp, result[-1].timestamp, "Error in sorting"
                    )  # Check sorting is ok
                    self.assertLessEqual(
                        prev_result[-1].timestamp,
                        result[0].timestamp,
                        "Error in paging",
                    )  # Check next page
                else:
                    # Newest first
                    self.assertGreater(
                        prev_result[0].timestamp,
                        prev_result[-1].timestamp,
                        "Error in sorting",
                    )  # Check sorting is ok
                    self.assertGreater(
                        result[0].timestamp, result[-1].timestamp, "Error in sorting"
                    )  # Check sorting is ok
                    self.assertGreaterEqual(
                        prev_result[-1].timestamp,
                        result[0].timestamp,
                        "Error in paging",
                    )  # Check next page

            if page_count > 2:
                logging.info("Break to prevent RATE_LIMIT error.")
                break

        logging.info("Pages count: %s", page_count)

    # For debugging only
    def test_just_logging_for_paging(
        self, method_name="fetch_trades_history", is_auth=False, sorting=None
    ):
        if self.is_sorting_supported and not sorting:
            self.test_just_logging_for_paging(method_name, is_auth, Sorting.DESCENDING)
            self.test_just_logging_for_paging(method_name, is_auth, Sorting.ASCENDING)
            return

        client = self.client_authed if is_auth else self.client
        logging.info(
            "Logging paging with %s",
            "sorting: " + sorting
            if sorting
            else "default_sorting: " + client.converter.default_sorting,
        )
        if not sorting:
            sorting = client.converter.default_sorting

        logging.info("\n==First page==")
        result0 = result = getattr(client, method_name)(
            self.testing_symbol, sorting=sorting
        )

        self.assertGoodResult(result)
        logging.info("_result_info: %s", self._result_info(result, sorting))

        logging.info("\n==Next page==")
        # logging.info("\nXXX %s", result0[-1].timestamp)
        # result0[-1].timestamp -= 100
        # logging.info("\nXXX %s", result0[-1].timestamp)
        result = getattr(client, method_name)(
            self.testing_symbol, sorting=sorting, from_item=result0[-1]
        )
        # logging.info("\nXXX %s %s", result0[0].timestamp, result0[-1].timestamp)
        # logging.info("\nYYY %s %s", result[0].timestamp, result[-1].timestamp)

        if result:
            # To check rate limit error
            self.assertGoodResult(result)
        logging.info("_result_info: %s", self._result_info(result, sorting))

        logging.info("\n==Failed page==")
        result = getattr(client, method_name)(
            self.testing_symbol, sorting=sorting, from_item=result0[0]
        )

        self.assertGoodResult(result)
        logging.info("_result_info: %s", self._result_info(result, sorting))


# WebSocket


class TestWSConverter(TestProtocolConverter):
    converter_class = WSConverter

    def test_generate_subscriptions__params(self):
        # Test different params combinations for:
        endpoints = [Endpoint.CANDLE, Endpoint.ORDER_BOOK]
        symbols = ["BTCUSD", "ETHUSD"]
        self.converter.endpoint_lookup = {
            Endpoint.CANDLE: "c:{symbol}:{interval}",
            Endpoint.ORDER_BOOK: "ob:{symbol}:{level}",
        }

        # Empty
        with self.assertRaises(Exception):
            result = self.converter.generate_subscriptions(endpoints, symbols)
        # self.assertEqual(result, [])

        params = {}
        with self.assertRaises(Exception):
            result = self.converter.generate_subscriptions(endpoints, symbols, **params)
        # self.assertEqual(result, [])

        # Simple
        params = {
            ParamName.INTERVAL: CandleInterval.MIN_1,
            ParamName.LEVEL: OrderBookDepthLevel.DEEP,
        }
        result = self.converter.generate_subscriptions(endpoints, symbols, **params)
        self.assertEqual(
            result, {"c:BTCUSD:1m", "c:ETHUSD:1m", "ob:BTCUSD:deep", "ob:ETHUSD:deep"}
        )

        # List
        params = {
            ParamName.INTERVAL: [CandleInterval.MIN_1, CandleInterval.HRS_1],
            ParamName.LEVEL: OrderBookDepthLevel.DEEP,
        }
        result = self.converter.generate_subscriptions(endpoints, symbols, **params)
        self.assertEqual(
            result,
            {
                "c:BTCUSD:1m",
                "c:ETHUSD:1m",
                "c:BTCUSD:1h",
                "c:ETHUSD:1h",
                "ob:BTCUSD:deep",
                "ob:ETHUSD:deep",
            },
        )

        # Two lists
        params = {
            ParamName.INTERVAL: [CandleInterval.MIN_1, CandleInterval.HRS_1],
            ParamName.LEVEL: [OrderBookDepthLevel.DEEP],
        }
        result = self.converter.generate_subscriptions(endpoints, symbols, **params)
        self.assertEqual(
            result,
            {
                "c:BTCUSD:1m",
                "c:ETHUSD:1m",
                "c:BTCUSD:1h",
                "c:ETHUSD:1h",
                "ob:BTCUSD:deep",
                "ob:ETHUSD:deep",
            },
        )

        params = {
            ParamName.INTERVAL: [CandleInterval.MIN_1, CandleInterval.HRS_1],
            ParamName.LEVEL: [OrderBookDepthLevel.DEEP, OrderBookDepthLevel.MEDIUM],
        }
        result = self.converter.generate_subscriptions(endpoints, symbols, **params)
        self.assertEqual(
            result,
            {
                "c:BTCUSD:1m",
                "c:ETHUSD:1m",
                "c:BTCUSD:1h",
                "c:ETHUSD:1h",
                "ob:BTCUSD:deep",
                "ob:ETHUSD:deep",
                "ob:BTCUSD:medium",
                "ob:ETHUSD:medium",
            },
        )

    def test_break_params_to_params_list(self):
        result = self.converter._break_params_to_params_list(None, None)
        self.assertEqual(result, [])

        result = self.converter._break_params_to_params_list(Endpoint.CANDLE, None)
        self.assertEqual(result, [])

        result = self.converter._break_params_to_params_list(Endpoint.CANDLE, {})
        self.assertEqual(result, [])

        result = self.converter._break_params_to_params_list(
            Endpoint.CANDLE, {ParamName.INTERVAL: CandleInterval.MIN_1}
        )
        self.assertEqual(result, [{ParamName.INTERVAL: CandleInterval.MIN_1}])

        result = self.converter._break_params_to_params_list(
            Endpoint.CANDLE,
            {ParamName.INTERVAL: [CandleInterval.MIN_1, CandleInterval.HRS_1]},
        )
        self.assertEqual(
            result,
            [
                {ParamName.INTERVAL: CandleInterval.MIN_1},
                {ParamName.INTERVAL: CandleInterval.HRS_1},
            ],
        )

        result = self.converter._break_params_to_params_list(
            Endpoint.CANDLE,
            {
                ParamName.INTERVAL: [CandleInterval.MIN_1, CandleInterval.HRS_1],
                ParamName.LEVEL: [OrderBookDepthLevel.DEEP],
            },
        )
        self.assertEqual(
            result,
            [
                {
                    ParamName.INTERVAL: CandleInterval.MIN_1,
                    ParamName.LEVEL: OrderBookDepthLevel.DEEP,
                },
                {
                    ParamName.INTERVAL: CandleInterval.HRS_1,
                    ParamName.LEVEL: OrderBookDepthLevel.DEEP,
                },
            ],
        )

        result = self.converter._break_params_to_params_list(
            Endpoint.CANDLE,
            {
                ParamName.INTERVAL: [CandleInterval.MIN_1, CandleInterval.HRS_1],
                ParamName.LEVEL: [OrderBookDepthLevel.DEEP, OrderBookDepthLevel.MEDIUM],
            },
        )
        self.assertEqual(
            result,
            [
                {
                    ParamName.INTERVAL: CandleInterval.MIN_1,
                    ParamName.LEVEL: OrderBookDepthLevel.DEEP,
                },
                {
                    ParamName.INTERVAL: CandleInterval.HRS_1,
                    ParamName.LEVEL: OrderBookDepthLevel.DEEP,
                },
                {
                    ParamName.INTERVAL: CandleInterval.MIN_1,
                    ParamName.LEVEL: OrderBookDepthLevel.MEDIUM,
                },
                {
                    ParamName.INTERVAL: CandleInterval.HRS_1,
                    ParamName.LEVEL: OrderBookDepthLevel.MEDIUM,
                },
            ],
        )

        # Test endpoint
        result = self.converter._break_params_to_params_list(
            Endpoint.ORDER_BOOK_AGG,
            {
                ParamName.INTERVAL: [CandleInterval.MIN_1, CandleInterval.HRS_1],
                ParamName.PLATFORM_ID: [3, 2],
            },
        )
        self.assertEqual(
            result,
            [
                {
                    ParamName.INTERVAL: CandleInterval.MIN_1,
                    ParamName.PLATFORM_ID: [3, 2],
                },
                {
                    ParamName.INTERVAL: CandleInterval.HRS_1,
                    ParamName.PLATFORM_ID: [3, 2],
                },
            ],
        )

        result = self.converter._break_params_to_params_list(
            Endpoint.CANDLE,
            {
                ParamName.INTERVAL: [CandleInterval.MIN_1, CandleInterval.HRS_1],
                ParamName.PLATFORM_ID: [3, 2],
            },
        )
        self.assertEqual(
            result,
            [
                {ParamName.INTERVAL: CandleInterval.MIN_1, ParamName.PLATFORM_ID: 3},
                {ParamName.INTERVAL: CandleInterval.HRS_1, ParamName.PLATFORM_ID: 3},
                {ParamName.INTERVAL: CandleInterval.MIN_1, ParamName.PLATFORM_ID: 2},
                {ParamName.INTERVAL: CandleInterval.HRS_1, ParamName.PLATFORM_ID: 2},
            ],
        )


class TestWSClient(TestClient):
    is_rest = False

    testing_symbols = ["ETHBTC", "BTCUSD"]
    received_items = None

    def setUp(self):
        self.skipIfBase()

        super().setUp()
        self.received_items = []

        def on_data(items):
            # if items:
            items = [item for item in items if isinstance(item, DataObject)]
            self.received_items.extend(items)

        self.client.on_data = on_data
        self.client_authed.on_data = on_data

    def tearDown(self):
        self.client.close()
        super().tearDown()

    def test_trade_1_channel(self):
        self._test_endpoint_channels(
            [Endpoint.TRADE], [self.testing_symbol], self.assertTradeIsValid
        )

    def test_trade_2_channels(self):
        self._test_endpoint_channels(
            [Endpoint.TRADE],
            self.testing_symbols,
            self.assertTradeIsValid,
            use_milliseconds=False,
        )

    def test_candle_1_channel(self):
        params = {ParamName.INTERVAL: CandleInterval.MIN_1}
        self._test_endpoint_channels(
            [Endpoint.CANDLE], [self.testing_symbol], self.assertCandleIsValid, params
        )

        # Make sure that it was parsed right
        candle = self.received_items[0]
        self.assertIsNotNone(candle.price_open)

    def test_candle_2_channels(self):
        params = {ParamName.INTERVAL: CandleInterval.MIN_1}
        self._test_endpoint_channels(
            [Endpoint.CANDLE],
            self.testing_symbols,
            self.assertCandleIsValid,
            params,
            use_milliseconds=False,
        )

        # Make sure that it was parsed right
        candle = self.received_items[0]
        self.assertIsNotNone(candle.price_open)

    def test_candle_4_channels__interval_as_list(self):
        params = {
            ParamName.INTERVAL: [CandleInterval.MIN_1, CandleInterval.HRS_1],
            ParamName.LEVEL: [
                OrderBookDepthLevel.MEDIUM,
                OrderBookDepthLevel.DEEP,
            ],  # Skipped
        }
        self._test_endpoint_channels(
            [Endpoint.CANDLE],
            self.testing_symbols,
            self.assertCandleIsValid,
            params,
            use_milliseconds=False,
            subscription_count=4,
        )

        # Make sure that it was parsed right
        candle = self.received_items[0]
        self.assertIsNotNone(candle.price_open)

    def test_ticker_1_channel(self):
        self._test_endpoint_channels(
            [Endpoint.TICKER], [self.testing_symbol], self.assertTickerIsValid
        )

    def test_ticker_2_channels(self):
        self._test_endpoint_channels(
            [Endpoint.TICKER],
            self.testing_symbols,
            self.assertTickerIsValid,
            use_milliseconds=False,
        )

    def test_ticker_all_channel(self):
        self._test_endpoint_channels(
            [Endpoint.TICKER_ALL], None, self.assertTickerIsValid
        )

    def test_order_book_1_channel(self):
        params = {ParamName.LEVEL: OrderBookDepthLevel.LIGHT}
        self._test_endpoint_channels(
            [Endpoint.ORDER_BOOK],
            [self.testing_symbol],
            self.assertOrderBookIsValid,
            params,
            item_number=2,
        )
        self.assertAnyOrderBookHasAsksAndBids(self.received_items)

    def test_order_book_2_channels(self):
        params = {ParamName.LEVEL: OrderBookDepthLevel.DEEP}
        self._test_endpoint_channels(
            [Endpoint.ORDER_BOOK],
            self.testing_symbols,
            self.assertOrderBookIsValid,
            params,
            use_milliseconds=False,
        )
        self.assertAnyOrderBookHasAsksAndBids(self.received_items)

    def test_quote_1_channel(self):
        self._test_endpoint_channels(
            [Endpoint.QUOTE],
            self.testing_symbols,
            self.assertQuoteIsValid,
            use_milliseconds=False,
            item_number=4,
        )
        self.assertAnyQuote(self.received_items, self.testing_symbols)

    # TODO remove diff
    def test_order_book_diff_1_channel(self):
        self._test_endpoint_channels(
            [Endpoint.ORDER_BOOK_DIFF],
            [self.testing_symbol],
            self.assertOrderBookDiffIsValid,
        )

    # TODO remove diff
    def test_order_book_diff_2_channels(self):
        self._test_endpoint_channels(
            [Endpoint.ORDER_BOOK_DIFF],
            self.testing_symbols,
            self.assertOrderBookDiffIsValid,
            use_milliseconds=False,
        )

    # TODO TEST UNSUBSCRIBE!!!
    def _test_endpoint_channels(
        self,
        endpoints,
        symbols,
        assertIsValidFun=None,
        params=None,
        is_auth=False,
        use_milliseconds=True,
        subscription_count=None,
        item_number=None,
    ):
        client = self.client_authed if is_auth else self.client
        client.use_milliseconds = use_milliseconds

        if not isinstance(endpoints, (list, tuple, set)):
            endpoints = [endpoints]
        if symbols and not isinstance(symbols, (list, tuple, set)):
            symbols = [symbols]
        if not subscription_count:
            subscription_count = (len(endpoints) if endpoints else 0) * (
                len(symbols) if symbols else 0
            )

        params = params if params else {}
        client.subscribe(endpoints, symbols, **params)

        self.waitAndAssertResults(
            self.received_items,
            endpoints,
            symbols,
            client=client,
            is_check_all_received=True,
            item_number=item_number,
        )
        # Assert item.subscription is OK
        for item in self.received_items.copy():
            self.assertIn(item.subscription, client.current_subscriptions)
        if self.platform_id not in [Platform.BITMEX]:
            # supports aggregated subscription
            self.assertEqual(len(client.current_subscriptions), subscription_count)
        # -
        # # todo wait for all endpoints and all symbols?
        # wait_for(self.received_items, timeout_sec=1000000)
        #
        # self.assertResults(self.received_items, endpoints, symbols, client=client,
        #                    is_check_all_received=False)
        #                    # is_check_all_received=True)

    def test_sequential_subscriptions_and_closing__trade_and_ticker(self):
        endpoints = [Endpoint.TRADE, Endpoint.TICKER]
        self._test_sequential_subscriptions_and_closing(endpoints, self.testing_symbols)

    def test_sequential_subscriptions_and_closing_orderbook(self):
        endpoints = [Endpoint.ORDER_BOOK]
        params = {ParamName.LEVEL: OrderBookDepthLevel.LIGHT}
        self._test_sequential_subscriptions_and_closing(
            endpoints, self.testing_symbols, params=params
        )

    def _test_sequential_subscriptions_and_closing(
        self,
        endpoints,
        symbols,
        assertIsValidFun=None,
        params=None,
        is_auth=False,
        use_milliseconds=True,
    ):
        client = self.client_authed if is_auth else self.client
        client.use_milliseconds = use_milliseconds

        if not isinstance(endpoints, (list, tuple)):
            endpoints = list(endpoints)
        if symbols and not isinstance(symbols, (list, tuple)):
            symbols = list(symbols)

        client.reconnect_count = max(1, len(endpoints) - 1)
        for endpoint, symbol in itertools.product(endpoints, symbols):
            params = params if params else {}
            client.subscribe([endpoint], [symbol], **params)
            time.sleep(5)

        self.waitAndAssertResults(
            self.received_items,
            endpoints,
            symbols,
            client=client,
            is_check_all_received=True,
        )
        # -
        # # wait_for(self.received_items, timeout_sec=1000000)
        # item_classes = self._get_item_classes_in_endpoints(endpoints)
        # wait_for_all(self.received_items, item_classes, symbols, timeout_sec=1000000)
        #
        # self.assertResults(self.received_items, endpoints, symbols, client=client,
        #                    is_check_all_received=True)

    def test_restoring_connection_on_error(self):
        attempts = 3
        while True:
            endpoints = [Endpoint.TRADE, Endpoint.TICKER]
            try:
                self._test_restoring_connection_on_error(
                    0, 0, endpoints, self.testing_symbols
                )
                return
            except:
                logging.warning(
                    "test_restoring_connection_on_error, Test failed, try again"
                )
                self.tearDown()
                attempts -= 1
                if attempts < 0:
                    raise

    def _test_restoring_connection_on_error(
        self,
        reconnect_delay_sec,
        reconnect_count,
        endpoints,
        symbols,
        assertIsValidFun=None,
        params=None,
        is_auth=False,
        use_milliseconds=True,
    ):
        failing_recv_data_frame = Mock(side_effect=Exception("SIMULATE DISCONNECT"))
        self._test_restoring_connection(
            failing_recv_data_frame,
            reconnect_delay_sec,
            reconnect_count,
            endpoints,
            symbols,
            assertIsValidFun,
            params,
            is_auth,
            use_milliseconds,
        )

    def test_restoring_connection_on_disconnect(self):
        # (reconnect_count is now ignored, other 0 value would fail)
        attempts = 3
        while True:
            endpoints = [Endpoint.TRADE, Endpoint.TICKER]
            try:
                self._test_restoring_connection_on_disconnect(
                    0, 0, endpoints, self.testing_symbols
                )
                return
            except:
                logging.warning(
                    "test_restoring_connection_on_disconnect, Test failed, try again"
                )
                self.tearDown()
                attempts -= 1
                if attempts < 0:
                    raise

    def _test_restoring_connection_on_disconnect(
        self,
        reconnect_delay_sec,
        reconnect_count,
        endpoints,
        symbols,
        assertIsValidFun=None,
        params=None,
        is_auth=False,
        use_milliseconds=True,
    ):
        failing_recv_data_frame = Mock(side_effect=((ABNF.OPCODE_CLOSE, None),) * 10)
        self._test_restoring_connection(
            failing_recv_data_frame,
            reconnect_delay_sec,
            reconnect_count,
            endpoints,
            symbols,
            assertIsValidFun,
            params,
            is_auth,
            use_milliseconds,
        )

    def _test_restoring_connection(
        self,
        failing_recv_data_frame,
        reconnect_delay_sec,
        reconnect_count,
        endpoints,
        symbols,
        assertIsValidFun=None,
        params=None,
        is_auth=False,
        use_milliseconds=True,
    ):
        DISCONNECT_COUNT = 2
        logging.info(
            "\n\nExpecting %s exceptions, %s reconnections, and then some data received "
            "for each endpoint and symbol from lists %s and %s\n\n",
            DISCONNECT_COUNT,
            DISCONNECT_COUNT,
            endpoints,
            symbols,
        )

        client = self.client_authed if is_auth else self.client
        client.use_milliseconds = use_milliseconds
        client.reconnect_delay_sec = reconnect_delay_sec
        # Note: Now, reconnect_count doesn't make any effect (see comment in WSClient._on_close())
        client.reconnect_count = reconnect_count

        if not isinstance(endpoints, (list, tuple)):
            endpoints = [endpoints]
        if symbols and not isinstance(symbols, (list, tuple)):
            symbols = [symbols]

        connection_count = 0

        def on_connect():
            nonlocal connection_count
            connection_count += 1
            if connection_count <= DISCONNECT_COUNT:
                logging.info("SIMULATING DISCONNECT by platform or ws client")
                client.ws.sock.recv_data_frame = failing_recv_data_frame

        # item_classes = [ProtocolConverter.item_class_by_endpoint[endpoint] for endpoint in endpoints]
        client.on_connect = on_connect
        params = params if params else {}
        client.subscribe(endpoints, symbols, **params)

        self.waitAndAssertResults(
            self.received_items,
            endpoints,
            symbols,
            client=client,
            is_check_all_received=True,
        )
        # -
        # # wait_for(self.received_items, timeout_sec=1000000)
        # item_classes = self._get_item_classes_in_endpoints(endpoints)
        # wait_for_all(self.received_items, item_classes, symbols, timeout_sec=1000000)
        #
        # self.assertResults(self.received_items, endpoints, symbols, client=client,
        #                    is_check_all_received=True)

    def waitAndAssertResults(
        self,
        received_items,
        endpoints=None,
        symbols=None,
        platform_ids=None,
        client=None,
        is_check_all_received=False,
        item_number=None,
    ):
        APITestCase.waitAndAssertResults(
            self,
            received_items,
            endpoints,
            symbols,
            platform_ids,
            client,
            is_check_all_received,
            item_number=item_number,
        )

    # def _get_item_classes_in_endpoints(self, endpoints):
    #     return {self.client.converter.item_class_by_endpoint[endpoint] for endpoint in endpoints}
    #
    # def assertResults(self, received_items, endpoints, symbols, client=None, assertIsValidFun=None,
    #                   is_check_all_received=False):
    #     self.assertGreaterEqual(len(received_items), 1)
    #
    #     item_classes = tuple(ProtocolConverter.item_class_by_endpoint[endpoint] for endpoint in endpoints)
    #     received_item_classes = set()
    #     received_symbols = set()
    #     for item in received_items:
    #         self.assertIsInstance(item, item_classes)
    #         if symbols:
    #             self.assertIn(item.symbol, symbols)
    #
    #         if client:
    #             self.assertEqual(client.use_milliseconds, item.is_milliseconds)
    #         if assertIsValidFun:
    #             assertIsValidFun(item, symbols)
    #
    #         received_item_classes.add(item.__class__)
    #         received_symbols.add(item.symbol)
    #
    #     if is_check_all_received:
    #         # Assert all endpoints and symbols subscribed
    #         self.assertEqual(received_item_classes, set(item_classes))


#         self.assertEqual(received_symbols, set(symbols))


class TestPrivateWSClient(TestClient):
    is_rest = False

    testing_symbols = ["ETHBTC", "BTCUSD"]
    received_items = None

    def setUp(self):
        self.skipIfBase()

        super().setUp()
        self.received_items = []
        self.open_order = None

        def on_data(items):
            # if items:
            items = [item for item in items if isinstance(item, DataObject)]
            self.received_items.extend(items)

        self.client.on_data = on_data
        self.client_authed.on_data = on_data

    def tearDown(self):
        self.client.close()
        delete_all_test_orders(
            self.platform_id, self.testing_symbol, pivot_symbol=self.pivot_symbol
        )
        super().tearDown()

    def test_balance_channel(self):
        self._test_endpoint_channels(
            [Endpoint.BALANCE],
            [self.pivot_symbol],
            self.assertBalanceIsValid,
            is_auth=True,
        )

    def test_position_channel(self):
        self._test_endpoint_channels(
            [Endpoint.POSITION],
            [self.testing_symbol],
            self.assertPositionIsValid,
            is_auth=True,
            make_trade=True,
        )

    def test_trade_my_channel(self):
        self._test_endpoint_channels(
            [Endpoint.TRADE_MY],
            [self.testing_symbol],
            self.assertTradeIsValid,
            is_auth=True,
            make_trade=True,
        )

    def test_order_channel(self):
        self._test_endpoint_channels(
            [Endpoint.ORDER],
            [self.testing_symbol],
            self.assertOrderIsValid,
            is_auth=True,
        )

    def _test_endpoint_channels(
        self,
        endpoints,
        symbols,
        assertIsValidFun=None,
        params=None,
        is_auth=False,
        use_milliseconds=True,
        subscription_count=None,
        make_trade=False,
    ):
        client = self.client_authed if is_auth else self.client
        client.use_milliseconds = use_milliseconds

        if not isinstance(endpoints, (list, tuple, set)):
            endpoints = [endpoints]
        if symbols and not isinstance(symbols, (list, tuple, set)):
            symbols = [symbols]

        client.subscribe(endpoints, symbols, **params or {})

        private_endpoints = [e for e in endpoints if Endpoint.check_is_private(e)]
        public_endpoints = [e for e in endpoints if not Endpoint.check_is_private(e)]
        if private_endpoints:
            time.sleep(2)
            if make_trade:
                self.open_order = create_test_order(
                    self.platform_id, self.testing_symbol, market=True
                )
                if Endpoint.POSITION in endpoints:
                    self.open_order = create_test_order(
                        self.platform_id, self.testing_symbol, market=True, side=1
                    )
            else:
                self.open_order = create_test_order(
                    self.platform_id, self.testing_symbol
                )
            self.waitAndAssertResults(
                self.received_items,
                private_endpoints,
                symbols,
                client=client,
                is_check_all_received=True,
                platform_ids=[self.platform_id],
            )
        if public_endpoints:
            self.waitAndAssertResults(
                self.received_items,
                public_endpoints,
                symbols,
                client=client,
                is_check_all_received=True,
            )
            # Assert item.subscription is OK
            for item in self.received_items.copy():
                self.assertIn(item.subscription, client.current_subscriptions)

    def waitAndAssertResults(
        self,
        received_items,
        endpoints=None,
        symbols=None,
        platform_ids=None,
        client=None,
        is_check_all_received=False,
    ):
        APITestCase.waitAndAssertResults(
            self,
            received_items,
            endpoints,
            symbols,
            platform_ids,
            client,
            is_check_all_received,
        )
