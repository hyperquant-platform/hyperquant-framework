from unittest import skip

from hyperquant.api import Endpoint, Platform
from hyperquant.clients.binance_future import BinanceFutureWSConverterV1
from hyperquant.clients.tests.test_init import (TestPrivateWSClient,
                                                TestProtocolConverter,
                                                TestWSClient)


class TestBinanceWSConverterV1(TestProtocolConverter):
    converter_class = BinanceFutureWSConverterV1


class BinanceFutureSettingsMixIn:
    platform_id = Platform.BINANCE_FUTURE
    testing_symbols = ["BTCUSDT"]
    pivot_symbol = "USDT"
    testing_symbol = testing_symbols[0]
    testing_order_symbol = testing_symbols[0]
    testing_order_symbol2 = testing_symbols[0]


class TestBinanceFutureWSClientV1(BinanceFutureSettingsMixIn, TestWSClient):
    def test_candle_4_channels__interval_as_list(self):
        super().test_candle_4_channels__interval_as_list()

    def test_trade_1_channel(self):
        super().test_trade_1_channel()

    @skip('Currently only one Symbol on platform')
    def test_trade_2_channels(self):
        super().test_trade_2_channels()

    def test_candle_1_channel(self):
        super().test_candle_1_channel()

    @skip('Currently only one Symbol on platform')
    def test_candle_2_channels(self):
        super().test_candle_2_channels()

    def test_ticker_1_channel(self):
        super().test_ticker_1_channel()

    @skip('Currently only one Symbol on platform')
    def test_ticker_2_channels(self):
        super().test_ticker_2_channels()

    @skip("No such endpoint")
    def test_ticker_all_channel(self):
        super().test_ticker_all_channel()

    def test_order_book_1_channel(self):
        super().test_order_book_1_channel()

    @skip('Currently only one Symbol on platform')
    def test_order_book_2_channels(self):
        super().test_order_book_2_channels()

    def test_order_book_diff_1_channel(self):
        super().test_order_book_diff_1_channel()

    @skip('Currently only one Symbol on platform')
    def test_order_book_diff_2_channels(self):
        super().test_order_book_diff_2_channels()

    def test_sequential_subscriptions_and_closing__trade_and_ticker(self):
        super().test_sequential_subscriptions_and_closing__trade_and_ticker()

    def test_sequential_subscriptions_and_closing_orderbook(self):
        super().test_sequential_subscriptions_and_closing_orderbook()

    # Be careful with 2 last tests, sometimes it fails without any changes in code
    def test_restoring_connection_on_error(self):
        super().test_restoring_connection_on_error()

    def test_restoring_connection_on_disconnect(self):
        super().test_restoring_connection_on_disconnect()

    @skip('Nut supported by platform')
    def test_quote_1_channel(self):
        super().test_quote_1_channel()


class TestBinanceFutureWSClientV1Private(BinanceFutureSettingsMixIn, TestPrivateWSClient):

    def test_balance_channel(self):
        # Binance futures doesn't push balance updater on orders
        self._test_endpoint_channels(
            [Endpoint.BALANCE],
            [self.pivot_symbol],
            self.assertBalanceIsValid,
            is_auth=True,
            make_trade=True,
        )

    def test_position_channel(self):
        super().test_position_channel()

    def test_order_channel(self):
        super().test_order_channel()

    def test_trade_my_channel(self):
        super().test_trade_my_channel()

    def test_unsubscribe_private_endpoint(self):
        self._test_endpoint_channels([Endpoint.BALANCE], [self.pivot_symbol],
                                     self.assertBalanceIsValid,
                                     is_auth=True, make_trade=True)
        self.client_authed.unsubscribe([Endpoint.BALANCE], [self.pivot_symbol])
        self.assertEqual(len(self.client_authed.current_subscriptions), 0)
