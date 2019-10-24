from unittest import skip

from hyperquant.api import Endpoint, Platform
from hyperquant.clients.binance import BinanceWSConverterV1
from hyperquant.clients.tests.test_init import (TestPrivateWSClient,
                                                TestProtocolConverter,
                                                TestWSClient)


class TestBinanceWSConverterV1(TestProtocolConverter):
    converter_class = BinanceWSConverterV1


class BinanceSettingsMixIn:
    platform_id = Platform.BINANCE
    pivot_symbol = "BTC"
    testing_symbol = "ETHBTC"  # As more popular
    testing_symbols = ["ETHBTC", "BNBBTC"]
    # version = "1"


class TestBinanceWSClientV1(BinanceSettingsMixIn, TestWSClient):
    def test_candle_4_channels__interval_as_list(self):
        super().test_candle_4_channels__interval_as_list()

    def test_trade_1_channel(self):
        super().test_trade_1_channel()

    def test_trade_2_channels(self):
        super().test_trade_2_channels()

    def test_candle_1_channel(self):
        super().test_candle_1_channel()

    def test_candle_2_channels(self):
        super().test_candle_2_channels()

    def test_ticker_1_channel(self):
        super().test_ticker_1_channel()

    def test_ticker_2_channels(self):
        super().test_ticker_2_channels()

    @skip("see below")
    def test_ticker_all_channel(self):
        # TODO что то там неправильно с эндпоинтами в клиенте, надо поправлять
        super().test_ticker_all_channel()

    def test_order_book_1_channel(self):
        super().test_order_book_1_channel()

    def test_order_book_2_channels(self):
        super().test_order_book_2_channels()

    def test_order_book_diff_1_channel(self):
        super().test_order_book_diff_1_channel()

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

    def test_quote_1_channel(self):
        super().test_quote_1_channel()


class TestBinanceWSClientV1Private(BinanceSettingsMixIn, TestPrivateWSClient):
    def test_balance_channel(self):
        super().test_balance_channel()

    def test_position_channel(self):
        super().test_position_channel()

    def test_order_channel(self):
        super().test_order_channel()

    def test_trade_my_channel(self):
        super().test_trade_my_channel()

    def test_unsubscribe_private_endpoint(self):
        self._test_endpoint_channels([Endpoint.BALANCE], [self.pivot_symbol],
                                     self.assertBalanceIsValid,
                                     is_auth=True)
        self.client_authed.unsubscribe([Endpoint.BALANCE], [self.pivot_symbol])
        self.assertEqual(len(self.client_authed.current_subscriptions), 0)
