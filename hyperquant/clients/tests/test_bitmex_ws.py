# WebSocket
import unittest

from hyperquant.api import Platform
from hyperquant.clients.bitmex import BitMEXWSConverterV1
from hyperquant.clients.tests.test_init import (TestProtocolConverter,
                                                TestWSClient, TestPrivateWSClient)

# WebSocket


class BitMEXSettingsMixIn:
    platform_id = Platform.BITMEX
    version = "1"

    testing_symbol = "XBTUSD"
    pivot_symbol = "XBT"
    testing_symbols = ["ETHUSD", "XBTUSD"]


class TestBitMEXWSConverterV1(TestProtocolConverter):
    converter_class = BitMEXWSConverterV1


class TestBitMEXWSClientV1Common(BitMEXSettingsMixIn, TestWSClient):
    def test_sequential_subscriptions_and_closing__trade_and_ticker(self):
        super().test_sequential_subscriptions_and_closing__trade_and_ticker()

    def test_restoring_connection_on_error(self):
        super().test_restoring_connection_on_error()

    def test_restoring_connection_on_disconnect(self):
        super().test_restoring_connection_on_disconnect()

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

    def test_ticker_all_channel(self):
        super().test_ticker_all_channel()

    def test_order_book_1_channel(self):
        super().test_order_book_1_channel()

    def test_order_book_2_channels(self):
        super().test_order_book_2_channels()

    def test_quote_1_channel(self):
        super().test_quote_1_channel()

    @unittest.skip("todo remove diff at all")
    def test_order_book_diff_1_channel(self):
        super().test_order_book_diff_1_channel()

    @unittest.skip("todo remove diff at all")
    def test_order_book_diff_2_channels(self):
        super().test_order_book_diff_2_channels()


class TestBitMEXWSClientV1Private(BitMEXSettingsMixIn, TestPrivateWSClient):

    def test_balance_channel(self):
        super().test_balance_channel()

    def test_position_channel(self):
        super().test_position_channel()

    def test_trade_my_channel(self):
        super().test_trade_my_channel()

    def test_order_channel(self):
        super().test_order_channel()
