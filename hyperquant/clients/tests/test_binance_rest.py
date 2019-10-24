import unittest

from hyperquant.api import Endpoint, ParamName, Platform
from hyperquant.clients import Error, ErrorCode
from hyperquant.clients.binance import BinanceRESTConverterV1
from hyperquant.clients.tests.test_init import (TestPlatformRESTClientCommon,
                                                TestPlatformRESTClientHistory,
                                                TestPlatformRESTClientPrivate,
                                                TestProtocolConverter)

# REST


class BinanceSettingsMixInV1:
    platform_id = Platform.BINANCE
    # version = "1"

    testing_symbols = ["ETHBTC", "BNBBTC"]
    pivot_symbol = "BTC"
    testing_symbol, testing_symbol2 = testing_symbols
    testing_order_symbol, testing_order_symbol2 = testing_symbols
    is_support_fetch_orders_without_symbol = False

    # (All numbers taken from https://api.binance.com/api/v1/exchangeInfo for EOSETH.
    # LOT_SIZE error - amount is too small
    # MIN_PRICE error - price is too small
    # MIN_NOTIAL error - amount * price is too small
    buy_sell_amount = 0.63
    buy_sell_amount_for_second_symbol = buy_sell_amount
    is_show_closed_positions = False


class TestBinanceRESTConverterV1(TestProtocolConverter):
    converter_class = BinanceRESTConverterV1


class TestBinanceRESTClientCommonV1(BinanceSettingsMixInV1,
                                    TestPlatformRESTClientCommon):
    def test_ping(self, is_auth=False):
        super().test_ping(is_auth)

    def test_get_server_timestamp(self, is_auth=False):
        super().test_get_server_timestamp(is_auth)

    def test_get_symbols(self, is_auth=False):
        super().test_get_symbols(is_auth)

    def test_get_currecy_pairs(self, is_auth=False):
        super().test_get_currecy_pairs(is_auth)

    def test_fetch_trades(self, method_name="fetch_trades", is_auth=False):
        super().test_fetch_trades(method_name, is_auth)

    def test_fetch_trades_errors(self,
                                 method_name="fetch_trades",
                                 is_auth=False):
        super().test_fetch_trades_errors(method_name, is_auth)

    def test_fetch_trades_limit(self,
                                method_name="fetch_trades",
                                is_auth=False):
        super().test_fetch_trades_limit(method_name, is_auth)

    def test_fetch_trades_limit_is_too_big(self,
                                           method_name="fetch_trades",
                                           is_auth=False):
        super().test_fetch_trades_limit_is_too_big(method_name, is_auth)

    def test_fetch_trades_sorting(self,
                                  method_name="fetch_trades",
                                  is_auth=False):
        super().test_fetch_trades_sorting(method_name, is_auth)

    def test_fetch_candles(self):
        super().test_fetch_candles()

    def test_fetch_ticker(self):
        super().test_fetch_ticker()

    def test_fetch_tickers(self):
        super().test_fetch_tickers()

    def test_fetch_order_book(self):
        super().test_fetch_order_book()

    def test_fetch_quote(self):
        super().test_fetch_quote()


class TestBinanceRESTClientPrivateV1(BinanceSettingsMixInV1,
                                     TestPlatformRESTClientPrivate):
    @unittest.skip("not supported")
    def test_set_leverage(self):
        super().test_set_leverage()

    def test_get_account_info(self):
        super().test_get_account_info()

    def test_check_credentials(self):
        super().test_check_credentials()

    def test_fetch_balance(self):
        super().test_fetch_balance()

    def test_fetch_my_trades(self):
        super().test_fetch_my_trades()

    def test_create_order(self, is_test=False):
        super().test_create_order(is_test)

    def test_create_stop_orders(self):
        super().test_create_stop_orders()

    def test_cancel_order(self):
        super().test_cancel_order()

    def test_fetch_order(self):
        super().test_fetch_order()

    def test_fetch_orders(self):
        super().test_fetch_orders()

    def test_get_positions(self):
        super().test_get_positions()

    def test_close_all_positions(self, is_buy=True):
        super().test_close_all_positions()

    def test_cancel_all_orders(self, is_buy=True):
        super().test_cancel_all_orders()


class TestBinanceRESTClientHistoryV1(BinanceSettingsMixInV1,
                                     TestPlatformRESTClientHistory):

    is_to_item_by_id = True

    # fetch_history

    def test_fetch_history_from_and_to_item(
            self,
            endpoint=Endpoint.TRADE,
            is_auth=True,
            timestamp_param=ParamName.TIMESTAMP):
        super().test_fetch_history_from_and_to_item(endpoint, is_auth,
                                                    timestamp_param)

    def test_fetch_history_with_all_params(
            self,
            endpoint=Endpoint.TRADE,
            is_auth=True,
            timestamp_param=ParamName.TIMESTAMP):
        super().test_fetch_history_with_all_params(endpoint, is_auth,
                                                   timestamp_param)

    # fetch_trades

    # fetch_trades_history

    def test_fetch_trades_history(self):
        super().test_fetch_trades_history()

    def test_fetch_trades_history_errors(self):
        super().test_fetch_trades_history_errors()

        # Testing create_rest_client() which must set api_key for Binance
        result = self.client.fetch_trades_history(self.testing_symbol)

        self.assertIsNotNone(result)
        self.assertGoodResult(result)

        # Note: for Binance to get trades history you must send api_key
        self.client.set_credentials(None, None)
        result = self.client.fetch_trades_history(self.testing_symbol)

        self.assertIsNotNone(result)
        self.assertIsInstance(result, Error)
        self.assertEqual(result.code, ErrorCode.UNAUTHORIZED)

    def test_fetch_trades_history_limit(self):
        super().test_fetch_trades_history_limit()

    def test_fetch_trades_history_limit_is_too_big(self):
        super().test_fetch_trades_history_limit_is_too_big()

    def test_fetch_trades_history_sorting(self):
        super().test_fetch_trades_history_sorting()

    def test_fetch_trades_is_same_as_first_history(self):
        self.skipTest("Check other platforms (not very important.)")
        super().test_fetch_trades_is_same_as_first_history()

    def test_fetch_trades_history_over_and_over(self, sorting=None):
        super().test_fetch_trades_history_over_and_over(sorting)

    def test_just_logging_for_paging(self,
                                     method_name="fetch_trades_history",
                                     is_auth=False,
                                     sorting=None):
        super().test_just_logging_for_paging(method_name, True, sorting)
