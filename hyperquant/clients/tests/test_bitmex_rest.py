import logging
import time
import unittest
from decimal import Decimal

from hyperquant.api import Endpoint, ParamName, Platform
from hyperquant.clients import Candle, Order, OrderBookItem, Ticker, Trade
from hyperquant.clients.bitmex import BitMEXRESTConverterV1
from hyperquant.clients.tests.test_init import (TestPlatformRESTClientCommon,
                                                TestPlatformRESTClientHistory,
                                                TestPlatformRESTClientPrivate,
                                                TestProtocolConverter)

# REST


class BitMEXSettingsMixIn:
    platform_id = Platform.BITMEX
    version = "1"
    testing_symbol = "XBTUSD"
    testing_symbol2 = "ETHUSD"
    testing_symbols = [
        testing_symbol, testing_symbol2
    ]  # BitMEX returns all symbols if symbol param is not specified

    testing_order_symbol = "XBTUSD"  # Choose for testing orders because it has no commission
    # testing_order_symbol = "XRPM19"  # Choose for testing orders because it has no commission
    testing_order_symbol2 = "ETHUSD"
    buy_sell_amount = 1
    buy_sell_amount_for_second_symbol = buy_sell_amount
    # -is_limit_price_for_market_orders_available = True

    is_sorting_supported = True

    has_limit_error = True
    is_symbol_case_sensitive = True

    sleep_between_tests_sec = 3


class TestBitMEXRESTConverterV1(TestProtocolConverter):
    converter_class = BitMEXRESTConverterV1


class TestBitMEXRESTClientV1Common(BitMEXSettingsMixIn,
                                   TestPlatformRESTClientCommon):
    @unittest.skip("not implemented")
    def test_ping(self, is_auth=False):
        super().test_ping(is_auth)

    @unittest.skip("not implemented")
    def test_get_server_timestamp(self, is_auth=False):
        super().test_get_server_timestamp(is_auth)

    def test_get_symbols(self, is_auth=False):
        super().test_get_symbols(is_auth)

    def test_get_currecy_pairs(self, is_auth=False):
        super().test_get_currecy_pairs(is_auth)

    def test_fetch_trades(self, method_name="fetch_trades", is_auth=False):
        super().test_fetch_trades(method_name, is_auth)

    @unittest.skip("Platform doesn't complain on wrong symbol")
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

    # Specific for BitMEX

    def test__price_real(self):
        client = self.client

        custom_symbols = ["XBTUSD", "ETHUSD"]

        # Tickers
        # Custom price_real
        result = client.fetch_ticker("XBTUSD")
        self.assertGoodResult(result, False)
        self.assertEqual(result.price_real, round(1 / result.price, 8))

        result = client.fetch_ticker("ETHUSD")
        self.assertGoodResult(result, False)
        self.assertEqual(result.price_real, result.price * Decimal("0.000001"))

        # Common price_real
        result = client.fetch_tickers()
        self.assertGoodResult(result, False)
        for item in result:
            if item.symbol not in custom_symbols:
                self.assertEqual(item.price_real, item.price)

        # Test all item objects with prices with fixed values
        param_names_by_item_class = {
            Ticker: [ParamName.PRICE],
            OrderBookItem: [ParamName.PRICE],
            Order: [ParamName.PRICE],
            Trade: [ParamName.PRICE],
            Candle: [
                ParamName.PRICE_OPEN,
                ParamName.PRICE_CLOSE,
                ParamName.PRICE_HIGH,
                ParamName.PRICE_LOW,
            ],
        }
        for item_class, param_names in param_names_by_item_class.items():
            item = item_class(platform_id=self.platform_id)
            for param_name in param_names:
                # Custom symbol (unique formula)
                # (Fixed values from https://www.bitmex.com/app/contract/XBTUSD)
                item.symbol = "XBTUSD"
                #  (Getter)
                setattr(item, param_name, Decimal("10852.20"))
                self.assertEqual(getattr(item, param_name + "_real"),
                                 Decimal("0.00009215"))
                self.assertEqual(round(1 / Decimal("10852.20"), 11),
                                 Decimal("0.00009214721"))
                #  (Setter)
                setattr(item, param_name, 0)
                setattr(item, param_name + "_real", Decimal("0.00009215"))
                self.assertEqual(getattr(item, param_name),
                                 Decimal("10851.87194791"))
                self.assertEqual(round(1 / Decimal("10852.20"), 8),
                                 Decimal("0.00009215"))

                # (Fixed values from https://www.bitmex.com/app/contract/ETHUSD)
                item.symbol = "ETHUSD"
                #  (Getter)
                setattr(item, param_name, Decimal("307.41"))
                self.assertEqual(getattr(item, param_name + "_real"),
                                 Decimal("0.00030741"))
                #  (Setter)
                setattr(item, param_name, 0)
                setattr(item, param_name + "_real", Decimal("0.00030741"))
                self.assertEqual(getattr(item, param_name), Decimal("307.41"))

                # Common symbol
                # (Fixed values from https://www.bitmex.com/app/contract/ADAM19)
                item.symbol = "ADAM19"
                #  (Getter)
                setattr(item, param_name, Decimal("0.00000894"))
                self.assertEqual(getattr(item, param_name + "_real"),
                                 Decimal("0.00000894"))
                #  (Setter)
                setattr(item, param_name, 0)
                setattr(item, param_name + "_real", Decimal("0.00000894"))
                self.assertEqual(getattr(item, param_name),
                                 Decimal("0.00000894"))


class TestBitMEXRESTClientV1Private(BitMEXSettingsMixIn,
                                    TestPlatformRESTClientPrivate):
    is_possible_fetch_my_trades_without_symbols = True

    def setUp(self):
        super().setUp()
        time.sleep(5) # Make tests longer but try not to get rate limit

    def test_set_leverage(self):
        super().test_set_leverage()

    def test_get_account_info(self):
        super().test_get_account_info()

    def test_check_credentials(self):
        super().test_check_credentials()

    def test_fetch_balance(self):
        super().test_fetch_balance()

    def test_fetch_balance_transactions(self):
        super().test_fetch_balance_transactions()

    def test_fetch_my_trades(self):
        super().test_fetch_my_trades()

    def test_create_order(self, is_test=False):
        super().test_create_order(is_test)

    def test_create_stop_orders(self):
        super().test_create_stop_orders()

    def test_cancel_order(self):
        super().test_cancel_order()

    def test_cancel_all_orders(self, is_buy=True):
        super().test_cancel_all_orders(is_buy)

    def test_fetch_order(self):
        super().test_fetch_order()

    def test_fetch_orders(self):
        super().test_fetch_orders()

    def test_get_positions(self):
        super().test_get_positions()

    def test_close_position(self, is_buy=True):
        super().test_close_position(is_buy)

    def test_close_position__sell_positions(self):
        super().test_close_position(False)

    def test_close_all_positions(self, is_buy=True):
        super().test_close_all_positions(is_buy)

    def test_close_all_positions__sell_positions(self):
        super().test_close_all_positions(False)

    def test_common(self):
        client = self.client_authed
        symbols = [self.testing_symbol, "ETHUSD"]

        client.converter.secured_endpoints += [
            "user/wallet", "user/walletHistory", "user/walletSummary"
        ]
        result1 = client._send("GET", "user/wallet")
        result2 = client._send("GET", "user/walletHistory")
        result2_2 = [
            r for r in result2 if r.get("transactType") != "RealisedPNL"
        ]

        pnls = sum([
            r.get("amount") for r in result2
            if r.get("transactType") == "RealisedPNL"
        ])
        withdrawals = sum([
            r.get("amount") for r in result2
            if r.get("transactType") == "Withdrawal"
        ])
        deposits = sum([
            r.get("amount") for r in result2
            if r.get("transactType") == "Deposit"
        ])
        total = withdrawals + deposits
        total2 = withdrawals + deposits + pnls

        result3 = client._send("GET", "user/walletSummary")
        result4 = client.fetch_balance_transactions()
        result4_2 = client.fetch_balance_transactions(10, 2)
        result4_3 = client.fetch_balance_transactions(10, 3)
        result4_4 = client.fetch_balance_transactions(20, 1)
        test = result4_4 == result4_2 + result4_3
        result4_5 = client.fetch_balance_transactions(is_only_by_user=True)

        data0 = self._show_info(symbols)

        # print("\n\n=== 1 ===\n")
        order1 = self._create_position(self.testing_symbol)

        data1 = self._show_info(symbols)

        # print("\n\n=== 2 ===\n")
        order2 = self._create_position(self.testing_symbol)

        data2 = self._show_info(symbols)

        # print("\n\n=== 3 ===\n")
        order3 = self._create_position("ETHUSD")

        data3 = self._show_info(symbols)

        # print("\n\n=== 4 ===\n")
        result4 = client.close_all_positions()

        data4 = self._show_info(symbols)
        # CHECK BALANCE CHANGED!!!!!!


    def _show_info(self, symbols=None):
        client = self.client_authed

        time.sleep(self.wait_before_fetch_s)

        # print("\n\n---\n")
        balances = self.client_authed.fetch_balance()
        logging.info(f"### Balances: {balances}")

        positions = client.get_positions()
        logging.info(f"### Positions: {positions}")

        all_my_trades = []
        if symbols:
            for symbol in symbols:
                my_trades = client.fetch_my_trades(symbol)
                logging.info(f"### For: {symbol} My trades: {my_trades}")
                all_my_trades.append(my_trades)
        # print("\n\n---\n")
        return balances, positions, all_my_trades


class TestBitMEXRESTClientV1History(BitMEXSettingsMixIn,
                                    TestPlatformRESTClientHistory):
    testing_symbols = None  # BitMEX returns data for all symbols if symbol param is not specified


    @unittest.skip('TODO first fix DESC sorting')
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

    # todo return error in client
    # Or just stay empty list
    def test_fetch_trades_errors(self, method_name="fetch_trades", is_auth=False):
        client = self.client_authed if is_auth else self.client
        # Wrong symbol
        result = getattr(client, method_name)(self.wrong_symbol)
        # Empty list instead of error
        self.assertEqual(result, [])
        if self.is_symbol_case_sensitive:
            # Symbol in lower case as wrong symbol
            result = getattr(client, method_name)(self.testing_symbol2.lower())

            self.assertIsNotNone(result)
            self.assertEqual(result, [])


    def test_fetch_trades_history(self):
        super().test_fetch_trades_history()

    def test_fetch_trades_history_errors(self):
        super().test_fetch_trades_history_errors()

    def test_fetch_trades_history_limit(self):
        super().test_fetch_trades_history_limit()

    def test_fetch_trades_history_limit_is_too_big(self):
        super().test_fetch_trades_history_limit_is_too_big()

    def test_fetch_trades_history_sorting(self):
        super().test_fetch_trades_history_sorting()

    def test_fetch_trades_is_same_as_first_history(self):
        super().test_fetch_trades_is_same_as_first_history()

    @unittest.skip('TODO first fix DESC sorting')
    def test_fetch_trades_history_over_and_over(self, sorting=None):
        super().test_fetch_trades_history_over_and_over(sorting)

    def test_just_logging_for_paging(self,
                                     method_name="fetch_trades_history",
                                     is_auth=False,
                                     sorting=None):
        super().test_just_logging_for_paging(method_name, is_auth, sorting)
