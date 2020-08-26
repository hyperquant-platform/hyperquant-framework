import logging

from hyperquant.api import Endpoint, ParamName, Platform, OrderStatus, Sorting, TransactionType
from hyperquant.clients import Error, ErrorCode
from hyperquant.clients.tests.test_init import (TestPlatformRESTClientCommon,
                                                TestPlatformRESTClientHistory,
                                                TestPlatformRESTClientPrivate,
                                                )

# REST


class BinanceFutureSettingsMixInV1:
    platform_id = Platform.BINANCE_FUTURE
    version = "1"

    testing_symbols = ["BTCUSDT"]
    pivot_symbol = "USDT"
    testing_symbol = testing_symbols[0]
    testing_order_symbol = testing_symbols[0]
    testing_order_symbol2 = testing_symbols[0]
    is_support_fetch_orders_without_symbol = False
    is_symbol_case_sensitive = False
    is_market_orders_possible_to_cancel = False
    has_limit_error = True
    # is_show_closed_positions = False
    sleep_between_tests_sec = 2


class TestBinanceRESTClientCommonV1(BinanceFutureSettingsMixInV1,
                                    TestPlatformRESTClientCommon):
    def test_ping(self, is_auth=False):
        super().test_ping(is_auth)

    def test_get_server_timestamp(self, is_auth=False):
        super().test_get_server_timestamp(is_auth)

    def test_get_symbols(self, is_auth=False):
        client = self.client_authed if is_auth else self.client

        result = client.get_symbols()

        # as far as it returns dict of Symbol objects
        self.assertIsInstance(result, list)
        self.assertGreaterEqual(len(result), 1)
        self.assertIsInstance(result[0], str)
        self.assertEqual(result, [symbol.upper() for symbol in result])
        if self.testing_symbol:
            self.assertIn(self.testing_symbol, result)

    def test_get_currecy_pairs(self, is_auth=False):
        super().test_get_currecy_pairs(is_auth)

    def test_fetch_trades(self, method_name="fetch_trades", is_auth=True):
        super().test_fetch_trades(method_name, is_auth)

    def test_fetch_trades_errors(self,
                                 method_name="fetch_trades",
                                 is_auth=True):
        super().test_fetch_trades_errors(method_name, is_auth)

    def test_fetch_trades_limit(self,
                                method_name="fetch_trades",
                                is_auth=True):
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
        client = self.client
        result = client.fetch_tickers()
        self.assertGoodResult(result)
        self.assertGreaterEqual(len(result), 1)
        for item in result:
            self.assertTickerIsValid(item)

        # Full params
        result = client.fetch_tickers(self.testing_symbols)

        self.assertGoodResult(result)
        self.assertEqual(len(result), len(self.testing_symbols))
        for item in result:
            self.assertTickerIsValid(item, self.testing_symbols)

    def test_fetch_order_book(self):
        super().test_fetch_order_book()

    def test_fetch_quote(self):
        super().test_fetch_quote()


class TestBinanceFutureRESTClientPrivateV1(BinanceFutureSettingsMixInV1,
                                           TestPlatformRESTClientPrivate):

    def test_set_leverage(self):
        leverages = [1, 125, 20]
        client = self.client_authed
        for leverage in leverages:
            result = client.set_leverage(leverage, self.testing_symbol)
            self.assertGoodResult(result, is_iterable=False)
            self.assertAlmostEqual(result["leverage"], leverage)
        client.set_leverage(0, self.testing_symbol)

    def test_get_account_info(self):
        client = self.client_authed
        result = client.get_account_info()

        self.assertGoodResult(result, is_iterable=False)
        self.assertAccountIsValid(result, has_timestamp=False)

    def test_check_credentials(self):
        super().test_check_credentials()

    def test_fetch_balance(self):
        super().test_fetch_balance()

    def test_fetch_balance_transactions(self):
        client = self.client_authed

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

        # Paging
        result10_page1 = client.fetch_balance_transactions(limit=10)
        result10_page2 = client.fetch_balance_transactions(limit=10, to_time=result10_page1[-1].timestamp)
        self.assertEqual(result10_page2[0].timestamp, result10_page1[-1].timestamp)

        # Test that last page returns []
        while result:
            result = client.fetch_balance_transactions(limit=1000, to_time=result[-1].timestamp - 1)
            for item in result:
                self.assertBalanceTransactionIsValid(item)
            if not isinstance(result, list):
                break
        self.assertEqual(result, [])

        # is_direct
        result = client.fetch_balance_transactions()
        transaction_types = [item.transaction_type for item in result]
        self.assertIn(TransactionType.REALISED_PNL, transaction_types)

        result = client.fetch_balance_transactions(is_direct=True)
        transaction_types = [item.transaction_type for item in result]
        self.assertNotIn(TransactionType.REALISED_PNL, transaction_types)

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
        client = self.client_authed
        self.assertIsNotNone(self.testing_order_symbol)
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

        # (Tear down before next test)
        client.close_all_positions()

    def test_close_position(self, is_buy=True):
        client = self.client_authed
        self.assertIsNotNone(self.testing_order_symbol)
        # (Create positions)
        order1 = self._create_order(
            symbol=self.testing_order_symbol, is_limit=False, is_buy=is_buy
        )
        result0 = self.assertPositionsCount(1)

        # Close position
        self.assertTrue(result0[0].is_open)
        result = client.close_position(result0[0])

        self.assertGoodResult(result, False)
        if self.is_show_closed_positions:
            self.assertPositionIsValid(result)
            self.assertFalse(result.is_open)

        # Close if there is nothing to close
        result = client.close_position(result0[0])

        if self.is_show_closed_positions:
            self.assertGoodResult(result, False)
            self.assertPositionIsValid(result)  # self.assertIsNone(result)
            self.assertFalse(result.is_open)

    def test_close_all_positions(self, is_buy=True):

        client = self.client_authed

        self.assertIsNotNone(self.testing_order_symbol)
        # (Create position)
        logging.info("\n\nCreating position 1 %s", self.testing_order_symbol)
        order1 = self._create_order(
            symbol=self.testing_order_symbol, is_limit=False, is_buy=is_buy
        )
        result0 = self.assertPositionsCount(1)

        # Close selected positions
        result = client.close_all_positions(self.testing_order_symbol)

        self.assertGoodResult(result)
        for item in result:
            self.assertPositionIsValid(item)

        # (Create position back)
        order1 = self._create_order(
            symbol=self.testing_order_symbol, is_limit=False, is_buy=is_buy
        )

        # Close all position
        result = client.close_all_positions()

        self.assertGoodResult(result)
        for item in result:
            self.assertPositionIsValid(item)
            self.assertFalse(item.is_open)

        # Close if there is nothing to close
        result_x = client.close_all_positions()
        if self.is_show_closed_positions:
            self.assertGoodResult(result_x)
            self.assertEqual(len([p for p in result_x if p.is_open]), 0)
            for item in result_x:
                self.assertPositionIsValid(item)
                self.assertFalse(item.is_open)
        else:
            self.assertPositionsCount(0)

    def _create_several_orders(self):
        order1 = self._create_order(self.testing_order_symbol, is_buy=True)
        order2 = self._create_order(self.testing_order_symbol, is_buy=True)
        self.client_authed.close_all_positions()
        return order1, order2

    def assertOrdersCount(self, expected_count, is_open_only=True, is_wait=True):
        if is_wait:
            self.wait_before_fetch()

        client = self.client_authed
        if self.is_support_fetch_orders_without_symbol:
            result = client.fetch_orders(is_open_only=is_open_only)
        else:
            result = client.fetch_orders(self.testing_symbol, is_open_only=is_open_only)
        self.assertGoodResult(
            result, is_iterable=expected_count > 0
        )  # (Don't check len(result) if expecting 0 orders)
        self.assertGreaterEqual(len(result), expected_count)
        return result

    def test_cancel_all_orders(self, is_buy=True):
        client = self.client_authed

        # Nothing to close
        if self.is_support_fetch_orders_without_symbol:
            orders = client.fetch_orders(is_open_only=True)
        else:
            orders = client.fetch_orders(
                symbol=self.testing_order_symbol, is_open_only=True
            )
        self.assertGoodResult(orders, False)
        self.assertEqual(orders, [])

        if self.is_support_fetch_orders_without_symbol:
            result = client.cancel_all_orders()
        else:
            result = client.cancel_all_orders(self.testing_symbol)

        self.assertGoodResult(result, False)
        self.assertEqual(result, [])

        # Full params
        self._create_several_orders()

        self.assertOrdersCount(2)
        for item in result:
            self.assertFalse(item.is_closed)

        result = client.cancel_all_orders(self.testing_order_symbol)
        for order in result:
            for _order in list(self.created_orders):
                if order.item_id == _order.item_id:
                    self.created_orders.remove(_order)

        self.assertGoodResultForCanceledOrder(result)
        for item in result:
            self.assertEqual(item.symbol, self.testing_order_symbol)
            self.assertTrue(
                item.is_closed, "Check CANCELED status is supported for the platform."
            )
            self.assertIn(item.order_status, OrderStatus.closed)
            self.assertEqual(item.order_status, OrderStatus.CANCELED)

        self.assertOrdersCount(0)

        # Same by item_id and symbol
        # if self.platform_id == Platform.OKEX:
        order1 = self._create_order(
            self.testing_order_symbol, is_limit=True, is_buy=is_buy
        )
        self.assertOrdersCount(1)  # Includes waiting for order added

        if self.is_support_fetch_orders_without_symbol:
            orders = client.fetch_orders(is_open_only=True)
        else:
            orders = client.fetch_orders(
                symbol=self.testing_order_symbol, is_open_only=True
            )
        self.assertGreaterEqual(len({o.symbol for o in orders}), 1)

        if self.is_support_fetch_orders_without_symbol:
            result = client.cancel_all_orders()
        else:
            result = client.cancel_all_orders(self.testing_symbol)

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


class TestBinanceFutureRESTClientHistoryV1(BinanceFutureSettingsMixInV1,
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

    def test_fetch_trades_history(self, is_auth=False):
        super().test_fetch_trades_history(is_auth=True)

    def test_fetch_trades_history_errors(self, is_auth=False):
        super().test_fetch_trades_history_errors(is_auth=True)

        # Testing create_rest_client() which must set api_key for Binance
        result = self.client_authed.fetch_trades_history(self.testing_symbol)

        self.assertIsNotNone(result)
        self.assertGoodResult(result)

    def test_fetch_trades_history_limit(self, is_auth=True):
        super().test_fetch_trades_history_limit(is_auth=True)

    def test_fetch_trades_history_limit_is_too_big(self, is_auth=False):
        super().test_fetch_trades_history_limit_is_too_big(is_auth=True)

    def test_fetch_trades_history_sorting(self, is_auth=False):
        super().test_fetch_trades_history_sorting(is_auth=True)

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
