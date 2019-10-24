import json
import logging
import time
from collections import Iterable
from decimal import Decimal
from unittest import TestCase


from hyperquant.api import (CandleInterval, Direction, Endpoint,
                            OrderBookDirection, OrderStatus, OrderType,
                            Platform, TransactionType, convert_items_to_obj,
                            item_format_by_endpoint)
from hyperquant.clients import (Account, Balance, BalanceTransaction, Candle,
                                ItemObject, MyTrade, Order, OrderBook,
                                OrderBookItem, Position, ProtocolConverter,
                                Quote, Ticker, Trade)
from hyperquant.utils import dict_util


def wait_for(value_or_callable, min_count=2, timeout_sec=10.):
    # Wait for value is of "min_count" length or "timeout_sec" elapsed.
    start_time = time.time()
    value, fun = (None, value_or_callable) if callable(value_or_callable) \
        else (value_or_callable, None)
    # print("\n### Waiting a list for min_count: %s%s or timeout_sec: %s" %
    #       (min_count, " or function is True" if fun else "", timeout_sec))
    while not timeout_sec or time.time() - start_time < timeout_sec:
        if fun:
            value = fun()
        if isinstance(value, bool):
            if value:
                # print("\n### Result is true: %s in %s seconds" %
                #       (value, time.time() - start_time))
                return
        else:
            value_count = value if isinstance(value, int) else len(value)
            if value_count >= min_count:
                print("\n### Count reached: %s of %s in %s seconds" %
                      (value_count, min_count, time.time() - start_time))
                return
            # print(
            #     "\n### Sleep... current min_count: %s of %s, %s seconds passed. Value: %s"
            #     % (value_count, min_count, time.time() - start_time, value))
        time.sleep(min(1, timeout_sec / 10) if timeout_sec else 1)
    print("\n### Time is out! (value)")
    raise Exception("Time is out!")


def wait_for_all(received_items,
                 item_classes=None,
                 symbols=None,
                 platform_ids=None,
                 timeout_sec=10,
                 is_check_all_received=False,
                 is_strict_binding=False,
                 item_number=None):
    def iterator(platform_ids, symbols, is_strict_binding=False):
        if not is_strict_binding:
            for platform_id in platform_ids:
                for symbol in symbols:
                    yield platform_id, symbol
        else:
            for platform_id, symbol in zip(platform_ids, symbols):
                yield platform_id, symbol

    if received_items is None:
        logging.warning("received_items cannot be None!")
        return
    if is_check_all_received and item_classes and platform_ids and symbols:
        item_classes = set(item_classes)
        for platform_id, symbol in iterator(platform_ids, symbols,
                                            is_strict_binding):
            logging.debug(
                "\n##### Waiting for item classes (endpoints): %s for platform_id: %s symbol: %s",
                item_classes, platform_id, symbol)
            wait_for(lambda: {
                i.__class__
                for i in (received_items()
                          if callable(received_items) else received_items)
                if i.platform_id == platform_id and i.symbol == symbol
            } >= item_classes,
                     timeout_sec=timeout_sec)
            if item_number is not None:
                logging.debug("Waiting for %s items", item_number)

                def count(platform_id, symbol, cnt):
                    found_items = 0
                    for i in received_items:
                        if i.platform_id == platform_id and i.symbol == symbol:
                            found_items += 1
                    return found_items >= cnt

                wait_for(lambda: count(platform_id, symbol, item_number),
                         timeout_sec)
    else:
        if platform_ids:
            # print("\n##### Waiting for all platform_ids:", platform_ids)
            wait_for(lambda: {
                i.platform_id
                for i in (received_items()
                          if callable(received_items) else received_items)
            } == set(platform_ids),
                     timeout_sec=timeout_sec / 2)
            if item_number is not None:
                logging.debug("Waiting for %s items", item_number)

                def count(platform_id, cnt):
                    found_items = 0
                    for i in received_items:
                        if i.platform_id == platform_id:
                            found_items += 1
                    return found_items >= cnt

                for platform_id in platform_ids:
                    wait_for(lambda: count(platform_id, item_number),
                             timeout_sec)
        if symbols:
            # print("\n##### Waiting for all symbols:", symbols)
            wait_for(lambda: {
                i.symbol
                for i in (received_items()
                          if callable(received_items) else received_items)
            } == set(symbols),
                     timeout_sec=timeout_sec / 2)
            if item_number is not None:
                logging.debug("Waiting for %s items", item_number)

                def count(symbol, cnt):
                    found_items = 0
                    for i in received_items:
                        if i.symbol == symbol:
                            found_items += 1
                    return found_items >= cnt

                for symbol in symbols:
                    wait_for(lambda: count(symbol, item_number), timeout_sec)
        if item_classes:
            # print("\n##### Waiting for item classes (endpoints):",
            #       item_classes)

            wait_for(lambda: {
                i.__class__
                for i in (received_items()
                          if callable(received_items) else received_items)
            } == set(item_classes),
                     timeout_sec=timeout_sec)


class APITestCase(TestCase):
    # To test any API: RESTful, WS or just classes in code.
    # Contains methods to check VOs as they are used everywhere in our system.
    # ?(Don't extend this class, but use functions in your test classes)

    def assertItemIsValid(self,
                          item,
                          testing_symbol_or_symbols=None,
                          platform_id=None,
                          is_with_item_id=True,
                          is_with_timestamp=True):
        self.assertIsNotNone(item)
        self.assertIsInstance(item, ItemObject, item)

        # Not empty
        self.assertIsNotNone(item.platform_id)
        self.assertIsNotNone(item.symbol)
        if is_with_timestamp:
            self.assertIsNotNone(item.timestamp)
        if is_with_item_id:
            self.assertIsNotNone(
                item.item_id
            )  # trade_id: binance, bitfinex - int converted to str; bitmex - str

        # Type
        if hasattr(item, "platform_ids"):
            for pl_id in item.platform_ids:
                self.assertIsInstance(pl_id, int)
        else:
            self.assertIsInstance(item.platform_id, int)
        self.assertIsInstance(item.symbol, str)
        self.assertRegex(item.symbol, "[A-Z]+", "Wrong symbol format!"
                         )  # to prevent "ETH/BTC", "ethbtc"-like formats
        if is_with_timestamp:
            self.assertTrue(isinstance(item.timestamp, (float, int)))
        if is_with_item_id:
            self.assertIsInstance(item.item_id, str)

        # Value
        if platform_id:
            self.assertEqual(item.platform_id, platform_id)
        if is_with_timestamp:
            self.assertGreater(item.timestamp, 1000000000)
            if item.is_milliseconds:
                self.assertGreater(item.timestamp, 10000000000)
        if testing_symbol_or_symbols:
            self.assertEqual(item.symbol, item.symbol.upper())
            if isinstance(testing_symbol_or_symbols, str):
                self.assertEqual(item.symbol, testing_symbol_or_symbols)
            else:
                self.assertIn(item.symbol, testing_symbol_or_symbols)
        if is_with_item_id:
            self.assertGreater(len(str(item.item_id)), 0)

    def assertTradeIsValid(self,
                           trade,
                           testing_symbol_or_symbols=None,
                           platform_id=None,
                           is_dict=False):
        if is_dict and trade:
            trade = Trade(**trade)
        APITestCase.assertItemIsValid(self, trade, testing_symbol_or_symbols,
                                      platform_id, True)
        self.assertIsInstance(trade, Trade)

        # Not empty
        self.assertIsNotNone(trade.amount)
        self.assertIsNotNone(trade.price)
        # self.assertIsNotNone(trade.direction)

        # Type
        self.assertIsInstance(trade.amount, Decimal)
        self.assertIsInstance(trade.price, Decimal)
        if trade.direction is not None:
            self.assertIsInstance(trade.direction, int)

        # Value
        self.assertGreater(trade.amount, 0)
        self.assertGreater(trade.price, 0)
        if trade.direction is not None:
            self.assertIn(trade.direction, Direction.name_by_value)

    def assertMyTradeIsValid(self,
                             my_trade,
                             testing_symbol_or_symbols=None,
                             platform_id=None,
                             is_dict=False):
        if is_dict and my_trade:
            my_trade = MyTrade(**my_trade)

        APITestCase.assertTradeIsValid(self, my_trade,
                                       testing_symbol_or_symbols, platform_id,
                                       is_dict)

        self.assertIsInstance(my_trade, MyTrade)

        # Not empty
        self.assertIsNotNone(my_trade.order_id)
        # self.assertIsNotNone(my_trade.fee)
        # self.assertIsNotNone(my_trade.rebate)

        # Type
        self.assertIsInstance(my_trade.order_id, str)
        if my_trade.fee is not None:
            self.assertIsInstance(my_trade.fee, Decimal)
        if my_trade.rebate is not None:
            self.assertIsInstance(my_trade.rebate, Decimal)

        # Value
        if my_trade.fee is not None:
            self.assertGreater(my_trade.fee, 0)
        if my_trade.rebate is not None:
            self.assertGreater(my_trade.rebate, 0)

    def assertCandleIsValid(self,
                            candle,
                            testing_symbol_or_symbols=None,
                            platform_id=None,
                            is_dict=False):
        if is_dict and candle:
            candle = Candle(**candle)

        APITestCase.assertItemIsValid(self, candle, testing_symbol_or_symbols,
                                      platform_id, False)

        self.assertIsInstance(candle, Candle)

        # Not empty
        if candle.platform_id != Platform.BITMEX:
            self.assertIsNotNone(candle.interval)
        # self.assertIsNotNone(candle.price_open)
        # self.assertIsNotNone(candle.price_close)
        # self.assertIsNotNone(candle.price_high)
        # self.assertIsNotNone(candle.price_low)
        # Optional
        # self.assertIsNotNone(candle.amount)
        # self.assertIsNotNone(candle.trades_count)

        # Type
        if candle.platform_id != Platform.BITMEX:
            self.assertIsInstance(candle.interval, str)
        if candle.price_open is not None:
            self.assertIsInstance(candle.price_open, Decimal)
            self.assertIsInstance(candle.price_close, Decimal)
            self.assertIsInstance(candle.price_high, Decimal)
            self.assertIsInstance(candle.price_low, Decimal)
        if candle.volume is not None:
            self.assertIsInstance(candle.volume, Decimal)
        if candle.trades_count is not None:
            self.assertIsInstance(candle.trades_count, int)

        # Value
        if candle.platform_id not in [Platform.BITMEX, Platform.COINSUPER]:
            self.assertIn(candle.interval, CandleInterval.ALL)
        if candle.price_open is not None:
            self.assertGreater(candle.price_open, 0)
            self.assertGreater(candle.price_close, 0)
            self.assertGreater(candle.price_high, 0)
            self.assertGreater(candle.price_low, 0)
        else:
            self.assertIsNone(candle.price_open)
            self.assertIsNone(candle.price_close)
            self.assertIsNone(candle.price_high)
            self.assertIsNone(candle.price_low)
        if candle.volume is not None:
            if candle.price_open is None:
                self.assertEqual(candle.volume, 0)
            elif candle.platform_id == Platform.OKEX:
                self.assertGreaterEqual(candle.volume, 0)
            else:
                if candle.platform_id not in [Platform.BITMEX]:
                    self.assertGreater(candle.volume, 0)
                else:
                    self.assertGreaterEqual(candle.volume, 0)
        if candle.trades_count is not None:
            if candle.price_open is None:
                self.assertEqual(candle.trades_count, 0)
            else:
                # binance indeed has zero-volumed candles
                if candle.platform_id not in [
                        Platform.BINANCE, Platform.BITMEX
                ]:
                    self.assertGreater(candle.trades_count, 0)
                else:
                    self.assertGreaterEqual(candle.trades_count, 0)

    def assertTickerIsValid(self,
                            ticker,
                            testing_symbol_or_symbols=None,
                            platform_id=None,
                            is_dict=False):
        if is_dict and ticker:
            ticker = Ticker(**ticker)

        APITestCase.assertItemIsValid(self, ticker, testing_symbol_or_symbols,
                                      platform_id, False, False)

        self.assertIsInstance(ticker, Ticker, ticker)

        # Not empty
        self.assertIsNotNone(ticker.price, ticker)

        # Type
        # todo decimal only
        self.assertIsInstance(ticker.price, Decimal)

        # Value
        if platform_id in [Platform.OKEX, Platform.BILAXY, Platform.BITMEX]:
            # Okex send ticker with 0 price
            # For example LIGHT_BTC
            # Bilaxy send ticker with 0 price
            # For example SPRK/ETH
            # Binance example: https://www.bitmex.com/chartEmbed?symbol=XBT7D_D95
            self.assertGreaterEqual(ticker.price, 0)
        else:
            self.assertGreater(ticker.price, 0)

    def assertOrderBookIsValid(self,
                               order_book,
                               testing_symbol_or_symbols=None,
                               platform_id=None,
                               is_dict=False,
                               is_diff=False):
        if is_dict and order_book:
            order_book = OrderBook(**order_book)

        # Assert order book
        APITestCase.assertItemIsValid(self, order_book,
                                      testing_symbol_or_symbols, platform_id,
                                      False, False)

        self.assertIsInstance(order_book, OrderBook)
        self.assertIsNotNone(order_book.asks)
        self.assertIsNotNone(order_book.bids)
        # # if is_diff:
        self.assertGreaterEqual(len(order_book.asks), 0)
        self.assertGreaterEqual(len(order_book.bids), 0)
        # # For order book diff
        # self.assertGreater(len(order_book.asks + order_book.bids), 0)
        # # else:
        # #     self.assertGreater(len(order_book.asks), 0)
        # #     self.assertGreater(len(order_book.bids), 0)

        # Assert order book items
        for item in order_book.asks:
            APITestCase.assertOrderBookItemIsValid(self, item,
                                                   testing_symbol_or_symbols,
                                                   platform_id)
        for item in order_book.bids:
            APITestCase.assertOrderBookItemIsValid(self, item,
                                                   testing_symbol_or_symbols,
                                                   platform_id)

    def assertQuoteIsValid(self,
                           quote,
                           testing_symbol_or_symbols=None,
                           platform_id=None,
                           is_dict=False,
                           is_diff=False):

        if is_dict and quote:
            quote = Quote(**quote)

        # Assert order book
        APITestCase.assertItemIsValid(self, quote, testing_symbol_or_symbols,
                                      platform_id, False, False)

        self.assertIsInstance(quote, Quote)
        self.assertIsNotNone(quote.bestask)
        self.assertIsNotNone(quote.bestbid)

    def assertAnyQuote(
            self,
            quotes,
            testing_symbol_or_symbols=None,
    ):
        for quote in quotes:
            self.assertQuoteIsValid(quote, testing_symbol_or_symbols)

    def assertAnyOrderBookHasAsksAndBids(self,
                                         order_books,
                                         max_len: int = None):
        is_asks_ok = False
        is_bids_ok = False
        for order_book in order_books:
            if len(order_book.asks) > 0 and (max_len is None or
                                             len(order_book.asks) <= max_len):
                is_asks_ok = True
            if len(order_book.bids) > 0 and (max_len is None or
                                             len(order_book.asks) <= max_len):
                is_bids_ok = True
        self.assertTrue(is_asks_ok)
        self.assertTrue(is_bids_ok)

    def assertOrderBookDiffIsValid(self,
                                   order_book,
                                   testing_symbol_or_symbols=None,
                                   platform_id=None,
                                   is_dict=False):
        APITestCase.assertOrderBookIsValid(self,
                                           order_book,
                                           testing_symbol_or_symbols,
                                           platform_id,
                                           is_dict,
                                           is_diff=True)

    def assertOrderBookItemIsValid(self,
                                   order_book_item,
                                   testing_symbol_or_symbols=None,
                                   platform_id=None,
                                   is_dict=False):
        if is_dict and order_book_item:
            order_book_item = OrderBookItem(**order_book_item)

        APITestCase.assertItemIsValid(self, order_book_item,
                                      testing_symbol_or_symbols, platform_id,
                                      False, False)

        self.assertIsInstance(order_book_item, OrderBookItem)

        # Not empty
        self.assertIsNotNone(order_book_item.amount, order_book_item)
        self.assertIsNotNone(order_book_item.price, order_book_item)
        # self.assertIsNotNone(order_book_item.direction)
        # self.assertIsNotNone(order_book_item.orders_count)

        # Type
        self.assertIsInstance(order_book_item.amount, Decimal)
        self.assertIsInstance(order_book_item.price, Decimal)
        if order_book_item.direction is not None:
            self.assertIsInstance(order_book_item.direction, int)
        if order_book_item.orders_count is not None:
            self.assertIsInstance(order_book_item.orders_count, int)

        # Value
        self.assertGreaterEqual(order_book_item.amount, 0)
        self.assertGreater(order_book_item.price, 0)
        if order_book_item.direction is not None:
            self.assertIn(order_book_item.direction,
                          OrderBookDirection.name_by_value)
        if order_book_item.orders_count is not None:
            self.assertGreaterEqual(order_book_item.orders_count, 0)

    def assertAccountIsValid(self, account, platform_id=None, is_dict=False):
        if is_dict and account:
            account = Account(**account)

        self.assertIsInstance(account, Account)

        # Not empty
        self.assertIsNotNone(account.platform_id)
        self.assertIsNotNone(account.timestamp)
        # self.assertIsNotNone(account.balances)

        # Type
        self.assertIsInstance(account.platform_id, int)
        self.assertIsInstance(account.timestamp, (int, float))
        # self.assertIsInstance(account.balances, list)

        # Value
        self.assertEqual(account.platform_id, platform_id)
        self.assertGreater(account.timestamp, 1000000000)
        if account.is_milliseconds:
            self.assertGreater(account.timestamp, 10000000000)
        # self.assertGreaterEqual(len(account.balances), 0)
        # for balance in account.balances:
        #     APITestCase.assertBalanceIsValid(self, balance, platform_id)
        # # for debug
        # balances_with_money = [balance for balance in account.balances if balance.amount_available or balance.amount_reserved]
        # print("balances_with_money:", balances_with_money, datetime.datetime.now().isoformat())
        # pass

    def assertBalanceIsValid(self, balance, platform_id=None, is_dict=False):
        if is_dict and balance:
            balance = Balance(**balance)

        self.assertIsInstance(balance, Balance)

        # Not empty
        self.assertIsNotNone(balance.platform_id)
        self.assertIsNotNone(balance.symbol)
        self.assertIsNotNone(balance.amount_available)
        # self.assertIsNotNone(balance.amount_reserved)

        # Type
        self.assertIsInstance(balance.platform_id, int)
        self.assertIsInstance(balance.symbol, str)
        self.assertIsInstance(balance.amount_available, Decimal)
        if balance.amount_reserved is not None:
            self.assertIsInstance(balance.amount_reserved, Decimal)

        # Value
        if platform_id:
            self.assertEqual(balance.platform_id, platform_id)
        self.assertEqual(balance.symbol, balance.symbol.upper())
        self.assertGreaterEqual(balance.amount_available, 0)
        if balance.amount_reserved is not None:
            self.assertGreaterEqual(balance.amount_reserved, 0)

    def assertBalanceTransactionIsValid(self,
                                        transaction,
                                        platform_id=None,
                                        is_dict=False):
        if is_dict and transaction:
            transaction = BalanceTransaction(**transaction)

        self.assertIsInstance(transaction, BalanceTransaction)

        # Not empty
        self.assertIsNotNone(transaction.platform_id)
        self.assertIsNotNone(transaction.symbol)
        self.assertIsNotNone(transaction.transaction_type)
        self.assertIsNotNone(transaction.amount)
        # self.assertIsNotNone(transaction.fee)

        # Type
        self.assertIsInstance(transaction.platform_id, int)
        self.assertIsInstance(transaction.symbol, str)
        self.assertIsInstance(transaction.transaction_type, int)
        self.assertIsInstance(transaction.amount, Decimal)
        if transaction.fee is not None:
            self.assertIsInstance(transaction.fee, Decimal)

        # Value
        if platform_id:
            self.assertEqual(transaction.platform_id, platform_id)
        self.assertEqual(transaction.symbol, transaction.symbol.upper())
        self.assertIn(transaction.transaction_type, TransactionType.ALL)

    def assertOrderIsValid(self,
                           order,
                           testing_symbol_or_symbols=None,
                           platform_id=None,
                           is_dict=False):
        if is_dict and order:
            order = Order(**order)

        APITestCase.assertItemIsValid(self, order, testing_symbol_or_symbols,
                                      platform_id, True)
        self.assertIsInstance(order, Order)

        # Not empty
        if platform_id not in [
                Platform.OKEX, Platform.BITTREX, Platform.COINSUPER,
                Platform.BILAXY
        ]:
            # Okex, Bittrex, Coinsuper api doesn't fill the field 'user_order_id'
            self.assertIsNotNone(order.user_order_id)
        if platform_id not in [Platform.BILAXY, Platform.COINSUPER]:
            # Sometimes None in Coinsuper
            self.assertIsNotNone(order.order_type)
        if order.order_type == OrderType.MARKET:
            # (None: Binance,)
            # (Not None: BitMEX,)
            # self.assertIsNone(order.price)
            pass
        elif order.order_type in [
                OrderType.TAKE_PROFIT_MARKET, OrderType.STOP_MARKET
        ]:
            self.assertIsNotNone(order.price_stop)
        else:
            self.assertIsNotNone(order.price)
        if order.platform_id not in (
                Platform.BITMEX,
                Platform.COINSUPER):  # (Sometimes is None in BitMEX)
            self.assertIsNotNone(order.amount_original)
            self.assertIsNotNone(order.amount_executed)
        if order.platform_id != Platform.BITMEX:  # (Sometimes is None in BitMEX)
            self.assertIsNotNone(order.direction)
        self.assertIsNotNone(order.order_status)

        # Type
        # Not empty
        if platform_id not in [
                Platform.OKEX, Platform.BITTREX, Platform.COINSUPER,
                Platform.BILAXY
        ]:
            # Okex, Bittrex, Coinsuper api doesn't fill the field 'user_order_id'
            self.assertIsInstance(order.user_order_id, str)
        if platform_id is not Platform.COINSUPER:
            # `cause sometimes platform return null in order_type
            self.assertIsInstance(order.order_type, int)
        if not (order.order_type == OrderType.MARKET or (order.order_type in [
                OrderType.TAKE_PROFIT_MARKET, OrderType.STOP_MARKET
        ] and order.price is None)):
            self.assertIsInstance(order.price, Decimal)
        if order.amount_original is not None:
            self.assertIsInstance(order.amount_original, Decimal)
        if order.amount_executed is not None:
            self.assertIsInstance(order.amount_executed, Decimal)
        if order.direction is not None:
            self.assertIsInstance(order.direction, int)
        self.assertIsInstance(order.order_status, int)

        # Value
        if platform_id is not Platform.COINSUPER:
            # `cause sometimes platform return null in order_type
            self.assertIn(order.order_type, OrderType.name_by_value)
        if order.order_type in [
                OrderType.MARKET, OrderType.TAKE_PROFIT_MARKET,
                OrderType.STOP_MARKET
        ]:
            # # For market order, price may vary, so price can be 0
            # # self.assertGreaterEqual(order.price, 0, order)
            # self.assertIsNone(order.price)
            pass
        else:
            if not (platform_id == Platform.COINSUPER
                    or order.order_type is None):
                self.assertGreater(order.price, 0, order)
        if order.amount_original is not None:
            self.assertGreater(order.amount_original, 0)
        if order.amount_executed is not None:
            self.assertGreaterEqual(order.amount_executed, 0)
        if order.direction is not None:
            self.assertIn(order.direction, Direction.name_by_value)
        self.assertIn(order.order_status, OrderStatus.name_by_value)

        # Check some properties (getters)
        if order.amount_original is not None:
            if order.is_new:
                self.assertGreater(order.amount_original, 0)
                self.assertEqual(order.amount_executed, 0)
            elif order.is_partially_filled:
                self.assertGreater(order.amount_original, 0)
                self.assertGreater(order.amount_executed, 0)
                self.assertGreater(order.amount_original,
                                   order.amount_executed)
            if order.is_filled:
                self.assertGreater(order.amount_original, 0)
                self.assertEqual(order.amount_original, order.amount_executed)

            if order.amount_original is not None and order.amount_executed is not None:
                self.assertEqual(order.amount_original,
                                 order.amount_executed + order.amount_left)
                self.assertLessEqual(
                    order.is_new + order.is_partially_filled + order.is_filled,
                    1)

            # Check consistency (state properties)
            self.assertEqual(order.is_open + order.is_closed, 1)

    def assertPositionIsValid(self,
                              position,
                              testing_symbol_or_symbols=None,
                              platform_id=None,
                              is_dict=False):
        if is_dict and position:
            position = Position(**position)

        self.assertIsInstance(position, Position)

        # Not empty
        self.assertIsNotNone(position.platform_id)
        self.assertIsNotNone(position.symbol)
        self.assertIsNotNone(position.amount, position)
        # self.assertIsNotNone(position.direction)

        # Type
        self.assertIsInstance(position.platform_id, int)
        self.assertIsInstance(position.symbol, str)
        self.assertIsInstance(position.amount, Decimal)
        if position.direction is not None:
            self.assertIsInstance(position.direction, int)

        # Value
        if platform_id:
            self.assertEqual(position.platform_id, platform_id)
        self.assertEqual(position.symbol, position.symbol.upper())
        if testing_symbol_or_symbols:
            self.assertEqual(position.symbol,
                             testing_symbol_or_symbols.upper())
        self.assertGreaterEqual(position.amount, 0)
        if position.direction is not None:
            self.assertIn(position.direction, Direction.name_by_value)
            self.assertEqual(position.is_buy + position.is_sell, 1)

    assert_by_item_class = {
        Trade: assertTradeIsValid,
        MyTrade: assertMyTradeIsValid,
        Candle: assertCandleIsValid,
        Ticker: assertTickerIsValid,
        OrderBook: assertOrderBookIsValid,
        Account: assertAccountIsValid,
        Balance: assertBalanceIsValid,
        Order: assertOrderIsValid,
        Position: assertPositionIsValid,
    }

    def waitAndAssertResults(self,
                             received_items,
                             endpoints=None,
                             symbols=None,
                             platform_ids=None,
                             client=None,
                             is_check_all_received=False,
                             is_exact=False,
                             is_strict_binding=False,
                             item_number=None):
        item_classes = tuple(ProtocolConverter.item_class_by_endpoint[endpoint]
                             for endpoint in endpoints) if endpoints else None

        # Wait
        # # Needed at least 2 items (if the 1st and the next items parsed differently)
        # wait_for(received_items, 2, timeout_sec=1000000)
        wait_for_all(received_items,
                     item_classes,
                     symbols,
                     platform_ids,
                     timeout_sec=50,
                     is_check_all_received=is_check_all_received,
                     is_strict_binding=is_strict_binding,
                     item_number=item_number)
        APITestCase.assertResults(self, received_items, endpoints, symbols,
                                  platform_ids, client, is_check_all_received,
                                  is_exact, is_strict_binding)

    def assertResults(self,
                      received_items,
                      endpoints=None,
                      symbols=None,
                      platform_ids=None,
                      client=None,
                      is_check_all_received=False,
                      is_exact=False,
                      is_strict_binding=False):
        def iterator(platform_ids, symbols, is_strict_binding=False):
            if not is_strict_binding:
                for platform_id in platform_ids:
                    for symbol in symbols:
                        yield platform_id, symbol
            else:
                for platform_id, symbol in zip(platform_ids, symbols):
                    yield platform_id, symbol

        if callable(received_items):
            received_items = received_items()
        # print("\n\nassertResults received_items:", received_items)

        item_classes = tuple(ProtocolConverter.item_class_by_endpoint[endpoint]
                             for endpoint in endpoints) if endpoints else None

        # Assert not empty
        self.assertIsNotNone(received_items)
        self.assertGreaterEqual(len(received_items), 1)

        # Assert each item
        for item in received_items:
            # todo refactor removing "client" out of here
            if client:
                self.assertEqual(client.use_milliseconds, item.is_milliseconds)

            assertIsValidFun = APITestCase.assert_by_item_class.get(
                item.__class__)
            if assertIsValidFun:
                assertIsValidFun(self, item)

        # Assert at least one item for each defined param is received,
        # and there is no item which is not expected by these params
        if item_classes:
            APITestCase._assertMatched(self,
                                       {i.__class__
                                        for i in received_items}, item_classes,
                                       is_exact, received_items)
        if platform_ids:
            APITestCase._assertMatched(self,
                                       {i.platform_id
                                        for i in received_items}, platform_ids,
                                       is_exact, received_items)
        if symbols:
            APITestCase._assertMatched(self,
                                       {i.symbol
                                        for i in received_items}, symbols,
                                       is_exact, received_items)

        # Assert all expected items are received:
        # at least one item of each endpoint for each platform_id & symbol combination
        if is_check_all_received and platform_ids and symbols:
            # Assert all endpoints, platforms and symbols subscribed
            for platform_id, symbol in iterator(platform_ids, symbols,
                                                is_strict_binding):
                received_item_classes = {
                    i.__class__
                    for i in received_items
                    if i.platform_id == platform_id and i.symbol == symbol
                }
                APITestCase._assertMatched(self, received_item_classes,
                                           item_classes, is_exact,
                                           received_items)

    def _assertMatched(self,
                       received_items,
                       expected_items,
                       is_exact=False,
                       msg=None):
        # If is_exact, check all unique expected_items == unique received_items
        # If not is_exact, check all unique expected_items are in unique received_items
        if not expected_items:
            return
        if is_exact:
            self.assertEqual(set(received_items), set(expected_items), msg)
        else:
            self.assertGreaterEqual(set(received_items), set(expected_items),
                                    msg)
            # (same)
            self.assertTrue(
                set(received_items).issuperset(set(expected_items)), msg)
