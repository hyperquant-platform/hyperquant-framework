import datetime
import logging
import os
import sys
import time

from hyperquant.api import Direction, OrderType, ParamName, Platform
from hyperquant.clients import Error, PrivatePlatformRESTClient, SingleDataAggregator
from hyperquant.clients.utils import (get_credentials_for,
                                      get_or_create_rest_client)

logger = logging.getLogger(__name__)

# Utility


# TODO use django logging settings
def set_up_logging(is_debug=True, is_file=False, level=None):
    logging_format = "%(asctime)s %(levelname)s:%(name)s: %(message)s"
    params = {
        "filename": _generate_log_filename()
    } if is_file else {
        "stream": sys.stdout
    }

    if isinstance(level, str):
        level = getattr(logging, level.upper(), None)
    if level is None:
        level = logging.DEBUG if is_debug else logging.INFO

    logging.basicConfig(level=level, format=logging_format, **params)


def _generate_log_filename():
    datetime_str = "{0:%Y%m%d_%H%M%S}".format(datetime.datetime.now())
    dir_name = "logs/"
    if not os.path.exists(dir_name):
        os.mkdir(dir_name)
    return "%s%s_%s.log" % (dir_name, os.path.basename(
        sys.argv[0]), datetime_str)


def wait_for_history(history_connector, timeout_sec=10):
    # Wait for item_list is of "count" length or "timeout_sec" elapsed.
    start_time = time.time()
    logger.debug("\n### Waiting a history_connector or timeout_sec: %s" %
                 timeout_sec)
    while not timeout_sec or time.time() - start_time < timeout_sec:
        if not history_connector.is_in_progress:
            if history_connector.is_complete:
                logger.debug(
                    "\n### All (or no) history retrieved in: %s seconds" %
                    (time.time() - start_time))
            else:
                logger.debug(
                    "\n### All history closed complete. Worked: %s seconds" %
                    (time.time() - start_time))
            return True
        time.sleep(min(3, timeout_sec / 10) if timeout_sec else 1)
    logger.debug("\n### Time is out! (history_connector)")
    raise Exception("Time is out!")
    # return False


def get_min_price(platform_id, symbol, side, rest_auth_client=None):
    if not rest_auth_client:
        rest_auth_client = get_or_create_rest_client(
            platform_id,
            True,
            credentials=get_credentials_for(platform_id),
            pivot_symbol=symbol or 'BTC')
    book = rest_auth_client.fetch_order_book(symbol)
    # side 1= Sell, side -2 = Buy
    if side == 1:
        return book.asks[-1].price
    elif side == 2:
        return book.bids[-1].price


def create_test_order(platform_id,
                      symbol=None,
                      side=Direction.BUY,
                      sleep_before=0,
                      market=False,
                      pivot_symbol='BTC'):
    while True:
        time.sleep(sleep_before)
        if isinstance(symbol, list):
            symbol = symbol[0]
        rest_auth_client = get_or_create_rest_client(
            platform_id,
            True,
            credentials=get_credentials_for(platform_id),
            pivot_symbol=pivot_symbol)
        if not symbol:
            symbol = rest_auth_client.get_symbols()[0]
        # rest_auth_client.close_all_positions()
        # orders_debug = rest_auth_client.fetch_orders(symbol, is_open_only=True)
        # balances_debug = rest_auth_client.fetch_balance()
        # positions_debug = rest_auth_client.get_positions()
        if platform_id == Platform.BITMEX:
            min_amount = 1
        else:
            min_amount = SingleDataAggregator().get_symbol_min_amount(platform_id, symbol)
        if not market:
            price_to_place = get_min_price(platform_id, symbol, side,
                                           rest_auth_client)
            order_params = {
                ParamName.ORDER_TYPE: OrderType.LIMIT,
                ParamName.DIRECTION: side,
                ParamName.PRICE: price_to_place,
                ParamName.AMOUNT: min_amount,
            }
        else:
            order_params = {
                ParamName.ORDER_TYPE: OrderType.MARKET,
                ParamName.DIRECTION: side,
                ParamName.AMOUNT: min_amount,
                ParamName.PRICE: 0,
                # ParamName.AMOUNT: 0.3,
            }
        order = rest_auth_client.create_order(symbol,
                                              **order_params,
                                              is_test=False)
        if isinstance(order, Error):
            if 'The system is currently overloaded' in order.message:
                logger.warning("Can't place test order, RETRY after 30 sec",
                               str(order.message))
                time.sleep(30)
            else:
                raise Exception("Can't place test order", str(order))
        else:
            return order


def delete_all_test_orders(platform_id,
                           symbol,
                           rest_auth_client=None,
                           pivot_symbol='BTC'):
    if not rest_auth_client:
        rest_auth_client: PrivatePlatformRESTClient = get_or_create_rest_client(
            platform_id,
            True,
            credentials=get_credentials_for(platform_id),
            pivot_symbol=pivot_symbol)
    if rest_auth_client:
        if isinstance(symbol, list):
            for s in symbol:
                delete_all_test_orders(platform_id, s, rest_auth_client,
                                       pivot_symbol)
        else:
            rest_auth_client.cancel_all_orders(symbol)


def close_all_positions(platform_id, rest_auth_client=None,
                        pivot_symbol='BTC'):
    if not rest_auth_client:
        rest_auth_client: PrivatePlatformRESTClient = get_or_create_rest_client(
            platform_id,
            True,
            credentials=get_credentials_for(platform_id),
            pivot_symbol=pivot_symbol)
    if rest_auth_client:
        rest_auth_client.close_all_positions()
