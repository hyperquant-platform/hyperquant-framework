import time
from collections import defaultdict
from typing import Dict, Tuple

import math

from hyperquant.api import CurrencyPair


class SingleDataAggregator:
    """
    To reduce REST requests to server one instance of SingleDataAggregator will
    be shared between all clients both rest and ws. Instance stores last quotes
    and also referencing_pair table for each platform that made a request
    """

    __instance = None

    def __new__(cls, *args, **kwargs):
        if not SingleDataAggregator.__instance:
            SingleDataAggregator.__instance = super().__new__(cls, *args, **kwargs)
        return SingleDataAggregator.__instance

    def __init__(self):
        from hyperquant.api import CurrencyPair
        from hyperquant.clients import Quote
        self.quotes_by_symbol_by_platform_id: Dict[int, Dict[str, Tuple[Quote, float]]] = defaultdict(dict)
        self.currency_pair_by_name_by_platform_id: Dict[int, Dict[str, CurrencyPair]]  = defaultdict(dict)
        self.symbols_by_platform_id = {}

    def get_currency_pair(self, platform_id, symbol):
        currency_pair_by_name = self.get_currency_pairs_by_name(platform_id)
        return currency_pair_by_name[symbol]

    def get_currency_pairs_by_name(self, platform_id, force_fetching=False):
        currency_pair_by_name = self.currency_pair_by_name_by_platform_id.get(platform_id)
        if not currency_pair_by_name or force_fetching:
            client = self.get_rest_client(platform_id)
            curr_pairs = client.fetch_currency_pairs()
            self.currency_pair_by_name_by_platform_id[platform_id] = {
                cp.name_in_platform: cp
                for cp in curr_pairs
            }
        return self.currency_pair_by_name_by_platform_id[platform_id]

    def _make_currency_pair(self, platform_id, rating_currency, pivot_symbol):
        currency_pairs_by_name = self.get_currency_pairs_by_name(platform_id)
        result = [
            s for s in list(currency_pairs_by_name.keys())
            if rating_currency in s and pivot_symbol in s
        ]
        if len(result):
            return result[0]

    def get_rest_client(self, platform_id):
        from hyperquant.clients.utils import get_or_create_rest_client
        return get_or_create_rest_client(platform_id)

    def get_quote(self, platform_id, symbol, data_age_sec=30):
        current_time = time.time()
        if symbol not in self.quotes_by_symbol_by_platform_id:
            client = self.get_rest_client(platform_id)
            quote = client.fetch_quote(symbol)
            self.quotes_by_symbol_by_platform_id[platform_id][symbol] = (quote, current_time)
        elif (current_time - self.quotes_by_symbol_by_platform_id[platform_id][symbol][1]) > data_age_sec:
            client = self.get_rest_client(platform_id)
            quote = client.fetch_quote(symbol)
            self.quotes_by_symbol_by_platform_id[platform_id][symbol] = (quote, current_time)
        return self.quotes_by_symbol_by_platform_id[platform_id][symbol][0]

    def get_symbols(self, platform_id, data_age_sec=300):
        current_time = time.time()
        if platform_id not in self.symbols_by_platform_id:
            rest_client = self.get_rest_client(platform_id)
            available_symbols = rest_client.fetch_symbols()
            self.symbols_by_platform_id[platform_id] = (available_symbols, current_time)
        elif (current_time - self.symbols_by_platform_id[platform_id][1]) > data_age_sec:
            rest_client = self.get_rest_client(platform_id)
            available_symbols = rest_client.fetch_symbols()
            self.symbols_by_platform_id[platform_id] = (available_symbols, current_time)
        return self.symbols_by_platform_id[platform_id][0]

    # Note! Min_lot doesn't mean mon amount
    def get_actual_min_lot(self, platform_id, pair, price=None):
        client = self.get_rest_client(platform_id)
        if client.is_futures:
            return pair.lot_min
        quotation = self.get_quote(platform_id, pair.to_string())
        price = price if price else quotation.bestask
        # min_notonal is expressed in Quote
        # lot is in Base
        min_notional_in_base = pair.min_notional / price
        return max(pair.lot_min, min_notional_in_base)

    def get_symbol_min_amount(self, platform_id, symbol, price=None):
        pair = self.get_currency_pair(platform_id, symbol)
        lot = self.get_actual_min_lot(platform_id, pair, price)
        lot = (math.ceil(lot / pair.lot_min) + 1) * pair.lot_min
        return lot

    def create_position_if_exist(self, balance, pivot_symbol, symbol=None):
        from hyperquant.api import Direction
        from hyperquant.clients import Position
        from hyperquant.api import Endpoint

        currency_pair = self._make_currency_pair(balance.platform_id, balance.symbol, pivot_symbol)
        if pivot_symbol != balance.symbol and balance.amount_available > 0 and (
                not symbol or currency_pair == symbol):
            currency_pair_by_name = self.get_currency_pairs_by_name(balance.platform_id)
            referencing_pair = currency_pair_by_name[currency_pair]
            min_lot = self.get_actual_min_lot(balance.platform_id, referencing_pair)
            if pivot_symbol == referencing_pair.quote:
                amount = balance.amount_available
            elif pivot_symbol == referencing_pair.base:
                amount = self.convert_amount_to_pivot(balance.platform_id, pivot_symbol,
                    balance.amount_available, referencing_pair.base,
                    referencing_pair.quote)
            else:
                raise Exception('Cross change not currently supported')
            if float(amount) >= float(min_lot) * 1.05:
                position = Position(balance.platform_id,
                                    currency_pair, None,
                                    amount,
                                    Direction.BUY
                                    if balance.symbol == referencing_pair.base else Direction.SELL)
                position.endpoint = Endpoint.POSITION
                return position


    def convert_amount_to_pivot(self,
                                platform_id,
                                pivot_symbol,
                                amount,
                                base: str = None,
                                quote: str = None,
                                price=None):
        assert (base and quote)
        symbol = f"{base}{quote}"
        quotation = self.get_quote(platform_id, symbol)
        if quote == pivot_symbol:
            # SOME / PIVOT
            # BUY
            price = price if price else quotation.bestask
            return amount * price
        elif base == pivot_symbol:
            # PIVOT / SOME
            # SELL
            price = price if price else quotation.bestbid
            return amount / price
        raise NotImplementedError(
            "Conversion not implemented: pivot {self.pivot_symbol}, pair {symbol}"
        )
