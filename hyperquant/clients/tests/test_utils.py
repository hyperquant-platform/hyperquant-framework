import unittest

from hyperquant.api import Platform, Sorting
from hyperquant.clients import Trade, Balance
from hyperquant.clients.binance import BinanceRESTClient, BinanceWSClient
from hyperquant.clients.bitmex import BitMEXRESTClient, BitMEXWSClient
from hyperquant.clients.utils import create_rest_client, create_ws_client, sort_items, filter_items_by_last_id


class TestCreateClient(unittest.TestCase):

    def test_create_rest_client(self):
        self._test_create_client()

    def test_create_ws_client(self):
        self._test_create_client(False)

    def test_create_rest_client_private(self):
        self._test_create_client(is_private=True)

    def test_create_ws_client_private(self):
        self._test_create_client(False, is_private=True)

    def _test_create_client(self, is_rest=True, is_private=False):
        create_client = create_rest_client if is_rest else create_ws_client

        # Binance
        client = create_client(Platform.BINANCE, is_private)

        self.assertIsInstance(client, BinanceRESTClient if is_rest else BinanceWSClient)
        self.assertEqual(client.version, BinanceRESTClient.version)
        if not is_private:
            self.assertIsNotNone(client._api_key,
                                 "For Binance, api_key must be set even for public API (for historyTrades endpoint)")
            self.assertIsNone(client._api_secret)
        else:
            self.assertIsNotNone(client._api_key)
            self.assertIsNotNone(client._api_secret)


        # BitMEX
        client = create_client(Platform.BITMEX, is_private)

        self.assertIsInstance(client, BitMEXRESTClient if is_rest else BitMEXWSClient)
        self.assertEqual(client.version, BitMEXRESTClient.version)
        if not is_private:
            self.assertIsNone(client._api_key)
            self.assertIsNone(client._api_secret)
        else:
            self.assertIsNotNone(client._api_key)
            self.assertIsNotNone(client._api_secret)

    def test_create_rest_client__credentials(self):
        client = create_rest_client(Platform.BINANCE, is_private=True, credentials=("aaa", "sss"))

        self.assertEqual(client._api_key, "aaa")
        self.assertEqual(client._api_secret, "sss")
        self.assertEqual(client._credentials, ("aaa", "sss"))

        client._send("", None)
        client._send("", None)

        self.assertEqual(client._api_key, "aaa")
        self.assertEqual(client._api_secret, "sss")
        self.assertEqual(client._credentials, ("aaa", "sss"))

    def test_create_rest_client__callable_credentials(self):
        credentials_list = [("aaa", "sss"), ("aaa22", "sss22")]
        client = create_rest_client(Platform.BINANCE, is_private=True, credentials=lambda: credentials_list.pop(0))

        self.assertEqual(client._api_key, "aaa")
        self.assertEqual(client._api_secret, "sss")
        self.assertTrue(callable(client._credentials))

        client._send(None, None)

        self.assertEqual(client._api_key, "aaa22")
        self.assertEqual(client._api_secret, "sss22")
        self.assertTrue(callable(client._credentials))


class TestSomeSeparateUtils(unittest.TestCase):

    def test_sorting_item_objects(self):
        items = None
        self.assertEqual(sort_items(items), None)
        items = []
        self.assertEqual(sort_items(items), [])
        t1 = Trade(timestamp=1000000000)
        items = [t1]
        self.assertEqual(sort_items(items), [t1])
        self.assertEqual(sort_items(items, sorting=Sorting.DESCENDING), [t1])
        t1 = Trade(timestamp=1000000000)
        t2 = Trade(timestamp=2000000000)
        items = [t1, t2]
        self.assertEqual(sort_items(items), [t1, t2])
        self.assertEqual(sort_items(items, sorting=Sorting.DESCENDING), [t2, t1])
        self.assertEqual(sort_items(items, sorting=Sorting.ASCENDING), [t1, t2])
        b1 = Balance(1, 'BTC')
        b2 = Balance(1, 'ETH')
        items = [b1, b2]
        self.assertEqual(sort_items(items), [b1, b2])
        self.assertEqual(sort_items(items, sorting=Sorting.DESCENDING), [b1, b2])

    def test_filter_item_objects_by_last_id(self):
        self.assertEqual(filter_items_by_last_id(None, None), None)
        self.assertEqual(filter_items_by_last_id([], None), [])
        t1 = Trade(item_id=1)
        items = [t1]
        self.assertEqual(filter_items_by_last_id(items, None), [t1])
        last_trade = Trade(item_id=None)
        self.assertEqual(filter_items_by_last_id(items, last_trade), [t1])
        t2 = Trade(item_id=2)
        items = [t1, t2]
        self.assertEqual(filter_items_by_last_id(items, last_trade), [t1, t2])
        last_trade = Trade(item_id=3)
        self.assertEqual(filter_items_by_last_id(items, last_trade), [t1, t2])
        t3 = Trade(item_id=3)
        items = [t1, t2, t3]
        last_trade = Trade(item_id=1)
        self.assertEqual(filter_items_by_last_id(items, last_trade), [t1, t2, t3])
        last_trade = Trade(item_id=2)
        self.assertEqual(filter_items_by_last_id(items, last_trade), [t2, t3])
        last_trade = Trade(item_id=3)
        self.assertEqual(filter_items_by_last_id(items, last_trade), [t3])

