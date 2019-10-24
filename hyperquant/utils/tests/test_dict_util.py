import unittest
from decimal import Decimal
from unittest import TestCase

from hyperquant.api import Platform, ParamName, convert_items_to_dict, item_format_by_endpoint, Endpoint
from hyperquant.clients import Trade
from hyperquant.utils import dict_util
from hyperquant.utils.dict_util import check_is_sub_data


class TestDictUtil(TestCase):
    @unittest.skip("todo")
    def test_inverse_dict(self):
        pass

    def test_convert_items_to_grouped_lookup(self):
        items = [(1, "BTCETH", 1000011), (2, "BTCETH", 1000021), (3, "BNBETH", 1000023)]

        # Normal
        expected = {1: {"BTCETH": [1000011]}, 2: {"BTCETH": [1000021]}, 3: {"BNBETH": [1000023]}}
        result = dict_util.convert_items_to_grouped_lookup(items, 3)
        self.assertEqual(result, expected)

        # Normal
        expected = {1: {"BTCETH": [1000011]}, 2: {"BTCETH": [1000021]}, 3: {"BNBETH": [1000023]}}
        result = dict_util.convert_items_to_grouped_lookup(items, 2)
        self.assertEqual(result, expected)

        # Normal
        expected = {1: ["BTCETH", 1000011], 2: ["BTCETH", 1000021], 3: ["BNBETH", 1000023]}
        result = dict_util.convert_items_to_grouped_lookup(items, 1)
        self.assertEqual(result, expected)

        # Normal
        expected = items
        result = dict_util.convert_items_to_grouped_lookup(items, 0)
        self.assertEqual(result, expected)

    def test_group_items(self):
        items = [Trade(1, "BTCETH", 1000011), Trade(2, "BTCETH", 1000021),
                 Trade(2, "BTCUSD", 1000012), Trade(1, "BTCUSD", 1000022),
                 Trade(1, "BNBETH", 1000013), Trade(3, "BNBETH", 1000023),
                 Trade(1, "BNBUSD", 1000014), Trade(1, "BNBUSD", 1000024)]
        expected = {1: {
            "BTCETH": [Trade(1, "BTCETH", 1000011)],
            "BTCUSD": [Trade(1, "BTCUSD", 1000022)],
            "BNBETH": [Trade(1, "BNBETH", 1000013)],
            "BNBUSD": [Trade(1, "BNBUSD", 1000014), Trade(1, "BNBUSD", 1000024)],
        }, 2: {
            "BTCETH": [Trade(2, "BTCETH", 1000021)],
            "BTCUSD": [Trade(2, "BTCUSD", 1000012)],
        }, 3: {
            "BNBETH": [Trade(3, "BNBETH", 1000023)],
        }}

        # Normal (ItemObject-s and dicts)
        result = dict_util.group_items(items, [ParamName.PLATFORM_ID, ParamName.SYMBOL])
        self.assertEqual(result, expected)

        result = dict_util.group_items(items, None)
        self.assertEqual(result, items)

        result = dict_util.group_items(items, [])
        self.assertEqual(result, items)

        # Normal (ItemObject-s and dicts)
        item_dicts = convert_items_to_dict(items, item_format_by_endpoint[Endpoint.TRADE])
        expected_dicts = {k: {kk: convert_items_to_dict(vv, item_format_by_endpoint[Endpoint.TRADE])
                              for kk, vv in v.items()}
                          for k, v in expected.items()}
        result = dict_util.group_items(item_dicts, [ParamName.PLATFORM_ID, ParamName.SYMBOL])
        self.assertEqual(result, expected_dicts)

        # Empty
        result = dict_util.group_items(None, None)
        self.assertEqual(result, None)

        result = dict_util.group_items([], None)
        self.assertEqual(result, [])

    def test_filter_keys(self):
        input = {"a": 1, "b": 2, "c": 3, "d": 4}
        expected = {"a": 1, "b": 2}

        class Temp:
            a = None
            b = None
            e = None

        result = dict_util.filter_keys(input, Temp)
        self.assertEqual(result, expected)

        result = dict_util.filter_keys(input, Temp())
        self.assertEqual(result, expected)

        result = dict_util.filter_keys(input, ["a", "b"])
        self.assertEqual(result, expected)

        result = dict_util.filter_keys(input, {"a", "b"})
        self.assertEqual(result, expected)

        result = dict_util.filter_keys(input, {"a": 44, "b": 55})
        self.assertEqual(result, expected)

        result = dict_util.filter_keys(input, input)
        self.assertEqual(result, input)

        # Empty
        result = dict_util.filter_keys(input, [])
        self.assertEqual(result, {})

        result = dict_util.filter_keys(input, None)
        self.assertEqual(result, input)  # expected input as result

        result = dict_util.filter_keys(None, ["a", "b"])
        self.assertEqual(result, None)

        # Wrong
        result = dict_util.filter_keys([1, 2], [])
        self.assertEqual(result, [1, 2])  # expected input as result

    def test_check_is_sub_data(self):
        # Empty

        self.assertTrue(check_is_sub_data(None, None))
        self.assertFalse(check_is_sub_data({}, None))
        self.assertFalse(check_is_sub_data(None, {}))
        self.assertFalse(check_is_sub_data(0, None))
        self.assertFalse(check_is_sub_data(None, 0))
        self.assertFalse(check_is_sub_data("", None))
        self.assertFalse(check_is_sub_data(None, ""))
        self.assertTrue(check_is_sub_data([], []))
        self.assertTrue(check_is_sub_data({}, {}))
        self.assertFalse(check_is_sub_data({}, []))
        self.assertFalse(check_is_sub_data([], {}))

        # Scalar

        self.assertTrue(check_is_sub_data(5, 5))
        self.assertTrue(check_is_sub_data(5, 4))
        self.assertFalse(check_is_sub_data(5, 6))
        self.assertTrue(check_is_sub_data(5.1, 5.1))
        self.assertTrue(check_is_sub_data(5.1, 4.1))
        self.assertFalse(check_is_sub_data(5.1, 6.1))
        self.assertTrue(check_is_sub_data(Decimal(5), Decimal(5)))
        self.assertTrue(check_is_sub_data(Decimal(5), Decimal(4)))
        self.assertFalse(check_is_sub_data(Decimal(5), Decimal(6)))
        self.assertFalse(check_is_sub_data("5", "4"))
        self.assertFalse(check_is_sub_data("a", "b"))
        self.assertFalse(check_is_sub_data("b", "a"))
        self.assertTrue(check_is_sub_data("abcd", "abcd"))
        self.assertTrue(check_is_sub_data("abcd", "bc"))
        self.assertFalse(check_is_sub_data("abcd", "dabc"))

        # Simple

        # Dict
        self.assertTrue(check_is_sub_data({"a": 1}, {"a": 1}))
        self.assertTrue(check_is_sub_data({"a": 1, "b": 2}, {"a": 1}))
        self.assertFalse(check_is_sub_data({"a": 1}, {"a": 2}))
        self.assertFalse(check_is_sub_data({"a": 1}, {"b": 1}))
        # # (dict as list of tuples)  # ?-
        # self.assertTrue(check_is_sub_data({"a": 1}, [("a", 1)]))
        # self.assertTrue(check_is_sub_data([("a", 1), ("b", 2)], {"a": 1}))
        self.assertFalse(check_is_sub_data({"a": 1}, [("a", 1)]))
        self.assertFalse(check_is_sub_data([("a", 1), ("b", 2)], {"a": 1}))

        # List
        self.assertTrue(check_is_sub_data(["a", 1], ["a", 1]))
        self.assertTrue(check_is_sub_data(["a", 1, "b", 2], ["a", 1]))
        # (with iterables)
        self.assertTrue(check_is_sub_data([("a", 1)], [("a", 1)]))
        self.assertTrue(check_is_sub_data([("a", 1), ["b", 2], {3, 4}, {"c": 5}],
                                          [("a", 1), {3, 4}, {"c": 5}]))
        #  (change order)
        self.assertTrue(check_is_sub_data([("a", 1), ["b", 2], {3, 4}, {"c": 5}],
                                          [{"c": 5}, ("a", 1), {3, 4}]))
        # (no need as previous is also True)
        # self.assertTrue(check_is_sub_data([("a", 1), ["b", 2], {3, 4}, {"c": 5}],
        #                                   reversed([("a", 1), {3, 4}, {"c": 5}]), False))

        # Tuple
        self.assertTrue(check_is_sub_data(("a", 1), ("a", 1)))
        self.assertTrue(check_is_sub_data(("a", 1, "b", 2), ("a", 1)))
        # (with iterables)
        self.assertTrue(check_is_sub_data((("a", 1), ["b", 2], {3, 4}, {"c": 5}), (("a", 1), {3, 4}, {"c": 5})))

        # Set
        self.assertTrue(check_is_sub_data({"a", 1}, {"a", 1}))
        self.assertTrue(check_is_sub_data({"a", 1, "b", 2}, {"a", 1}))
        # (with iterables)
        self.assertTrue(check_is_sub_data({("a", 1)}, {("a", 1)}))
        self.assertTrue(check_is_sub_data({("a", 1), "b", 2}, {("a", 1)}))

        # Complex

        # Dict
        self.assertTrue(check_is_sub_data({"a": 1, "b": {"c": 2, "d": 3}}, {"a": 1, "b": {"c": 2}}))

        data = {"a": {"b": 1, "c": {"d": [2, 3], "dd": 23}, "e": {4, 5}}, "f": (6, 7), "g": "hij"}
        sub_data = {"a": {"c": {"d": [2, 3]}}, "f": (6, 7)}
        not_sub_data1 = {"a": {"c": {"d": [2, 3], "dd": 11}}, "f": (6, 7)}
        not_sub_data2 = {"a": {"c": {"d": [2, 3], "uu": 23}}, "f": (6, 7)}

        self.assertTrue(check_is_sub_data(data, data))
        self.assertTrue(check_is_sub_data(data, sub_data))
        self.assertFalse(check_is_sub_data(data, not_sub_data1))
        self.assertFalse(check_is_sub_data(data, not_sub_data2))

        # List
        # (confict with other list/set tests)
        self.assertTrue(check_is_sub_data(["a", 1, ["b", [2, 3]]], ["a", 1, ["b"]]))  # ?
        self.assertFalse(check_is_sub_data(["a", 1, ["b", [2, 3]]], ["a", ["b"], 1]))  # ?
        self.assertTrue(check_is_sub_data(["a", 1, ["b", [2, 3]]], ["a", 1, ["b", [2]]]))  # ?
        self.assertFalse(check_is_sub_data(["a", 1, ["b", [2, 3]]], ["a", 1, [[2], "b"]]))  # ?
        self.assertFalse(check_is_sub_data(["a", 1, ["b", [2, 3]]], ["a", ["b", [2]]]))

        data = [{"b": 1, "c": {"d": [2, 3], "dd": 23}, "e": {4, 5}}, (6, 7), "hij"]
        sub_data = [{"c": {"d": [2, 3]}}, (6, 7)]
        not_sub_data1 = [{"c": {"d": [2, 3], "dd": 11}}, (6, 7)]
        not_sub_data2 = [{"c": {"d": [2, 3], "uu": 23}}, (6, 7)]

        self.assertTrue(check_is_sub_data(data, data))
        self.assertTrue(check_is_sub_data(data, sub_data))
        self.assertFalse(check_is_sub_data(data, reversed(sub_data)))
        self.assertFalse(check_is_sub_data(data, not_sub_data1))
        self.assertFalse(check_is_sub_data(data, not_sub_data2))

        # Tuple
        data = tuple(data)
        sub_data = tuple(sub_data)
        not_sub_data1 = tuple(not_sub_data1)
        not_sub_data2 = tuple(not_sub_data2)

        self.assertTrue(check_is_sub_data(data, data))
        self.assertTrue(check_is_sub_data(data, sub_data))
        self.assertFalse(check_is_sub_data(data, not_sub_data1))
        self.assertFalse(check_is_sub_data(data, not_sub_data2))

        # Set
        data = {1, (4, 5), (6, 7), "hij"}
        sub_data = {1, (6, 7)}
        not_sub_data1 = {2, (6, 7)}
        not_sub_data2 = {1, (6, 5)}

        self.assertTrue(check_is_sub_data(data, data))
        self.assertTrue(check_is_sub_data(data, sub_data))
        self.assertFalse(check_is_sub_data(data, not_sub_data1))
        self.assertFalse(check_is_sub_data(data, not_sub_data2))

        # Dict + List
        self.assertTrue(check_is_sub_data({"a": 1, "b": [{"c": 2, "d": 3}, {"e": 4}]}, {"a": 1, "b": [{"c": 2}]}))
        self.assertTrue(check_is_sub_data({"a": 1, "b": ({"c": 2, "d": 3}, {"e": 4})}, {"a": 1, "b": ({"c": 2},)}))
        # self.assertTrue(check_is_sub_data({"a": 1, "b": {{"c": 2, "d": 3}, {"e": 4}}}, {"a": 1, "b": {{"e": 4}}}))  # ?
        # self.assertTrue(check_is_sub_data({"a": 1, "b": {{"c": 2, "d": 3}, {"e": 4}}}, {"a": 1, "b": {{"c": 2}}}))  # ?
