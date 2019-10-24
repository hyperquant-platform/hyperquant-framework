import logging
import time
import unittest
from datetime import datetime

from hyperquant.api import ParamName
from hyperquant.clients import ItemObject, Candle
from hyperquant.utils.time_util import get_timestamp_ms, get_timestamp_s, get_timestamp_iso, get_timestamp, \
    start_timings, get_timings, get_timings_str, stop_timings


class TestTimeUtil(unittest.TestCase):
    timestamp_s = int(time.time() * 1000) / 1000
    timestamp_ms = int(timestamp_s * 1000)
    timestamp_s_str = str(timestamp_s)
    timestamp_ms_str = str(timestamp_ms)
    timestamp_iso = datetime.utcfromtimestamp(timestamp_s).isoformat()
    timestamp_iso_like_str = timestamp_iso.replace("T", " ")
    timestamp_datetime = datetime.utcfromtimestamp(timestamp_s)

    item_dict1 = {ParamName.TIMESTAMP: timestamp_s}
    item_dict2 = {ParamName.TIMESTAMP: timestamp_ms}
    item_dict3 = {ParamName.TIMESTAMP: str(timestamp_s)}
    item_dict4 = {ParamName.TIMESTAMP: str(timestamp_ms)}
    item_obj1 = ItemObject(**item_dict1)  # , is_milliseconds=False)
    item_obj2 = ItemObject(**item_dict2)  # , is_milliseconds=True)
    item_obj3 = ItemObject(**item_dict3)  # , is_milliseconds=False)
    item_obj4 = ItemObject(**item_dict4)  # , is_milliseconds=True)
    candle = Candle(timestamp_close=timestamp_s, **item_dict4)  # , is_milliseconds=True)

    def test_get_timestamp_ms(self):
        self._test_get_timestamp(get_timestamp_ms, self.timestamp_ms)

        result = get_timestamp_ms("14.07.2017, 2:40:00")
        self.assertEqual(result, 1500000000000)

        result = get_timestamp_ms(self.candle, ParamName.TIMESTAMP_CLOSE)
        self.assertEqual(result, self.timestamp_ms)

    def test_get_timestamp_s(self):
        self._test_get_timestamp(get_timestamp_s, self.timestamp_s)

        result = get_timestamp_s("14.07.2017, 2:40:00")
        self.assertEqual(result, 1500000000)

        result = get_timestamp_s(self.candle, ParamName.TIMESTAMP_CLOSE)
        self.assertEqual(result, self.timestamp_s)

    def test_get_timestamp_iso(self):
        self._test_get_timestamp(get_timestamp_iso, self.timestamp_iso)

        result = get_timestamp_iso("14.07.2017, 2:40:00")
        self.assertEqual(result, "2017-07-14T02:40:00")

        result = get_timestamp_iso("2017-07-14 02:40:00")
        self.assertEqual(result, "2017-07-14T02:40:00")

        result = get_timestamp_iso(self.candle, ParamName.TIMESTAMP_CLOSE)
        self.assertEqual(result, self.timestamp_iso)

    def test_get_timestamp(self):
        self._test_get_timestamp(lambda value, is_parse_timestamp_only=False: get_timestamp(
            value, True, is_parse_timestamp_only=is_parse_timestamp_only), self.timestamp_ms)
        self._test_get_timestamp(lambda value, is_parse_timestamp_only=False: get_timestamp(
            value, False, is_parse_timestamp_only=is_parse_timestamp_only), self.timestamp_s)

    def _test_get_timestamp(self, fun, expected):
        timestamp_values = [
            self.timestamp_s, self.timestamp_ms, self.timestamp_s_str, self.timestamp_ms_str,
            self.item_dict1, self.item_dict2, self.item_dict3, self.item_dict4,
            self.item_obj1, self.item_obj2, self.item_obj3, self.item_obj4]
        other_values = [
            self.timestamp_iso, self.timestamp_iso_like_str, self.timestamp_datetime]
        # results = [fun(value) for value in values]

        for value in timestamp_values:
            result = fun(value)
            self.assertEqual(result, expected, value)
            self.assertEqual(type(result), type(expected), value)
            # self.assertIs(result, expected, value)

            result = fun(value, is_parse_timestamp_only=True)
            self.assertEqual(result, expected, value)

        for value in other_values:
            result = fun(value)
            self.assertEqual(result, expected, value)
            self.assertEqual(type(result), type(expected), value)
            # self.assertIs(result, expected, value)

            result = fun(value, is_parse_timestamp_only=True)
            self.assertEqual(result, None, value)

        empty_values = [None, [], {}, '']
        for value in empty_values:
            result = fun(value)
            self.assertEqual(result, None)

        wrong_values = ["20170-07-14T02:40:00"]
        for value in wrong_values:
            logging.info("Expecting error and None returned while parsing \"20170-07-14T05:40:00\"...")
            result = fun(value)
            self.assertEqual(result, None)

    def test_timings(self):
        start_timings("a", "b")

        time.sleep(0.1)

        timings = stop_timings("b", "d")
        # (Rounding)
        timings = [round(t * 10) / 10 for t in timings]

        self.assertEqual(timings, [0.1, 0])

        start_timings("c")

        time.sleep(0.1)

        timings = get_timings("a", "b", "c", "d", "e")
        # (Rounding)
        timings = [round(t * 10) / 10 for t in timings]

        self.assertEqual(timings, [0.2, 0.1, 0.1, 0, 0])

        logging.info(get_timings_str("a", "b", "c", "d", "e"))
