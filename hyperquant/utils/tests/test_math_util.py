import math
from decimal import Decimal
from unittest import TestCase

from hyperquant.utils.math_util import round_to_tick


class TestRoundToTick(TestCase):

    def test_round_to_tick(self):
        # Empty
        self.assertEqual(round_to_tick(None), None)
        self.assertEqual(round_to_tick(0), 0)
        self.assertEqual(round_to_tick(10.12345), 10.12345)

        # Normal
        # (tick_size < 1)
        self.assertEqual(round_to_tick(10.1234567, 0.1), 10.1)
        self.assertEqual(round_to_tick(10.1234567, 0.0001), 10.1235)
        self.assertEqual(round_to_tick(10.1234567, 0.0002), 10.1234)
        self.assertEqual(round_to_tick(10.1234567, 0.00001), 10.12346)
        self.assertEqual(round_to_tick(10.1234567, 0.00002), 10.12346)
        self.assertEqual(round_to_tick(10.1234567, 0.5), 10)
        self.assertEqual(round_to_tick(Decimal("10.1234567"), 0.5), Decimal("10"))
        self.assertEqual(round_to_tick(Decimal("10.1234567"), Decimal("0.5")), Decimal("10"))

        # (tick_size >= 1)
        self.assertEqual(round_to_tick(10.1234567, 1), 10)
        self.assertEqual(round_to_tick(16.1234567, 5), 15)
        self.assertEqual(round_to_tick(19.1234567, 5), 20)

        # (tick_size < 0)
        self.assertEqual(round_to_tick(10.1234567, -1), 10)
        self.assertEqual(round_to_tick(16.1234567, -5), 15)

        # (value < 0)
        self.assertEqual(round_to_tick(-16.1234567, 5), -15)
        self.assertEqual(round_to_tick(-19.1234567, 5), -20)

        # Floor
        self.assertEqual(round_to_tick(10.1234567, 0.0001, math.floor), 10.1234)
        self.assertEqual(round_to_tick(10.1234567, 0.0002, math.floor), 10.1234)
        self.assertEqual(round_to_tick(10.1234567, 0.00001, math.floor), 10.12345)
        self.assertEqual(round_to_tick(10.1234567, 0.00002, math.floor), 10.12344)

        # Ceil
        self.assertEqual(round_to_tick(10.1234567, 0.0001, math.ceil), 10.1235)
        self.assertEqual(round_to_tick(10.1234567, 0.0002, math.ceil), 10.1234)
        self.assertEqual(round_to_tick(10.1234567, 0.00001, math.ceil), 10.12346)
        self.assertEqual(round_to_tick(10.1234567, 0.00002, math.ceil), 10.12346)
