from unittest import TestCase

from hyperquant.utils.log_util import protect_secret_data


class TestLogUtil(TestCase):

    def test_protect_secret_data(self):
        not_changing_data = [123, "ddd", [1, 2], (1, 2), {1, 2}, {"a": 2}, None, 0, ""]

        for item in not_changing_data:
            self.assertEqual(protect_secret_data(item), item)

        data = {"a": 2, "pWd": 123, "PWDD": ["x", 5], "password": "234", "pass": "1234567890"}
        expected = {"a": 2, "pWd": "...", "PWDD": "...", "password": "...", "pass": "1...0"}

        self.assertEqual(protect_secret_data(data), expected)
