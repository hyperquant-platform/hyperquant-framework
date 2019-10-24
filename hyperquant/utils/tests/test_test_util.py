from unittest import TestCase

from hyperquant.api import add_error_response_item, finish_error_response, ErrorCode, make_data_response
from hyperquant.utils.test_util import RESTAPITestCase


class TestRESTAPITestCase(TestCase):
    assertAPIResponseSuccess = RESTAPITestCase.assertAPIResponseSuccess
    assertAPIResponseData = RESTAPITestCase.assertAPIResponseData
    assertAPIResponseErrors = RESTAPITestCase.assertAPIResponseErrors

    def test_assertAPIResponseData__is_check_matched_fully(self):
        # Dict
        data = {"a": {"b": 1, "c": {"d": [2, 3], "dd": 23}, "e": [4, 5]}, "f": [6, 7], "g": "hij"}
        sub_data = {"a": {"c": {"d": [2, 3]}}, "f": [6, 7]}
        not_sub_data1 = {"a": {"c": {"d": [2, 3], "dd": 11}}, "f": [6, 7]}
        not_sub_data2 = {"a": {"c": {"d": [2, 3], "uu": 23}}, "f": [6, 7]}
        response = make_data_response(data)

        self.assertAPIResponseData(response, data)
        self.assertAPIResponseData(response, data, is_check_matched_fully=False)
        self.assertAPIResponseData(response, sub_data, is_check_matched_fully=False)

        # (sub_data with changes - no match)
        with self.assertRaises(AssertionError):
            self.assertAPIResponseData(response, not_sub_data1, is_check_matched_fully=False)
        with self.assertRaises(AssertionError):
            self.assertAPIResponseData(response, not_sub_data2, is_check_matched_fully=False)
        # (is_check_matched_fully enabled)
        with self.assertRaises(AssertionError):
            self.assertAPIResponseData(response, sub_data)

        # List
        data = [{"b": 1, "c": {"d": [2, 3], "dd": 23}, "e": [4, 5]}, [6, 7], "hij"]
        sub_data = [{"c": {"d": [2, 3]}}, [6, 7]]
        not_sub_data1 = [{"c": {"d": [2, 3], "dd": 11}}, [6, 7]]
        not_sub_data2 = [{"c": {"d": [2, 3], "uu": 23}}, [6, 7]]
        response = make_data_response(data)

        self.assertAPIResponseData(response, data)
        self.assertAPIResponseData(response, data, is_check_matched_fully=False)
        self.assertAPIResponseData(response, sub_data, is_check_matched_fully=False)

        # (sub_data with changes - no match)
        with self.assertRaises(AssertionError):
            self.assertAPIResponseData(response, not_sub_data1, is_check_matched_fully=False)
        with self.assertRaises(AssertionError):
            self.assertAPIResponseData(response, not_sub_data2, is_check_matched_fully=False)
        # (is_check_matched_fully enabled)
        with self.assertRaises(AssertionError):
            self.assertAPIResponseData(response, sub_data)

    def test_assertAPIResponseErrors(self):
        # Asserting no exceptions

        # Empty
        with self.assertRaises(Exception):
            self.assertAPIResponseErrors(None, None)
        with self.assertRaises(Exception):
            self.assertAPIResponseErrors(None, [])

        # Normal
        add_error_response_item(ErrorCode.MISS_REQ_PARAMS, description="Here's some text", field="param1")
        add_error_response_item(ErrorCode.WRONG_PARAM, description="Some wrong param", field=("param2", "param3"))
        error_response = finish_error_response()

        self.assertAPIResponseErrors(error_response, None)
        self.assertAPIResponseErrors(error_response, [])
        self.assertAPIResponseErrors(error_response, [
            ErrorCode.MISS_REQ_PARAMS, ErrorCode.WRONG_PARAM])
        self.assertAPIResponseErrors(error_response, [
            (ErrorCode.MISS_REQ_PARAMS, ),
            (ErrorCode.WRONG_PARAM, ),
        ])
        self.assertAPIResponseErrors(error_response, [
            ((ErrorCode.MISS_REQ_PARAMS, ErrorCode.WRONG_PARAM, ErrorCode.UNAUTHORIZED), ),
            ((ErrorCode.MISS_REQ_PARAMS, ErrorCode.WRONG_PARAM, ErrorCode.UNAUTHORIZED), ),
        ])
        self.assertAPIResponseErrors(error_response, [
            (ErrorCode.MISS_REQ_PARAMS, None),
            (ErrorCode.WRONG_PARAM, "wrong param"),
        ])
        self.assertAPIResponseErrors(error_response, [
            (ErrorCode.MISS_REQ_PARAMS, None, "param1"),
            (ErrorCode.WRONG_PARAM, None, ["param2", "param3"]),
        ])
        self.assertAPIResponseErrors(error_response, [
            (ErrorCode.MISS_REQ_PARAMS, None, "param1"),
            [ErrorCode.WRONG_PARAM, None, ("param2", "param3")],
        ])
        self.assertAPIResponseErrors(error_response, [
            (ErrorCode.MISS_REQ_PARAMS, "some text", "param1"),
            (ErrorCode.WRONG_PARAM, "wrong param", {"param2", "param3"}),
        ])
        self.assertAPIResponseErrors(error_response, [
            ((ErrorCode.MISS_REQ_PARAMS, ErrorCode.UNAUTHORIZED), ["some", "text"], {"param1", "param2", "param3"}),
            ((ErrorCode.WRONG_PARAM, ErrorCode.UNAUTHORIZED), ["wrong", "param"], {"param1", "param2", "param3"}),
        ])
        # (Same but different order)
        self.assertAPIResponseErrors(error_response, [
            ([ErrorCode.WRONG_PARAM, ErrorCode.UNAUTHORIZED], ("wrong", "param"), ("param2", "param1", "param3")),
            ({ErrorCode.MISS_REQ_PARAMS, ErrorCode.UNAUTHORIZED}, ["some", "text"], ["param2", "param1", "param3"]),
        ])

        # Assert fails
        data_response = make_data_response({}, ["warning"])
        empty_error_response = make_data_response({})
        empty_error_response.content = b'{"errors": []}'

        # (Is not an error response)
        with self.assertRaises(AssertionError):
            self.assertAPIResponseErrors(data_response, None)

        # (Is not an error response)
        with self.assertRaises(AssertionError):
            self.assertAPIResponseErrors(empty_error_response, None)
        with self.assertRaises(AssertionError):
            self.assertAPIResponseErrors(empty_error_response, [])

        # (Wrong error_msg)
        with self.assertRaises(AssertionError):
            self.assertAPIResponseErrors(error_response, [
                ((ErrorCode.MISS_REQ_PARAMS, ErrorCode.UNAUTHORIZED), ["some", "text", "WRONG"], {"param1", "param2", "param3"}),
                ((ErrorCode.WRONG_PARAM, ErrorCode.UNAUTHORIZED), ["wrong", "param", "WRONG"], {"param1", "param2", "param3"}),
            ])
        # (Wrong error_field)
        with self.assertRaises(AssertionError):
            self.assertAPIResponseErrors(error_response, [
                ((ErrorCode.MISS_REQ_PARAMS, ErrorCode.UNAUTHORIZED), ["some", "text"], {"param1", "param2", "param3"}),
                ((ErrorCode.WRONG_PARAM, ErrorCode.UNAUTHORIZED), ["wrong", "param"], {"param1", "param2"}),  # "param3" missed
            ])
        # (Too many errors than expected)
        with self.assertRaises(AssertionError):
            self.assertAPIResponseErrors(error_response, [
                ((ErrorCode.MISS_REQ_PARAMS, ErrorCode.UNAUTHORIZED), ["some", "text", "WRONG"], {"param1", "param2", "param3"}),
            ])
        # (Too many expected)
        with self.assertRaises(AssertionError):
            self.assertAPIResponseErrors(error_response, [
                ((ErrorCode.MISS_REQ_PARAMS, ErrorCode.UNAUTHORIZED), ["some", "text", "WRONG"], {"param1", "param2", "param3"}),
                ((ErrorCode.WRONG_PARAM, ErrorCode.UNAUTHORIZED), ["wrong", "param", "WRONG"], {"param1", "param2", "param3"}),
                ((ErrorCode.WRONG_PARAM, ErrorCode.UNAUTHORIZED), ["wrong", "param", "WRONG"], {"param1", "param2", "param3"}),
            ])
