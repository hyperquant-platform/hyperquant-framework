import logging
import time
from datetime import datetime

from dateutil import parser
from dateutil.tz import tzutc
from dateutil.utils import default_tzinfo


def get_timestamp_ms(value, timestamp_name="timestamp", is_parse_timestamp_only=False):
    if not value and value != 0:
        return None

    result = None

    # From complex object type
    if isinstance(value, dict):
        value = value.get(timestamp_name)
    elif hasattr(value, timestamp_name) and not isinstance(value, datetime):
        value = getattr(value, timestamp_name)

    if not is_parse_timestamp_only and isinstance(value, datetime):
        value = default_tzinfo(value, tzutc())
        value = value.timestamp()

    # From scalar type
    if isinstance(value, (int, float)):
        result = value
    elif isinstance(value, str):
        try:
            result = float(value)
        except ValueError:
            if not is_parse_timestamp_only:
                try:
                    dt = parser.parse(value)
                    dt = default_tzinfo(dt, tzutc())
                    result = dt.timestamp() * 1000
                except:
                    logging.exception("Error while parsing %s as datetime. Return None.", value)
                    result = None

    # s -> ms
    if result is not None and result < 1500000000 * 10:
        result *= 1000
    result_int = int(result) if isinstance(result, float) else result

    return result_int if result_int == result else result


def get_timestamp_s(value, timestamp_name="timestamp", is_parse_timestamp_only=False):
    timestamp_ms = get_timestamp_ms(value, timestamp_name, is_parse_timestamp_only)
    if not timestamp_ms:
        return timestamp_ms
    result = timestamp_ms / 1000
    result_int = int(result)
    return result_int if result_int == result else result


def get_timestamp_iso(value, timestamp_name="timestamp", is_parse_timestamp_only=False):
    timestamp_ms = get_timestamp_ms(value, timestamp_name, is_parse_timestamp_only)
    if not timestamp_ms:
        return timestamp_ms
    timestamp_s = timestamp_ms / 1000
    result = datetime.utcfromtimestamp(timestamp_s).isoformat()
    return result


def get_timestamp(value, use_milliseconds=True, timestamp_name="timestamp", is_parse_timestamp_only=False):
    return get_timestamp_ms(value, timestamp_name, is_parse_timestamp_only) \
        if use_milliseconds else get_timestamp_s(value, timestamp_name, is_parse_timestamp_only)


_start_time_by_key = {}
_stop_time_by_key = {}


def start_timings(*keys):
    current_time = time.time()
    for key in keys:
        _start_time_by_key[key] = current_time
        if key in _stop_time_by_key:
            del _stop_time_by_key[key]
    return [0] * len(keys)


def stop_timings(*keys):
    current_time = time.time()
    result = []
    for key in keys:
        _stop_time_by_key[key] = current_time
        result.append(current_time - _start_time_by_key.get(key, current_time))
    return result


def get_timings(*keys):
    current_time = time.time()
    result = []
    for key in keys:
        from_time = _stop_time_by_key.get(key, current_time)
        result.append(from_time - _start_time_by_key.get(key, from_time))
    return result


def get_timings_str(*keys):
    timings = get_timings(*keys)
    result = "Elapsed time for"
    for key, timing in zip(keys, timings):
        result += " %s: %s s" % (key, timing)
    return result
