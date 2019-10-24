import math
from decimal import Decimal


def drop_trailing_zeros(value: Decimal) -> Decimal:
    if value == 0:
        return Decimal('0')
    assert isinstance(value, Decimal)
    t = value.as_tuple()
    i = -1
    while i >= t[2] and t[1][i] == 0:
        i -= 1
    if i == -1:
        return value
    i += 1
    return Decimal((t[0], t[1][:i], t[2] - i))


def round_to_tick(value, tick_size=0, round_fun=None):
    if not value or not tick_size:
        return value
    if not round_fun:
        round_fun = round
    if tick_size < 0:
        tick_size = -tick_size
    if isinstance(value, Decimal):
        tick_size = Decimal(tick_size)

    # Fix math.floor/ceil for precision
    real_round_fun = (lambda val, prec: round_fun(val * pow(10, prec)) / pow(10, prec)) \
        if round_fun != round else round_fun

    log = math.log10(tick_size)
    precision = math.ceil(-log)
    value = real_round_fun(value, precision)

    rest = value % tick_size
    value1 = value - rest
    value2 = value1 + tick_size
    if value2 - value < value - value1:
        result = value2
    else:
        result = value1

    if precision > 0:
        result = real_round_fun(result, precision)

    return result
