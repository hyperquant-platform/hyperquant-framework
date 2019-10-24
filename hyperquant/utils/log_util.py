from copy import deepcopy


def make_short_str(string, max_len=200, is_strip_in_the_middle=True):
    string = str(string)
    if len(string) <= max_len or max_len < 0:
        return string

    if is_strip_in_the_middle:
        slice_num = round(max_len / 2)
        return string[:slice_num] + "<...>" + string[-slice_num:]

    return string[:max_len] + "..."


def items_to_interval_string(items, max_show_items_count=2):
    if not items or not isinstance(items, (list, tuple)):
        return items
    count = len(items)

    if max_show_items_count >= count:
        return "(count): %s (items): %s" % (count, items)
    if max_show_items_count == 2:
        return "(count): %s (first..last): %s .. %s" % (count, items[0], items[-1])
    return "(count): %s (items): [%s .. %s]" % (
        count, ", ".join(map(str, items[0:max_show_items_count - 1])), items[-1])


def protect_secret_data(data):
    if not isinstance(data, dict):
        return data
    data = deepcopy(data)
    for k, v in data.items():
        key = k.upper()
        if k and v and ("PASS" in key or "PWD" in key):
            data[k] = v[:1] + "..." + v[-1:] if isinstance(v, str) and len(v) > 4 else "..."
    return data
