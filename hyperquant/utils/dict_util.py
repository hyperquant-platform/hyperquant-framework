import logging
from collections import Iterable
from decimal import Decimal


def inverse_dict(lookup):
    if not lookup:
        return lookup

    result = {}
    for k, v in lookup.items():
        if v in result:
            if not isinstance(result[v], list):
                result[v] = [result[v]]
            result[v].append(k)
        else:
            result[v] = [k]
    return result


def convert_items_to_grouped_lookup(list_items, field_count):
    if field_count < 1:
        return list_items

    field_count += 1
    lookup = {}
    for item in list_items:
        last_index = min(field_count, len(item)) - 1
        sub_lookup = lookup
        for i, prop in enumerate(item):
            is_final_prop = i == last_index
            if is_final_prop:  # and prop not in lookup[prop]:
                sub_lookup.extend(item[i:])
                break
                # sub_lookup.append(prop)
            else:
                is_pre_final_prop = i == last_index - 1
                if prop not in sub_lookup:
                    sub_lookup[prop] = [] if is_pre_final_prop else {}
                sub_lookup = sub_lookup[prop]
    return lookup


def group_items(items, fields):
    if not items or not fields:
        return items

    lookup = {}
    field_count = len(fields)
    for item in items:
        sublookup = lookup
        for i, field in enumerate(fields):
            is_final_field = i == field_count - 1
            value = item[field] if isinstance(item, dict) else getattr(item, field)
            sublookup[value] = sublookup.get(value, [] if is_final_field else {})
            sublookup = sublookup[value]
            if is_final_field:
                sublookup.append(item)
    return lookup


def filter_keys(target_dict, properties_source):
    is_source_iterable = isinstance(properties_source, Iterable)
    return {k: v for k, v in target_dict.items()
            if hasattr(properties_source, k) or (k in properties_source if is_source_iterable else False)} if \
        isinstance(target_dict, dict) and properties_source is not None else target_dict


def deepcopy(obj):
    # Deep copy, but only for standard data structures: dict, list, tuple, set
    if isinstance(obj, dict):
        return {deepcopy(k): deepcopy(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [deepcopy(v) for v in obj]
    elif isinstance(obj, tuple):
        return tuple(deepcopy(v) for v in obj)
    elif isinstance(obj, set):
        return {deepcopy(v) for v in obj}
    return obj


def check_is_sub_data(data, sub_data):
    if data == sub_data:
        return True
    if data is None or sub_data is None:
        return False
    if isinstance(sub_data, (int, float, Decimal)):
        return data >= sub_data
    if isinstance(sub_data, str):
        return sub_data in data
    if type(data) != type(sub_data):
        return False

    if isinstance(sub_data, dict):
        for k, v in sub_data.items():
            if isinstance(v, (dict, list, tuple)):
                # Nesting check is only for dicts
                if k not in data or not check_is_sub_data(data[k], v):
                    logging.debug(f"check_is_sub_data: Key {k} not in dict {data}" if k not in data
                                  else f"check_is_sub_data: {v} \nis not sub_data of {data[k]}")
                    return False
            elif k not in data or data[k] != v:
                logging.debug(f"check_is_sub_data: Key {k} not in dict {data}" if k not in data
                              else f"check_is_sub_data: {k} {v} != {data[k]}")
                return False
        return True

    for k, v in enumerate(sub_data):
        # (Check for __getitem__ to know that data supports indexing)
        if v not in data and (not hasattr(data, "__getitem__") or not check_is_sub_data(data[k], v)):
            return False
    return True
