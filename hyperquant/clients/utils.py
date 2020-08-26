import settings
from hyperquant.api import ParamName, Platform, PlatformCredentials
from hyperquant.clients.binance import BinanceRESTClient, BinanceWSClient
from hyperquant.clients.binance_future import BinanceFutureRESTClient, BinanceFutureWSClient
from hyperquant.clients.bitmex import BitMEXRESTClient, BitMEXWSClient


_rest_client_class_by_platform_id = {
    Platform.BINANCE: BinanceRESTClient,
    Platform.BITMEX: BitMEXRESTClient,
    Platform.BINANCE_FUTURE: BinanceFutureRESTClient,
}

_ws_client_class_by_platform_id = {
    Platform.BINANCE: BinanceWSClient,
    Platform.BITMEX: BitMEXWSClient,
    Platform.BINANCE_FUTURE: BinanceFutureWSClient,
}

# Cache of clients (may be private clients need also api_key key in lookup, not only platform_id)
_rest_client_by_platform_id = {}
_private_rest_client_by_platform_id = {}
_ws_client_by_platform_id = {}
_private_ws_client_by_platform_id = {}


def create_rest_client(platform_id,
                       is_private=False,
                       version=None,
                       credentials=None,
                       pivot_symbol=None,
                       **kwargs):
    return _create_client(platform_id, True, is_private, version, credentials,
                          pivot_symbol, **kwargs)


def get_or_create_rest_client(platform_id,
                              is_private=False,
                              lookup=None,
                              credentials=None,
                              pivot_symbol=None,
                              **kwargs):
    return _get_or_create_client(platform_id, True, is_private, lookup,
                                 credentials, pivot_symbol, **kwargs)


def create_ws_client(platform_id,
                     is_private=False,
                     version=None,
                     credentials=None,
                     pivot_symbol=None,
                     **kwargs):
    return _create_client(platform_id, False, is_private, version, credentials,
                          pivot_symbol, **kwargs)


def get_or_create_ws_client(platform_id,
                            is_private=False,
                            lookup=None,
                            credentials=None,
                            pivot_symbol=None,
                            **kwargs):
    return _get_or_create_client(platform_id, False, is_private, lookup,
                                 credentials, pivot_symbol, **kwargs)


def get_credentials_for(platform_id):
    platform_name = Platform.get_platform_name_by_id(platform_id)
    credentials_template = PlatformCredentials.get_template_by_id(platform_id)
    if not platform_name or not credentials_template:
        return None, None, None
    credentials = settings.CREDENTIALS_BY_PLATFORM.get(platform_name.upper(),
                                                       (None, None))
    if len(credentials) != len(credentials_template):
        raise ValueError(
            f'Invalid credentials for {platform_name}. Expected {credentials_template}.'
        )
    return PlatformCredentials.to_common_with_passphrase(credentials)


def _create_client(platform_id,
                   is_rest,
                   is_private=False,
                   version=None,
                   credentials=None,
                   pivot_symbol=None,
                   **kwargs):
    # Create
    platform_id = Platform.get_platform_id_by_name(platform_id)
    class_lookup = _rest_client_class_by_platform_id if is_rest else _ws_client_class_by_platform_id
    client_class = class_lookup.get(platform_id)
    if not client_class:
        return None

    if is_private:
        client = client_class(version=version,
                              credentials=credentials
                              or get_credentials_for(platform_id),
                              pivot_symbol=pivot_symbol,
                              **kwargs)
        client.platform_id = platform_id  # If not set in class
    else:
        client = client_class(version=version)
        client.platform_id = platform_id  # If not set in class

        # For Binance's "historicalTrades" endpoint
        if platform_id == Platform.BINANCE:
            api_key, *_ = get_credentials_for(platform_id)
            client.set_credentials(api_key, None)
    return client


def _get_or_create_client(platform_id,
                          is_rest,
                          is_private=False,
                          lookup=None,
                          credentials=None,
                          pivot_symbol=None,
                          **kwargs):
    # Get
    if lookup is None:
        if is_rest:
            lookup = _private_rest_client_by_platform_id if is_private else _rest_client_by_platform_id
        else:
            lookup = _private_ws_client_by_platform_id if is_private else _ws_client_by_platform_id
    if platform_id in lookup:
        if is_private:
            if platform_id not in lookup:
                lookup[platform_id] = {}
            if credentials not in lookup[platform_id]:
                lookup[platform_id][credentials] = {}
            client = lookup[platform_id][credentials].get(pivot_symbol)
        else:
            client = lookup.get(platform_id)
        if client:
            return client

    # Create
    if is_private:
        client = _create_client(platform_id,
                                is_rest,
                                is_private,
                                credentials=credentials,
                                pivot_symbol=pivot_symbol,
                                **kwargs)
        if platform_id not in lookup:
            lookup[platform_id] = {}
        if credentials not in lookup[platform_id]:
            lookup[platform_id][credentials] = {}
        lookup[platform_id][credentials][pivot_symbol] = client
    else:
        lookup[platform_id] = client = _create_client(platform_id, is_rest,
                                                      is_private, **kwargs)
    return client


def set_up_symbols_lookup_on_client(client, platform_id):
    platform_symbol_by_common_symbol_by_platform_id = {
        Platform.BINANCE: {
            "BTCUSD": "BTCUSDT",
        },
        Platform.BITMEX: {
            "BTCUSD": "XBTUSD",
        }
    }
    platform_symbol_by_common_symbol = \
        platform_symbol_by_common_symbol_by_platform_id.get(platform_id)
    if not client or not platform_symbol_by_common_symbol:
        return

    if not client.converter.param_value_lookup.get(ParamName.SYMBOL):
        client.converter.param_value_lookup[ParamName.SYMBOL] = {}
    if not client.converter.param_value_reversed_lookup.get(ParamName.SYMBOL):
        client.converter.param_value_reversed_lookup[ParamName.SYMBOL] = {}
    for s, ps in platform_symbol_by_common_symbol.items():
        client.converter.param_value_lookup[ParamName.SYMBOL][s] = ps
        client.converter.param_value_reversed_lookup[ParamName.SYMBOL][ps] = s
