## HyperQuant Framework


### Features:

- Getting all public data from exchanges
- Operating with client private data
- Implementing unified easy-to-use object structure for all exchanges data
- Handling errors and exceptions

### Currently supported exchanges:

- Bitmex
- Binance

_More exchanges will be supported soon!_


### Starting library tests:
Note that you need credetials to settings.py file before start tests

    pipenv install
    pipenv shell
    python run_tests.py


### Quick Start:

Firstly, you need to create API Key for the desired exchange. Look through the exchange documentation for more information.
[Generate API keys on Binance](https://www.binance.com/login.html?callback=/userCenter/createApi.html) 

```python
from typing import List

from hyperquant.clients import Candle, Trade, OrderBook, Ticker, Quote, Balance, MyTrade, Order
from hyperquant.clients.binance import BinanceRESTClient, BinanceWSClient
from hyperquant.clients.bitmex import BitMEXRESTClient
from hyperquant.api import CandleInterval, CurrencyPair, OrderType, Direction, OrderStatus, Endpoint,\
    ParamName, OrderBookDepthLevel

# Create public rest client to get some market data
public_binance_rest_client = BinanceRESTClient()
public_bitmex_client = BitMEXRESTClient()

# Get all trading pairs from exchange
pairs: List[CurrencyPair] = public_binance_rest_client.fetch_currency_pairs()
candles: List[Candle] = public_binance_rest_client.fetch_candles(symbol='ETHBTC', interval=CandleInterval.HRS_1,
                                                                 limit=10)
trades: List[Trade] = public_binance_rest_client.fetch_trades_history(symbol='BNBBTC', limit=100)
order_book: OrderBook = public_binance_rest_client.fetch_order_book(symbol='ETHBTC')
ticker: Ticker = public_binance_rest_client.fetch_ticker(symbol='ETHBTC')
tickers: List[Ticker] = public_bitmex_client.fetch_tickers()
quote: Quote = public_binance_rest_client.fetch_quote(symbol='ETHBTC')


# To get any private user data you need firstly to set api_keys for each platform
# Usually it has two keys: public and secret. For some exchanges passphrase is also needed (Binance and Bitmex do not have such requirements)
# TODO replace to stubs
binance_api_key = "Your_api_key"
binance_api_secret = "Your_api_secret"


# Pivot Symbol is the main currency of the client created
# It will be used for converting other currency on client balance to pivot symbol currency

private_binance_rest_client = BinanceRESTClient(api_key=binance_api_key,
                                                api_secret=binance_api_secret,
                                                pivot_symbol='BTC')
balances: List[Balance] = private_binance_rest_client.fetch_balance()
my_trades: List[MyTrade] = private_binance_rest_client.fetch_my_trades('ETHBTC')

# Create a new buy order with the cheapest price at order book
cheapest_price = order_book.bids[-1].price
min_amount = BinanceRESTClient.helper.get_symbol_min_amount(BinanceRESTClient.platform_id, 'ETHBTC')
new_order: Order = private_binance_rest_client.create_order(symbol='ETHBTC',
                                                            order_type=OrderType.LIMIT,
                                                            direction=Direction.BUY,
                                                            amount=min_amount,
                                                            price=cheapest_price)

# Now cancel previously created order and check the returned result
canceled_order = private_binance_rest_client.cancel_order(new_order)
assert canceled_order.order_status == OrderStatus.CANCELED

# It's also possible to cancel all created orders at once
new_order_1: Order = private_binance_rest_client.create_order(symbol='ETHBTC',
                                                              order_type=OrderType.LIMIT,
                                                              direction=Direction.BUY,
                                                              amount=min_amount,
                                                              price=cheapest_price)
new_order_2: Order = private_binance_rest_client.create_order(symbol='ETHBTC',
                                                              order_type=OrderType.LIMIT,
                                                              direction=Direction.BUY,
                                                              amount=min_amount,
                                                              price=cheapest_price)

canceled_order = private_binance_rest_client.cancel_all_orders(symbol='ETHBTC')

# Update orders' statuses
new_order_1 = private_binance_rest_client.fetch_order(new_order_1)
new_order_2 = private_binance_rest_client.fetch_order(new_order_2)

assert new_order_1.order_status == OrderStatus.CANCELED
assert new_order_2.order_status == OrderStatus.CANCELED

# To get your lastest orders history
# Note that returned result doesn't show all your orders cause response limit
# Limit can vary for different exchanges, view exchange API documentation to find more
# or just make some test requests to find out limits of the exchange you are interested in
last_orders = private_binance_rest_client.fetch_orders(symbol='ETHBTC')

# To get all your orders use 'from_item' parameter
all_orders = []
ask_from = 0
while True:
    loaded_orders = private_binance_rest_client.fetch_orders(symbol='ETHBTC', from_item=ask_from)
    if not loaded_orders:
        break
    all_orders.extend(loaded_orders)
    ask_from = loaded_orders[-1]

# Getting your positions (read more about the positions on spot exchange below on readme)
order = private_binance_rest_client.create_order(symbol='ETHBTC',
                                                 order_type=OrderType.MARKET,
                                                 direction=Direction.BUY,
                                                 price=0,
                                                 amount=min_amount)
assert order.order_status == OrderStatus.FILLED
positions = private_binance_rest_client.get_positions()
closed_position = private_binance_rest_client.close_position(positions[0])
# private_binance_rest_client.close_all_positions()
# set_leverage


# Getting Data through WS

def on_data(data):
    print('Get public data: ', data)


def on_private_data(data):
    print('Get private data: ', data)


public_binance_ws_client = BinanceWSClient()
public_binance_ws_client.on_data = on_data

public_binance_ws_client.subscribe(endpoints=[Endpoint.TRADE, Endpoint.QUOTE], symbols='ETHBTC')
public_binance_ws_client.subscribe(endpoints=[Endpoint.CANDLE], symbols='ETHBTC',
                                   **{ParamName.INTERVAL: CandleInterval.HRS_1})
public_binance_ws_client.subscribe(endpoints=[Endpoint.ORDER_BOOK], symbols='ETHBTC',
                                   **{ParamName.LEVEL: OrderBookDepthLevel.DEEP})

private_binance_rest_client = BinanceWSClient(api_key=binance_api_key,
                                              api_secret=binance_api_secret,
                                              pivot_symbol='BTC')
private_binance_rest_client.on_data = on_private_data

# Note that Binance not return any private data if there wasn't any corresponding event
private_binance_rest_client.subscribe(endpoints=[Endpoint.TRADE_MY, Endpoint.ORDER, Endpoint.BALANCE],
                                      symbols='ETHBTC')                    
```

#### Additional information on positions and pivot symbols:

Combination of using pivot symbol and position enables to monitor your current balances state
and make decisions basing on current situation. While futures markets (like all Bitmex markets) have their 
own position objects, spot markets (like Binance) don't allow to look at your current profit and loss. 

So, pivot symbol is your wallet base currency for spot markets. Other balances different from the selected pivot 
symbol will be used like "virtual positions" and closing the position will cause selling all these 
currencies.

If you do not need such function just select any existing exchange currency by client initialisation
and ignore positions methods for spot markets.


### Use-case

#### Simple Bot Creation:

Let's create a simple bot that monitors BTC price and when the difference between hour candle open and close is 
more then 5% the bot buys on amount of 10% of our total balance and sells in case of the reverse situation.

```python
rest_client = BinanceRESTClient(api_key="Your_api_key",
                        api_secret="Your_api_secret",
                                pivot_symbol='BTC')
# First save the start balance
start_balance = rest_client.fetch_balance()

# Then create subscription on BitMex candles to monitor BTCUSDT price difference (Note XBT mean BTC on BitMex)


def on_candle(candles):
    for candle in candles:
        if candle.price_open / candle.price_close > 1.05:
            rest_client.create_order(symbol='BTCUSDT',
                                     order_type=OrderType.MARKET,
                                     direction=Direction.BUY,
                                     price=0,
                                     amount=start_balance / 10)

        if candle.price_close / candle.price_open > 1.05:
            rest_client.create_order(symbol='BTCUSDT',
                                     order_type=OrderType.MARKET,
                                     direction=Direction.SELL,
                                     price=0,
                                     amount=start_balance / 10)

ws_client = BinanceWSClient()
ws_client.subscribe(endpoints=[Endpoint.CANDLE], symbols='BTCUSDT',
                    **{ParamName.INTERVAL: CandleInterval.HRS_1})
ws_client.on_data = on_candle

while True:
    time.sleep(1)
```