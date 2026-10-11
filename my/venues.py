import builtins
import logging
import json
import time
import requests
import asyncio
import websockets
from typing import Callable, List
from functools import wraps
from http.client import HTTPConnection  # Import for debugging
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# Enable detailed HTTP logging for debugging:
HTTPConnection.debuglevel = 1
logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.INFO)
requests_log.propagate = True

class Venue:
	def __init__(self, instruments, venue, api_key=None):
		# Get parameters specific to this instrument
		self.instruments = instruments
		self.venue = venue
		self.__current_symbol = None
		self.__current_instrument = None
		self.api_key = api_key
		self.callbacks: List[Callable[[dict], None]] = []

	def _instrument(self, symbol):
		if symbol != self.__current_symbol:
			instrument_data = [i for i in self.instruments if i['symbol'] == symbol]
			self.__current_instrument = instrument_data[0] if instrument_data else None
			self.__current_symbol = symbol if instrument_data else None 

		return self.__current_instrument
	
	def tick(self, symbol):
		if i := self._instrument(symbol):
			return i['tickSize']
		return None
	
	def lot(self, symbol):
		if i := self._instrument(symbol):
			return i['lotSize']
		return None
	
	def lot_value(self, symbol):
		if i := self._instrument(symbol):
			if i['isInverse']:
				return self.lot(symbol)  # *1.0: Inverse instruments are quoted in USD
			else:
				return self.lot(symbol)*self.mark_price(symbol)
		return None
	
	def standard_size(self, symbol, fiat_amount):
		instrument = self._instrument(symbol)
		if not instrument:
			raise ValueError(f"Symbol {symbol} not found")
        
		mark_price = self.mark_price(symbol)
		lot_size = instrument['lotSize']
		return int(fiat_amount / (mark_price * lot_size))

	def mark_price(self, symbol):
		raise NotImplementedError("Subclasses should implement this method")
	
	# WebSocket Client Base Functionality
	def add_callback(self, callback: Callable[[dict], None]):
		"""Add a callback function that will be called when a new price update is received."""
		self.callbacks.append(callback)
	
	def callback(self, func: Callable[[dict], None]) -> Callable[[dict], None]:
		"""Decorator to register a function as a callback, supporting both functions and instance methods."""
		@wraps(func)
		def wrapper(*args, **kwargs):
			if args and hasattr(args[0], func.__name__):  # Check if it's a method
				method_self = args[0]  # Extract 'self' from instance method
				def bound_callback(price_data):
					func(method_self, price_data)  # Call method with 'self'
				self.add_callback(bound_callback)
			else:
				self.add_callback(func)
			return func
		return wrapper

	async def _stream_prices(self, stream_url):
		"""
		Streams prices from a WebSocket, manually managing the connection.
		"""
		parsed_url = urlparse(stream_url)
		host = parsed_url.netloc
		path = parsed_url.path + "?" + parsed_url.query if parsed_url.query else parsed_url.path

		# Manually construct the headers.  Note:  These MUST be bytes.
		headers = [
			(b"Host", host.encode()),
			(b"Authorization", f"Bearer {self.api_key}".encode()),
			(b"Connection", b"Upgrade"),
			(b"Upgrade", b"websocket"),
			(b"Sec-WebSocket-Key", b"dGhlIHNhbXBsZSBub25jZQ=="),  # Base64 encoded, but value doesn't matter
			(b"Sec-WebSocket-Version", b"13"),
		]

		# Get the event loop
		loop = asyncio.get_running_loop()

		try:
			# Create the connection.  This does NOT automatically handle the handshake.
			transport, protocol = await loop.create_connection(
				lambda: websockets.client.WebSocketClientProtocol(extra_headers=headers),
				host=parsed_url.hostname,
				port=parsed_url.port,
				ssl=True,  # OANDA requires SSL
			)

			# Manually perform the handshake.
			try:
				await protocol.handshake(
					host=host,
					path=path,
					subprotocols=None,
					extra_headers=headers,
				)
				print("WebSocket connection opened.")

				# Now we can receive messages.
				while True:
					try:
						message = await protocol.recv()
						data = json.loads(message)
						if "bids" in data and "asks" in data:
							bid = data["bids"][0]["price"]
							ask = data["asks"][0]["price"]
							price_update = {"bid": bid, "ask": ask}
							for callback in self.callbacks:
								callback(price_update)
					except websockets.exceptions.ConnectionClosed as e:
						print(f"WebSocket connection closed: {e}")
						break  # Exit the loop on connection close
					except Exception as e:
						print(f"An unexpected error occurred: {e}")
						break # Exit loop on other errors.

			except websockets.exceptions.InvalidHandshake as e:
				print(f"WebSocket handshake failed: {e}")
			except Exception as e:
				print(f"An unexpected error occurred during handshake: {e}")
			finally:
				await protocol.close()
				print("WebSocket connection closed.")

		except OSError as e:
			print(f"Failed to connect: {e}")
		except Exception as e:
			print(f"An unexpected error occurred during connection: {e}")

	async def start_stream(self, stream_url):
		"""Start the WebSocket streaming in an asyncio event loop."""
		await self._stream_prices(stream_url)
		

class BitMEX(Venue):
	NAME = 'BitMEX'
	INSTRUMENT_ENDPOINT = 'instrument'
	INSTRUMENT_PAGE_SIZE = 500
	ALGO_PARAMETERS = { 'tickSize': 'float'
					  , 'lotSize': 'int'
					  , 'markPrice': 'float'
					  , 'isInverse': 'bool'
					  , 'multiplier': 'float'
					  , 'settlCurrency': 'str'
					  , 'symbol': 'str'
					  , 'isQuanto': 'bool'
					  }
	RATE_LIMIT_DELAY = 0.5

	def __type_parameters(self, instruments):
		typed_instruments = []
		for i in instruments:
			ti = {}
			for p, v in BitMEX.ALGO_PARAMETERS.items():
				ti[p] = getattr(builtins, v)(i[p]) if i[p] else i[p]
			typed_instruments.append(ti)
		return typed_instruments

	def _fetch_instruments(self):
		"""Fetch all instruments from the API with pagination"""
		instrument_count = 0
		all_instruments_data = []
		
		while True:
			instruments = self.trading.call_endpoint(
				self.NAME,
				self.INSTRUMENT_ENDPOINT,
				'public',
				method='GET', params={
					'count': self.INSTRUMENT_PAGE_SIZE,
					'start': instrument_count,
					'columns': json.dumps([*self.ALGO_PARAMETERS])
				})
			instruments_data = [i for i in instruments['data'] if i.get('settlCurrency') and i.get('markPrice')]
			all_instruments_data += instruments_data
			current_count = len(instruments['data'])
			instrument_count += current_count
			logger.info(f"{instrument_count=}")
			if current_count < self.INSTRUMENT_PAGE_SIZE: break
			time.sleep(self.rate_limit_delay)
		
		return all_instruments_data

	def __init__(self, trading, rate_limit_delay=RATE_LIMIT_DELAY, is_signal=False):
		self.trading = trading
		self.rate_limit_delay = rate_limit_delay
		self.is_signal = is_signal
		instruments_data = self._fetch_instruments() if not is_signal else []
		super().__init__(self.__type_parameters(instruments_data), self.NAME)

	def get_instruments(self):
		return self._instruments

	def get_instrument(self, symbol):
		return self._instrument(symbol)

	def mark_price(self, symbol):
		return self._instrument(symbol)['markPrice']
	
	def get_contract_multiplier(self, symbol):
		return self._instrument(symbol)['multiplier']
		
	def get_btc_mark_price(self):
		"""Get the XBT mark price from the BitMEX API."""
		if self.is_signal:
			raise NotImplementedError("Signals interface does not support getting the BTC mark price")
		
		xbtparams = self.trading.call_endpoint(
			self.NAME,
			self.INSTRUMENT_ENDPOINT,
			'public',
			method='GET', params={
				'symbol': 'XBT', 'columns': 'markPrice'
		})
		logger.info(f"{xbtparams=}")
		mark_price = float(xbtparams['data'][0]['markPrice'])
		logger.info(f"{mark_price=}")
		return mark_price
	
	def get_contract_usd_price(self, symbol):
		"""Get the contract price from the BitMEX API."""
		logger.info(f"{symbol=}")
		adjustment_multiplier = 0.00001  # Not clear why this is needed.

		mark_price = self.mark_price(symbol)
		logger.info(f"{mark_price=}")
		multiplier = self.get_contract_multiplier(symbol)
		logger.info(f"{multiplier=}")
		btc_mark_price = self.get_btc_mark_price()
		logger.info(f"{btc_mark_price=}")
		price = btc_mark_price*0.001*mark_price*multiplier*adjustment_multiplier
		logger.info(f"{price=}")
		return price

	def standard_size(self, symbol, dollar_amount):
		if self.is_signal:
			raise NotImplementedError("Signals interface does not support getting the standard size")
		
		d = self._instrument(symbol)
		mark_price = d['markPrice']

		if d['isInverse']: mark_price = 1/mark_price
		mark_multiplier = abs(float(d['multiplier']))*mark_price
		
		xbtparams = self.trading.call_endpoint(
			self.NAME,
			self.INSTRUMENT_ENDPOINT,
			'public',
			method='GET', params={
				'symbol': 'XBT', 'columns': 'markPrice'
		})
		xbtMark = float(xbtparams['data'][0]['markPrice'])

		USDt_in_USD = 1e-6  # USDt in $: https://blog.bitmex.com/api_announcement/api-usage-for-usdt-contracts/
		                    # and: https://www.bitmex.com/app/restAPI 
		BTC_in_SATOSHI = 1e8
		mark = xbtMark/BTC_in_SATOSHI if d['settlCurrency'] == 'XBt' else USDt_in_USD
		minimum_dollar_size = int(d['lotSize'])*mark*mark_multiplier
		assert(dollar_amount > minimum_dollar_size)
		dollar_multiple = dollar_amount//minimum_dollar_size;
		lot = self.lot(symbol)
		return dollar_multiple*lot
	
	def place_order(self, symbol, side, quantity, order_type='market', price=None):
		side = side.capitalize()
		# quantity depends on whether the instrument is 
		if order_type == 'market':
			# Use ProfitView's create_market_order method
			order = self.trading.create_market_order(
				venue=self.NAME,
				sym=symbol,
				side=side,
				size=quantity
			)
		elif order_type == 'limit' and price is not None:
			# Use ProfitView's create_limit_order method
			order = self.trading.create_limit_order(
				venue=self.NAME,
				symbol=symbol,
				side=side,
				size=quantity,
				price=price
			)
		else:
			raise ValueError("Invalid order type or missing price for limit order")
		
		return order


class Woo(Venue):
	NAME = 'Woo'
	SRC = 'woo'
	FUTURES_URL = 'https://api.woo.org/v1/public/futures'

	def __init__(self, trading, is_signal=False):
		self.trading = trading
		self.is_signal = is_signal
		super().__init__([], self.NAME)

	def mark_price(self, symbol):
		"""Mark price for a WOO X perpetual, such as PERP_BTC_USDT."""
		if not isinstance(symbol, str) or not symbol.startswith('PERP_'):
			raise ValueError(f"{symbol} is not a WOO X perpetual contract")
		response = requests.get(f"{self.FUTURES_URL}/{symbol}", timeout=10)
		response.raise_for_status()
		info = response.json().get('info') or {}
		if info.get('symbol') != symbol or info.get('mark_price') is None:
			raise ValueError(f"WOO X did not return a mark price for {symbol}")
		return float(info['mark_price'])

class OANDA(Venue):
	NAME = 'OANDA'
	INSTRUMENTS_ENDPOINT = 'v3/accounts/{account_id}/instruments'
	PRICING_ENDPOINT = 'v3/accounts/{account_id}/pricing'
	TRADING_ENDPOINT = 'v3/accounts/{account_id}/orders'
	HEADERS = { 'Content-Type': 'application/json' }

	def __init__(self, trading, account_id, api_key, endpoint='https://api-fxtrade.oanda.com'):
		logger.info(f"{endpoint=}")
		self.trading = trading
		self.account_id = account_id
		logger.info(f"{self.account_id=}")
		self.api_key = api_key
		logger.info(f"{self.api_key=}")
		self.HEADERS['Authorization'] = f'Bearer {self.api_key}'
		self.HEADERS['Content-Type'] = 'application/json'
		logger.info(f"{self.HEADERS=}")
		self.instruments_endpoint = f'{endpoint}/{self.INSTRUMENTS_ENDPOINT}'.format(account_id=account_id)
		logger.info(f"{self.instruments_endpoint=}")
		self.pricing_endpoint = f'{endpoint}/{self.PRICING_ENDPOINT}'.format(account_id=account_id)
		self.trading_endpoint = f'{endpoint}/{self.TRADING_ENDPOINT}'.format(account_id=account_id)
		logger.info(f"{self.trading_endpoint=}")
		# logger.info(f"{self.instruments_endpoint=}")
		# logger.info(f"{self.pricing_endpoint=}")
		instruments_data = self.get_instruments()
		super().__init__(instruments_data, self.NAME, api_key)

	def get_instruments(self):
		response = requests.get(self.instruments_endpoint.format(account_id=self.account_id), headers=self.HEADERS)
		response.raise_for_status()
		data = response.json()
		# logger.info(f"{data=}")
		instruments = [
			{
				'symbol': i['name'],
				'pipLocation': float(i['pipLocation']),
				'tickSize': 10**float(i['pipLocation']),
				'lotSize': 1,  # OANDA does not use lot size in the traditional sense
				'isInverse': i.get('isInverted', False),
				'multiplier': 1,
				'settlCurrency': i.get('quoteCurrency', 'USD'),
				'markPrice': float(i.get('closeoutAsk', 1))
			}
			for i in data['instruments']
		]
		return instruments

	def mark_price(self, symbol):
		endpoint = self.pricing_endpoint.format(account_id=self.account_id)
		logger.info(f"{endpoint=}")
		headers = self.HEADERS
		logger.info(f"{headers=}")
		response = requests.get(
			endpoint,
			headers=headers,
			params={'instruments': symbol}
		)
		response.raise_for_status()
		pricing_data = response.json()
		return float(pricing_data['prices'][0]['closeoutAsk'])

	def place_order(self, symbol, side, quantity, order_type='MARKET'):
		logger.info(f"{symbol=}")
		side = side.lower()
		logger.info(f"OANDA: {side=}")	
		quantity = quantity if side == 'buy' else -quantity
		logger.info(f"OANDA: {quantity=}")	

		quantity = int(quantity)

		order_data = {
			"order": {
				"instrument": symbol,
				"units": quantity,
				"type": order_type,
				"positionFill": "DEFAULT"
			}
		}
		logger.info(f"OANDA: {order_data=}")
		endpoint = self.trading_endpoint.format(account_id=self.account_id)
		logger.info(f"{endpoint=}")
		headers = self.HEADERS
		logger.info(f"{headers=}")
		response = requests.post(endpoint, json=order_data, headers=headers)
		response.raise_for_status()
		return response.json()

	def handle_price_update(self, price_data):
		# Handle real-time price updates
		print(f"Received price update: {price_data}")


class Alpaca(Venue):
    NAME = 'Alpaca'
    INSTRUMENTS_ENDPOINT = 'v2/assets'
    PRICING_ENDPOINT = 'v2/stocks/{symbol}/quotes/latest'
    HEADERS = {'Content-Type': 'application/json'}
    
    def __init__(self, trading, api_key, secret_key, 
				 trading_endpoint='https://paper-api.alpaca.markets', 
				 data_endpoint='https://data.alpaca.markets',
				 stream_url='wss://stream.data.alpaca.markets/v2/iex'):  # Use 'sip' for paid plan
        self.trading = trading
        self.api_key = api_key
        self.secret_key = secret_key
        self.stream_url = stream_url
        self.HEADERS['APCA-API-KEY-ID'] = self.api_key
        self.HEADERS['APCA-API-SECRET-KEY'] = self.secret_key
        
        # Use trading endpoint for assets
        self.instruments_endpoint = f'{trading_endpoint}/{self.INSTRUMENTS_ENDPOINT}'
        
        # Use data endpoint for quotes
        self.pricing_endpoint = f'{data_endpoint}/{self.PRICING_ENDPOINT}'
        
        instruments_data = self.get_instruments()
        super().__init__(instruments_data, self.NAME, api_key)
        self.trading_endpoint = trading_endpoint  # Store for place_order compatibility
    
    def get_instruments(self):
        response = requests.get(self.instruments_endpoint, headers=self.HEADERS)
        response.raise_for_status()
        data = response.json()
        instruments = [
            {
                'symbol': i['symbol'],
                'tickSize': 0.01,  # Alpaca typically supports decimal precision
                'lotSize': 1       # Alpaca uses whole shares
            }
            for i in data if i['tradable']
        ]
        return instruments

    def mark_price(self, symbol):
        try:
            url = self.pricing_endpoint.format(symbol=symbol)
            logger.debug(f"Fetching quote from: {url}")
            response = requests.get(url, headers=self.HEADERS)
            response.raise_for_status()
            pricing_data = response.json()
            if 'quote' in pricing_data and 'ap' in pricing_data['quote']:
                return float(pricing_data['quote']['ap'])  # Ask price as mark price
            else:
                logger.warning(f"Unexpected response format for symbol {symbol}: {pricing_data}")
                return None
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error occurred: {e} - Response content: {response.content}")
            return None
        except Exception as e:
            logger.error(f"An error occurred: {e}")
            return None

    def place_order(self, symbol, side, quantity, order_type='market'):
        url = f'{self.trading_endpoint}/v2/orders'
        order_data = {
            "symbol": symbol,
            "qty": quantity,
            "side": side,
            "type": order_type,
            "time_in_force": "gtc"
        }
        response = requests.post(url, json=order_data, headers=self.HEADERS)
        response.raise_for_status()
        return response.json()

    async def _stream_prices(self, symbols: List[str] = None):
        """
        Streams live prices from Alpaca's WebSocket API, calling registered callbacks with price updates.
        :param symbols: List of symbols to subscribe to (e.g., ["AAPL", "TSLA"]). Defaults to all instruments.

		@todo:  Reconcile this with the Venue class. This local code should be cut down to separate commonalities and differences.
        """
        if symbols is None:
            symbols = [i['symbol'] for i in self.instruments]  # Default to all tradable symbols

        async with websockets.connect(self.stream_url) as websocket:
            # Authenticate
            auth_message = {
                "action": "auth",
                "key": self.api_key,
				"secret": self.secret_key
            }
            await websocket.send(json.dumps(auth_message))

            # Keep reading until authentication is explicitly confirmed
            authenticated = False
            while not authenticated:
                auth_response = await websocket.recv()
                auth_data = json.loads(auth_response)

                for msg in auth_data:
                    if msg.get("T") == "success":
                        if msg.get("msg") == "authenticated":
                            logger.info("Authenticated successfully.")
                            authenticated = True
                        elif msg.get("msg") == "connected":
                            logger.debug("Connection established, awaiting authentication confirmation...")
                        elif msg.get("T") == "error":
                            logger.error(f"Authentication error: {msg}")
                            return
            # Subscribe after authentication succeeds
            subscribe_message = {
                "action": "subscribe",
                "quotes": symbols
            }
            await websocket.send(json.dumps(subscribe_message))

            subscription_response = await websocket.recv()
            logger.debug(f"Subscription response: {subscription_response}")

            # Stream price updates
            while True:
                try:
                    message = await websocket.recv()
                    data = json.loads(message)
                    for msg in data:
                        if msg.get("T") == "q":  # Quote message
                            price_update = {
                                "symbol": msg["S"],
                                "bid": float(msg["bp"]),  # Bid price
                                "ask": float(msg["ap"])   # Ask price
                            }
                            for callback in self.callbacks:
                                callback(price_update)
                        elif msg.get("T") == "error":
                            logger.error(f"Stream error: {msg}")
                            break
                except websockets.ConnectionClosed as e:
                    logger.info(f"WebSocket connection closed: {e}")
                    break
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    break

    async def start_stream(self, symbols: List[str] = None):
        """Start the WebSocket streaming for the specified symbols."""
        await self._stream_prices(symbols)