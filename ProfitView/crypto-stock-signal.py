import datetime

from profitview import Link, logger, http
import os
import asyncio
import threading
import requests
import io
import csv
import copy
from my.venues import Woo, Alpaca
from dotenv import load_dotenv
import xml.etree.ElementTree as ET
import pandas as pd
from bs4 import BeautifulSoup
import talib as ta
import numpy as np

load_dotenv()


def get_ishares_data(portfolio_id: str):
    """
    Pull the latest IBIT holdings data from the iShares website, extract the total BTC held and shares outstanding, and return them as a list.
    """

    as_of_date = datetime.datetime.now().strftime("%Y%m%d")
    target_date = pd.to_datetime(as_of_date)  # Monday

    # Subtract 1 Business Day
    last_business_day = target_date - pd.tseries.offsets.BDay(4)
    last_business_day = last_business_day.strftime('%Y%m%d')
    logger.info(f"Fetching iShares holdings {portfolio_id=} as_of={last_business_day}")

    # Rebuild the direct API download link using the portfolio ID
    csv_url = (
        f"https://www.blackrock.com/varnish-api/blk-one01-product-data/product-data/api/v1/get-fund-document"
        f"?appType=PRODUCT_PAGE&appSubType=ONE&targetSite=one&locale=en_US"
        f"&portfolioId={portfolio_id}&userType=individual&asOfDate={last_business_day}&component=holdings"
    )

    # Download the data directly
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    response = requests.get(csv_url, headers=headers)
    logger.info(f"iShares holdings response status={response.status_code} bytes={len(response.content)}")
    # The CSV file is in two sections - the "preamble" and main data
    lines_in_preamble = 8

    preamble = response.text.splitlines()[:lines_in_preamble]
    shares_outstanding_string = ""
    for line in csv.reader(preamble):
        if line and "Shares Outstanding" in line[0]:
            shares_outstanding_string = line[1].strip('"')
            break

    shares_outstanding = int(float(shares_outstanding_string.replace(",", "")))

    df = pd.read_csv(io.StringIO(response.text), skiprows=lines_in_preamble + 1)

    # Extract BTC Held from Units column
    btc_row = df[df['Ticker'] == 'BTC']
    btc_held = 0.0
    if not btc_row.empty:
        btc_held_string = btc_row['Units'].values[0]
        btc_held = float(btc_held_string.replace(",", ""))
    else:
        logger.warning("iShares holdings CSV has no BTC row")

    logger.info(f"{btc_held=}, {shares_outstanding=}")

    return [btc_held, shares_outstanding]

def implied_btc(share_price, total_btc_held, total_shares_outstanding):
    if not total_btc_held:
        return None
    return share_price*total_shares_outstanding / total_btc_held


class Signals(Link):
    ALPACA_API_KEY=os.getenv('ALPACA_API_KEY')
    ALPACA_API_SECRET=os.getenv('ALPACA_API_SECRET')
    ALPACA_PAPER_API_KEY=os.getenv('ALPACA_PAPER_API_KEY')
    ALPACA_PAPER_API_SECRET=os.getenv('ALPACA_PAPER_API_SECRET')
    ALPACA_DATA_ENDPOINT = 'https://data.alpaca.markets'
    ALPACA_STREAM_URL = 'wss://stream.data.alpaca.markets/v2/iex'
    
    PORTFOLIO_ID = '333011'
    PRODUCT_SYMBOL = 'IBIT'
    CRYPTO_SRC = 'woo'
    CRYPTO_SYMBOL = 'PERP_BTC_USDT'
    
    DIFFERENCES_SIZE = 50

    def __init__(self):
        self.running = False
        self.stock_venue = None
        self.crypto_venue = None
        self.venues_ready = False
        self._ignored_quotes = set()
        self._crypto_quotes_since_ibit = 0
        
        try:
            self.ibit_quote_changed = False
            super().__init__()
            self.venue_setup()
            self.venues_ready = True
            # Register callback after venues are set up
            self.stock_venue.add_callback(self.on_ibit_price_update)
            self.ibit_btc, self.ibit_shares = get_ishares_data(self.PORTFOLIO_ID)
            if not self.ibit_btc:
                logger.error(f"IBIT BTC holdings are {self.ibit_btc}; implied price is unavailable")
            self.ibit_quote = self.previous_ibit_quote = {}
            self.ibit_quote_changed = False
            self.differences = {'bid': [], 'ask': []}
            # Start stream in background after initialization
            self.schedule_stream(self.PRODUCT_SYMBOL)
            logger.info(
                f"Watching {self.CRYPTO_SRC} {self.CRYPTO_SYMBOL} against {self.PRODUCT_SYMBOL}; "
                f"holdings btc={self.ibit_btc} shares={self.ibit_shares}; "
                f"window={self.DIFFERENCES_SIZE}"
            )
            logger.info("Initialization complete; venues ready")
        except Exception as e:
            logger.error(f"Exception in __init__ during venue setup: {e}")
            self.venues_ready = False

    def venue_setup(self):
        try:
            self.stock_venue = Alpaca(
                self, self.ALPACA_PAPER_API_KEY, self.ALPACA_PAPER_API_SECRET,
                data_endpoint=self.ALPACA_DATA_ENDPOINT, stream_url=self.ALPACA_STREAM_URL)
            self.crypto_venue = Woo(self, is_signal=True)
            logger.info("Venues set up successfully")
            return True
        except Exception as ex:
            logger.error(f"Couldn't set up venues. Exception: {ex}")
            self.stock_venue = None
            self.crypto_venue = None
            raise

    def schedule_stream(self, symbol):
        """Schedule the WebSocket stream to start in the background."""
        if not self.venues_ready or not self.stock_venue:
            logger.warning("Venues not ready; cannot schedule stream")
            return
        try:
            logger.info(f"Scheduling {symbol} stream in a new thread")
            threading.Thread(target=lambda: asyncio.run(self.start_stream(symbols=[symbol])), daemon=True).start()
        except Exception as e:
            logger.error(f"Failed to schedule stream: {e}")

    def on_start(self):
        """Synchronous start method; logs startup."""
        logger.info("Starting algo")
        if not self.venues_ready:
            logger.error("Venues not ready yet; stream scheduled in __init__")

    async def start_stream(self, symbols=["IBIT"]):
        """Asynchronous method to start the WebSocket stream."""
        if self.running:
            logger.info("Stream already running")
            return
        self.running = True
        logger.info(f"Starting stream for symbols: {symbols}")
        try:
            await self.stock_venue.start_stream(symbols)
        except Exception as e:
            logger.error(f"Stream error: {e}")
        finally:
            self.running = False
            logger.info("Stream stopped")

    def on_ibit_price_update(self, price_data):
        """Handle WebSocket price updates."""
        self.ibit_quote = { 'bid': price_data["bid"], 'ask': price_data["ask"] }
        if self.ibit_quote != self.previous_ibit_quote:
            self.ibit_quote_changed = True
            self.previous_ibit_quote = copy.deepcopy(self.ibit_quote)
            logger.info(f"IBIT quote bid={self.ibit_quote['bid']} ask={self.ibit_quote['ask']}")

    def quote_update(self, src, sym, data):
        if src != self.CRYPTO_SRC or sym != self.CRYPTO_SYMBOL:
            key = (src, sym)
            if key not in self._ignored_quotes:
                self._ignored_quotes.add(key)
                logger.info(
                    f"Ignoring quotes for {src=} {sym=}; "
                    f"watching {self.CRYPTO_SRC} {self.CRYPTO_SYMBOL}"
                )
            return
        if not self.ibit_quote_changed:
            self._crypto_quotes_since_ibit += 1
            if self._crypto_quotes_since_ibit in (1, 10) or self._crypto_quotes_since_ibit % 100 == 0:
                buffer = len(self.differences['bid']) if hasattr(self, 'differences') else 0
                logger.info(
                    f"Crypto quote received but IBIT unchanged "
                    f"({self._crypto_quotes_since_ibit} since last IBIT change); "
                    f"buffer={buffer}/{self.DIFFERENCES_SIZE}"
                )
            return
        self._crypto_quotes_since_ibit = 0

        bid = data['bid'][0]
        ask = data['ask'][0]

        # Get IBIT quotes with default handling
        ibit_bid = self.ibit_quote.get('bid')
        ibit_ask = self.ibit_quote.get('ask')

        if ibit_bid is None or ibit_ask is None:
            logger.warning(f"IBIT quote incomplete; skipping update {self.ibit_quote=}")
            return
        
        # Calculate implied prices
        implied_ibit_bid = implied_btc(ibit_bid, self.ibit_btc, self.ibit_shares)
        implied_ibit_ask = implied_btc(ibit_ask, self.ibit_btc, self.ibit_shares)
        if implied_ibit_bid is None or implied_ibit_ask is None:
            logger.warning(f"Skipping quote; cannot imply BTC price from {self.ibit_btc=}")
            self.ibit_quote_changed = False
            return

        # Calculate differences (premium/discount)
        bid_difference = bid - implied_ibit_bid  # Positive means futures premium
        ask_difference = implied_ibit_ask - ask  # Positive means futures discount
        
        # Store absolute differences for analysis
        self.differences['bid'].append(float(abs(bid_difference)))
        self.differences['ask'].append(float(abs(ask_difference)))
        sample_count = len(self.differences['bid'])
        logger.info(
            f"Sample {sample_count}/{self.DIFFERENCES_SIZE}: "
            f"perp bid={bid} ask={ask}, "
            f"implied bid={implied_ibit_bid:,.2f} ask={implied_ibit_ask:,.2f}, "
            f"bid_diff={bid_difference:,.2f} ask_diff={ask_difference:,.2f}"
        )

        if sample_count > self.DIFFERENCES_SIZE: 
            self.differences['bid'] = self.differences['bid'][1:]
            self.differences['ask'] = self.differences['ask'][1:]

            # Calculate statistics
            bid_diffs = np.array(self.differences['bid'], dtype=np.float64)
            ask_diffs = np.array(self.differences['ask'], dtype=np.float64)

            bid_mean = ta.SMA(bid_diffs, timeperiod=self.DIFFERENCES_SIZE)[-1]
            ask_mean = ta.SMA(ask_diffs, timeperiod=self.DIFFERENCES_SIZE)[-1]
            bid_std = ta.STDDEV(bid_diffs, timeperiod=self.DIFFERENCES_SIZE)[-1]
            ask_std = ta.STDDEV(ask_diffs, timeperiod=self.DIFFERENCES_SIZE)[-1]

            # Calculate z-scores for current differences
            bid_zscore = (bid_difference - bid_mean) / bid_std if bid_std > 0 else 0
            ask_zscore = (ask_difference - ask_mean) / ask_std if ask_std > 0 else 0
            
            # More aggressive minimum z-score
            MIN_ZSCORE = 0.5
            
            # Modified scaling function to be more aggressive at lower z-scores
            def calculate_size(zscore):
                if abs(zscore) < MIN_ZSCORE:
                    return None
                # Steeper scaling for smaller z-scores
                return np.tanh((abs(zscore) - MIN_ZSCORE) * 1.5)

            logger.info(
                f"Window ready: {bid_mean=:.2f} {bid_std=:.2f} {bid_zscore=:.3f}; "
                f"{ask_mean=:.2f} {ask_std=:.2f} {ask_zscore=:.3f}; min_zscore={MIN_ZSCORE}"
            )

            # Trading signals based on z-scores
            if bid_zscore > MIN_ZSCORE:  # Premium
                size = calculate_size(bid_zscore)
                if size is not None:
                    size = -size  # Negative for selling
                    logger.info("BTC Perp is trading at premium")
                    logger.info(f"{bid_mean=}, {bid_std=}, {bid_difference=}, {bid_zscore=}, {size=}")
                    logger.info(f"Sending signal size={size} for {self.CRYPTO_SRC} {self.CRYPTO_SYMBOL}")
                    self.signal(self.CRYPTO_SRC, self.CRYPTO_SYMBOL, size=size)
                else:
                    logger.info(f"Premium z-score {bid_zscore:.3f} produced no size")
            elif ask_zscore > MIN_ZSCORE:  # Discount
                size = calculate_size(ask_zscore)
                if size is not None:
                    logger.info("BTC Perp is trading at discount")
                    logger.info(f"{ask_mean=}, {ask_std=}, {ask_difference=}, {ask_zscore=}, {size=}")
                    logger.info(f"Sending signal size={size} for {self.CRYPTO_SRC} {self.CRYPTO_SYMBOL}")
                    self.signal(self.CRYPTO_SRC, self.CRYPTO_SYMBOL, size=size)
                else:
                    logger.info(f"Discount z-score {ask_zscore:.3f} produced no size")
            else:
                logger.info(
                    f"No signal: bid_zscore={bid_zscore:.3f} ask_zscore={ask_zscore:.3f} "
                    f"both at or below {MIN_ZSCORE}"
                )
                self.signal(self.CRYPTO_SRC, self.CRYPTO_SYMBOL, size=None)

            logger.info(f"Differences (Premium/Discount) - Ask: ${ask_difference:,.2f} USD; Bid: ${bid_difference:,.2f}")
        else:
            logger.info(f"Filling difference window {sample_count}/{self.DIFFERENCES_SIZE}; no signal yet")

        self.ibit_quote_changed = False

    @http.route
    def get_start_stream(self, data):
        """HTTP endpoint to manually start streaming."""
        logger.info("Received request to start stream via HTTP")
        self.schedule_stream()
        return {"status": "Stream start requested"}

    @http.route
    def get_ibit_btc(self, data):
        return get_ibit_btc_holdings()

    @http.route
    def get_ibit_shares(self, data):
        return get_ishares_data(self.PORTFOLIO_ID)

    @http.route
    def get_prices(self, data):

        ibit_share_price = self.stock_venue.mark_price(data['stock'])  # IBIT share price in USD
        current_btc_price = self.crypto_venue.mark_price(data.get('crypto') or self.CRYPTO_SYMBOL)

        # Calculate the implied Bitcoin price
        implied_price = implied_btc(ibit_share_price, self.ibit_btc, self.ibit_shares)
        if implied_price is None:
            logger.error(f"Cannot imply BTC price from {self.ibit_btc=}")
            return {"error": "IBIT BTC holdings are zero"}

        # Print the result
        logger.info(f"IBIT Share Price: ${ibit_share_price:.2f}")
        btc_per_share = self.ibit_btc / self.ibit_shares if self.ibit_shares else None
        if btc_per_share is None:
            logger.warning(f"Shares outstanding are {self.ibit_shares}; skipping BTC-per-share ratio")
        else:
            logger.info(f"Bitcoin per Share Ratio: {btc_per_share:.7f} BTC/share")
        logger.info(f"Implied Bitcoin Price: ${implied_price:,.2f} USD/BTC")

        # Optional: Compare with a known BTC price for sanity check (e.g., Feb 25 estimate)

        logger.info(f"Estimated Market Bitcoin Price: ${current_btc_price:,.2f} USD/BTC")
        logger.info(f"Difference (Premium/Discount): ${(implied_price - current_btc_price):,.2f} USD/BTC")

        return data

    @http.route
    def post_route(self, data):
        """Definition of POST request endpoint - see docs for more info"""
        return data
