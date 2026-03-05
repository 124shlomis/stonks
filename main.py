import datetime
import glob
import json
import logging
import os
import time
import brotli
import requests
import yfinance as yf
import chromedriver_autoinstaller
from selenium.webdriver.chrome.options import Options
from seleniumwire import webdriver

logging.getLogger("seleniumwire").setLevel(logging.ERROR)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

chromedriver_autoinstaller.install()

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
SYMBOLS_DIR = os.path.join(SCRIPT_DIR, "symbols")
DIST_DIR = os.path.join(SCRIPT_DIR, "dist")


def get_latest_user_agent(operating_system="windows", browser="chrome"):
    url = "https://jnrbsn.github.io/user-agents/user-agents.json"
    r = requests.get(url)
    r.raise_for_status()
    user_agents = r.json()

    for user_agent in user_agents:
        if operating_system.lower() in user_agent.lower() and browser.lower() in user_agent.lower():
            return user_agent

    return None


def get_issa_rest_api_response(request):
    if 400 <= request.response.status_code < 600:
        raise Exception(f"Status code {request.response.status_code}")

    response = brotli.decompress(request.response.body)
    response = response.decode("utf-8")
    response = json.loads(response)
    return response


def fetch_issa_price(symbol_track_info, symbol, max_attempts=5):
    """
    Attempt to fetch the price for an ISSA symbol using Selenium.
    Returns (price, price_date) or raises an exception if all attempts fail.
    Always ensures the driver is properly closed after each attempt.
    """
    symbol_price = 0
    symbol_price_date = ""

    for attempt in range(1, max_attempts + 1):
        driver = None
        try:
            logging.info(f"ISSA fetch attempt {attempt}/{max_attempts} for symbol {symbol}")

            options = Options()
            options.add_argument("--headless=new")
            options.add_argument("--window-size=1920,980")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            driver = webdriver.Chrome(options=options)

            if symbol_track_info["type"] == "etf":
                url = f"https://market.tase.co.il/he/market_data/security/{symbol}"
            else:
                url = f"https://maya.tase.co.il/he/funds/mutual-funds/{symbol}"

            driver.get(url)

            # Poll for the expected API requests rather than a flat sleep,
            # checking every 5 seconds up to (15 * attempt) seconds total.
            wait_seconds = 15 * attempt
            poll_interval = 5
            elapsed = 0
            while elapsed < wait_seconds:
                time.sleep(poll_interval)
                elapsed += poll_interval

                for request in driver.requests:
                    if not request.response:
                        continue

                    if request.url.startswith("https://api.tase.co.il/api/company/securitydata"):
                        try:
                            response = get_issa_rest_api_response(request)
                            symbol_price = response["LastRate"] / 100  # ILA -> ILS
                            symbol_price_date = datetime.datetime.strptime(
                                response["TradeDate"], "%d/%m/%Y"
                            ).strftime("%Y-%m-%d")
                        except Exception as e:
                            logging.warning(f"Failed to parse securitydata response: {e}")

                    if request.url.startswith("https://maya.tase.co.il/api/v1/funds/mutual"):
                        try:
                            response = get_issa_rest_api_response(request)
                            symbol_price = response["purchasePrice"] / 100  # ILA -> ILS
                            symbol_price_date = datetime.datetime.strptime(
                                response["ratesAsOf"], "%Y-%m-%d"
                            ).strftime("%Y-%m-%d")
                        except Exception as e:
                            logging.warning(f"Failed to parse mutual funds response: {e}")

                if symbol_price:
                    logging.info(f"Got price on attempt {attempt} after {elapsed}s")
                    break

        except Exception as e:
            logging.warning(f"Attempt {attempt} failed with error: {e}")
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass

        if symbol_price:
            break

        if attempt < max_attempts:
            backoff = 10 * attempt
            logging.info(f"No price captured, waiting {backoff}s before next attempt...")
            time.sleep(backoff)

    return symbol_price, symbol_price_date


def main():
    logging.info(f"reading symbols *.json files in {SYMBOLS_DIR} ...")
    for symbol_track_file_path in glob.glob(os.path.join(SYMBOLS_DIR, "*.json"), recursive=True):
        logging.info(f"processing {symbol_track_file_path} ...")

        try:
            with open(symbol_track_file_path) as f:
                symbol_track_info = json.load(f)

            symbol_price = 0
            symbol_price_date = ""

            symbol_id = symbol_track_info["id"]
            symbol = symbol_track_info["symbol"]
            currency = symbol_track_info["currency"]
            user_agent_header = get_latest_user_agent(operating_system="windows", browser="chrome")

            if symbol_track_info["source"] == "justetf":
                url = f"https://www.justetf.com/api/etfs/{symbol}/quote?locale=en&currency={currency}&isin={symbol}"
                r = requests.get(url, headers={"User-Agent": user_agent_header, "Accept": "application/json"})
                r.raise_for_status()
                symbol_info = r.json()
                symbol_price = symbol_info["latestQuote"]["raw"]
                symbol_price_date = symbol_info["latestQuoteDate"]

            elif symbol_track_info["source"] == "yahoo_finance":
                ticker_yahoo = yf.Ticker(symbol)
                symbol_info = ticker_yahoo.history()
                symbol_price = symbol_info["Close"].iloc[-1]
                symbol_price_date = symbol_info["Close"].index[-1]
                symbol_price_date = datetime.datetime.strftime(symbol_price_date, "%Y-%m-%d")

            elif symbol_track_info["source"] == "issa":
                symbol_price, symbol_price_date = fetch_issa_price(symbol_track_info, symbol)

            if not symbol_price:
                raise Exception(f"Failed to get price for {symbol}")

            symbol_dist_dir = os.path.join(DIST_DIR, symbol_id)
            os.makedirs(symbol_dist_dir, exist_ok=True)
            symbol_track_info["price"] = symbol_price
            symbol_track_info["price_date"] = symbol_price_date

            with open(os.path.join(symbol_dist_dir, "price"), "w+") as f:
                f.write(str(symbol_price))

            with open(os.path.join(symbol_dist_dir, "currency"), "w+") as f:
                f.write(currency)

            with open(os.path.join(symbol_dist_dir, "date"), "w+") as f:
                f.write(symbol_price_date)

            with open(os.path.join(symbol_dist_dir, "info.json"), "w+") as f:
                json.dump(symbol_track_info, f)

            logging.info(f'symbol "{symbol_id}" update completed. price: {symbol_price} {currency} date: {symbol_price_date}')

        except Exception as e:
            logging.exception(f"Failed to process {symbol_track_file_path}")
            raise


if __name__ == "__main__":
    main()
