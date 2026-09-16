import os
from datetime import datetime

import pandas as pd
import yfinance as yf

TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA", "META", "JPM"]
PERIOD = "6mo"     # how far back to pull
INTERVAL = "1d"    # daily candles

RAW_DIR = "/app/raw"


def fetch_ticker(ticker: str) -> str:
    """Fetch OHLCV data for one ticker and save as raw CSV. Returns the file path."""
    df = yf.download(ticker, period=PERIOD, interval=INTERVAL, progress=False)
    #yf.download() returns a pandas DataFrame with columns like Open, High, Low, Close, Volume, indexed by date.
    if df.empty:
        print(f"  WARNING: no data returned for {ticker}")
        return ""
        # Recent yfinance versions return MultiIndex columns like (Close, AAPL)
    # even for a single ticker. Flatten to plain column names, otherwise a
    # leftover header-like row leaks into the CSV as fake data.
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df.reset_index()
    '''
    reset_index() turns that date index into a normal Date column 
    (needed later since Spark reads flat columns, not pandas indexes)
    '''
    df["ticker"] = ticker

    os.makedirs(RAW_DIR, exist_ok=True)
    filename = f"{ticker}_{datetime.now().strftime('%Y-%m-%d')}.csv"
    filepath = os.path.join(RAW_DIR, filename)
    df.to_csv(filepath, index=False)
    print(f"  Saved {len(df)} rows -> {filepath}")
    return filepath


def main():
    print(f"Fetching {len(TICKERS)} tickers, period={PERIOD}, interval={INTERVAL}")
    saved_files = []
    for ticker in TICKERS:
        print(f"Fetching {ticker}...")
        path = fetch_ticker(ticker)
        if path:
            saved_files.append(path)

    print(f"\nDone. {len(saved_files)}/{len(TICKERS)} tickers fetched successfully.")


if __name__ == "__main__":
    main()