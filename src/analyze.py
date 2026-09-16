import os

import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()

PG_CONFIG = {
    "host": os.getenv("PG_HOST"),
    "port": os.getenv("PG_PORT"),
    "database": os.getenv("PG_DATABASE"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
    "sslmode": os.getenv("PG_SSLMODE", "require"),
}

# Postgres doesn't know what "sector" means — we're the ones who know these
# tickers, so we tag them here rather than storing it as a fake DB fact.
SECTOR_MAP = {
    "RELIANCE.NS": "Energy/Conglomerate",
    "TCS.NS": "IT",
    "INFY.NS": "IT",
    "HDFCBANK.NS": "Banking",
    "ICICIBANK.NS": "Banking",
    "SBIN.NS": "Banking",
    "BAJFINANCE.NS": "Finance/NBFC",
}

def load_data():
    conn = psycopg2.connect(**PG_CONFIG)
    df = pd.read_sql("SELECT * FROM clean_prices", conn)
    conn.close()
    df["sector"] = df["ticker"].map(SECTOR_MAP).fillna("Other")
    return df

def sector_summary(df):
    print("\n=== Sector Summary ===")
    summary = (
        df.groupby("sector")
        .agg(
            avg_daily_return=("daily_return", "mean"),
            avg_volatility=("volatility_7d", "mean"),
            tickers=("ticker", "nunique"),
        )
        .round(5)
        .sort_values("avg_daily_return", ascending=False)
    )
    print(summary)
    return summary

def correlation_matrix(df):
    print("\n=== Correlation Matrix (daily returns) ===")
    pivoted = df.pivot_table(index="date", columns="ticker", values="daily_return")
    corr = pivoted.corr().round(2)
    print(corr)
    return corr


def run_summary(df):
    print("\n=== Pipeline Run Summary ===")
    latest_date = df["date"].max()
    latest = df[df["date"] == latest_date]

    top_gainer = latest.loc[latest["daily_return"].idxmax()]
    top_loser = latest.loc[latest["daily_return"].idxmin()]
    most_volatile = df.groupby("ticker")["volatility_7d"].mean().idxmax()

    print(f"Date range     : {df['date'].min()} to {df['date'].max()}")
    print(f"Tickers loaded : {df['ticker'].nunique()}")
    print(f"Total rows     : {len(df)}")
    print(f"Latest day ({latest_date}):")
    print(f"  Top gainer   : {top_gainer['ticker']} ({top_gainer['daily_return']:.2%})")
    print(f"  Top loser    : {top_loser['ticker']} ({top_loser['daily_return']:.2%})")
    print(f"Most volatile ticker overall: {most_volatile}")


def main():
    df = load_data()
    sector_summary(df)
    correlation_matrix(df)
    run_summary(df)


if __name__ == "__main__":
    main()