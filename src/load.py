import os

import psycopg2
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

load_dotenv()

PROCESSED_DIR = "/app/processed"

PG_CONFIG = {
    "host": os.getenv("PG_HOST"),
    "port": os.getenv("PG_PORT"),
    "database": os.getenv("PG_DATABASE"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
    "sslmode": os.getenv("PG_SSLMODE", "require"),
}
'''
Spark needs the actual PostgreSQL JDBC driver (a Java library) to talk to Postgres — 
psycopg2 doesn't help Spark here, that's a separate Python-only driver. spark.jars.packages 
tells Spark to fetch that driver automatically from Maven the first time this runs 
(needs internet access, which your container has).
'''

JDBC_URL = f"jdbc:postgresql://{PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['database']}?sslmode={PG_CONFIG['sslmode']}"
JDBC_PROPERTIES = {
    "user": PG_CONFIG["user"],
    "password": PG_CONFIG["password"],
    "driver": "org.postgresql.Driver",
}

def get_spark():
    return (
        SparkSession.builder.appName("TickerFlowLoad")
        .master("local[*]")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )

def load_processed_data(spark):
    clean_df = spark.read.parquet(os.path.join(PROCESSED_DIR, "clean"))
    clean_df = clean_df.select([F.col(c).alias(c.lower()) for c in clean_df.columns])
#The .alias(c.lower()) loop just lowercases every column name (Date → date, Close → close, etc.)
    quarantine_path = os.path.join(PROCESSED_DIR, "quarantine")
    if os.path.exists(quarantine_path):
        quarantine_df = spark.read.parquet(quarantine_path)
        quarantine_df = quarantine_df.select([F.col(c).alias(c.lower()) for c in quarantine_df.columns])
    else:
        quarantine_df = None

    return clean_df, quarantine_df

def write_staging(clean_df):
    (
        clean_df.write.mode("overwrite")
        .jdbc(url=JDBC_URL, table="staging_prices", properties=JDBC_PROPERTIES)
    )
    print(f"Wrote {clean_df.count()} row(s) to staging_prices")

def upsert_from_staging():
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS clean_prices (
            ticker TEXT,
            date DATE,
            open DOUBLE PRECISION,
            high DOUBLE PRECISION,
            low DOUBLE PRECISION,
            close DOUBLE PRECISION,
            volume BIGINT,
            daily_return DOUBLE PRECISION,
            moving_avg_7d DOUBLE PRECISION,
            moving_avg_30d DOUBLE PRECISION,
            volatility_7d DOUBLE PRECISION,
            PRIMARY KEY (ticker, date)
        );
    """)

    cur.execute("""
        INSERT INTO clean_prices (
            ticker, date, open, high, low, close, volume,
            daily_return, moving_avg_7d, moving_avg_30d, volatility_7d
        )
        SELECT ticker, date, open, high, low, close, volume,
               daily_return, moving_avg_7d, moving_avg_30d, volatility_7d
        FROM staging_prices
        ON CONFLICT (ticker, date) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            daily_return = EXCLUDED.daily_return,
            moving_avg_7d = EXCLUDED.moving_avg_7d,
            moving_avg_30d = EXCLUDED.moving_avg_30d,
            volatility_7d = EXCLUDED.volatility_7d;
    """)

    cur.execute("TRUNCATE TABLE staging_prices;")
    conn.commit()
    cur.close()
    conn.close()
    print("Upserted staging_prices -> clean_prices, staging table cleared")

def write_quarantine(quarantine_df):
    if quarantine_df is None:
        print("No quarantine data to write.")
        return
    (
        quarantine_df.write.mode("append")
        .jdbc(url=JDBC_URL, table="data_quality_log", properties=JDBC_PROPERTIES)
    )
    print(f"Appended {quarantine_df.count()} row(s) to data_quality_log")

def main():
    spark = get_spark()
    clean_df, quarantine_df = load_processed_data(spark)

    write_staging(clean_df)
    upsert_from_staging()
    write_quarantine(quarantine_df)

    spark.stop()


if __name__ == "__main__":
    main()