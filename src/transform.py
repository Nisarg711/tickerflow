import glob
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

RAW_DIR = "/app/raw"
PROCESSED_DIR = "/app/processed"

def get_spark():
    return (
        SparkSession.builder.appName("TickerFlowTransform")
        .master("local[*]") #local[*] means "use all available CPU cores on this machine as if they were a mini cluster."
        .getOrCreate()
    )

def load_raw(spark):
    """Read all raw CSVs written by extract.py into a single Spark DataFrame."""
    csv_files = glob.glob(os.path.join(RAW_DIR, "*.csv"))
    if not csv_files:
        raise FileNotFoundError(
            f"No CSV files found in {RAW_DIR}. Run extract.py first."
        )
    print(f"Loading {len(csv_files)} raw file(s)...")
    df = spark.read.option("header", True).option("inferSchema", True).csv(csv_files)
    return df


def clean_and_validate(df):
    """
    Cast types, drop exact duplicates, and split rows into valid vs
    quarantined based on basic sanity checks.
    """
    df = df.withColumn("Date", F.to_date("Date"))
    df = df.dropDuplicates(["ticker", "Date"])

    is_valid = (
        F.col("Close").isNotNull()
        & (F.col("Close") > 0)
        & F.col("Volume").isNotNull()
        & (F.col("Volume") >= 0)
        & F.col("Date").isNotNull()
    )

    valid_df = df.filter(is_valid)
    quarantine_df = df.filter(~is_valid).withColumn(
        "quarantine_reason", F.lit("failed basic validation: null/negative price or volume")
    )

    return valid_df, quarantine_df


def add_derived_metrics(df):
    """
    Compute daily return, 7-day and 30-day moving averages, and rolling
    volatility (stddev of daily return) per ticker, ordered by date.
    """
    window_by_ticker = Window.partitionBy("ticker").orderBy("Date")
    window_7d = window_by_ticker.rowsBetween(-6, 0)
    window_30d = window_by_ticker.rowsBetween(-29, 0)

    df = df.withColumn("prev_close", F.lag("Close").over(window_by_ticker))
    df = df.withColumn(
        "daily_return",
        F.when(
            F.col("prev_close").isNotNull() & (F.col("prev_close") != 0),
            (F.col("Close") - F.col("prev_close")) / F.col("prev_close"),
        ),
    )
    df = df.withColumn("moving_avg_7d", F.avg("Close").over(window_7d))
    df = df.withColumn("moving_avg_30d", F.avg("Close").over(window_30d))
    df = df.withColumn("volatility_7d", F.stddev("daily_return").over(window_7d))

    return df.drop("prev_close")

def main():
    spark = get_spark()
    raw_df = load_raw(spark)

    valid_df, quarantine_df = clean_and_validate(raw_df)
    enriched_df = add_derived_metrics(valid_df)

    os.makedirs(PROCESSED_DIR, exist_ok=True)
    clean_path = os.path.join(PROCESSED_DIR, "clean")
    quarantine_path = os.path.join(PROCESSED_DIR, "quarantine")

    enriched_df.write.mode("overwrite").parquet(clean_path)
    print(f"Wrote clean data -> {clean_path}")

    quarantine_count = quarantine_df.count()
    if quarantine_count > 0:
        quarantine_df.write.mode("overwrite").parquet(quarantine_path)
        print(f"Wrote {quarantine_count} quarantined row(s) -> {quarantine_path}")
    else:
        print("No rows quarantined.")

    print("\nSample of transformed data:")
    enriched_df.select(
        "ticker", "Date", "Close", "daily_return", "moving_avg_7d", "moving_avg_30d"
    ).orderBy("ticker", "Date").show(10)

    spark.stop() #spark.stop() cleanly shuts down the Spark session — always do this at the end of a script, otherwise Spark can leave background processes running.


if __name__ == "__main__":
    main()