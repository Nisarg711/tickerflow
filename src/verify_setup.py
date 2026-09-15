"""
Run this once after setup to confirm Spark and Postgres are both reachable
BEFORE writing any real pipeline code. Saves you from debugging two things
at once later.
"""
import os
from dotenv import load_dotenv

load_dotenv()


def check_spark():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("SetupCheck").master("local[*]").getOrCreate()
    df = spark.createDataFrame([(1, "AAPL"), (2, "MSFT")], ["id", "ticker"])
    print("Spark OK — sample DataFrame:")
    df.show()
    spark.stop()


def check_postgres():
    import psycopg2

    conn = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        sslmode=os.getenv("PG_SSLMODE", "require"),
    )
    cur = conn.cursor()
    cur.execute("SELECT version();")
    print("Postgres OK — version:", cur.fetchone()[0])
    cur.close()
    conn.close()


if __name__ == "__main__":
    print("Checking Spark...")
    check_spark()
    print("\nChecking Postgres...")
    check_postgres()
