import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    LongType,
    DoubleType,
    StringType,
    BooleanType,
)


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"


# ============================================================
# SPARK SESSION
# ============================================================

def create_spark_session():
    """
    Create the single Spark session used by the CLICKINSIGHT
    data pipeline.
    """

    spark = (
        SparkSession.builder
        .appName("CLICKINSIGHT")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ============================================================
# SCHEMAS
# ============================================================

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("country", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("signup_date", StringType(), True),
    StructField("marketing_opt_in", BooleanType(), True),
])


SESSIONS_SCHEMA = StructType([
    StructField("session_id", LongType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("start_time", StringType(), True),
    StructField("device", StringType(), True),
    StructField("source", StringType(), True),
    StructField("country", StringType(), True),
])


EVENTS_SCHEMA = StructType([
    StructField("event_id", LongType(), True),
    StructField("session_id", LongType(), True),
    StructField("timestamp", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("qty", IntegerType(), True),
    StructField("cart_size", IntegerType(), True),
    StructField("payment", StringType(), True),
    StructField("discount_pct", DoubleType(), True),
    StructField("amount_usd", DoubleType(), True),
])


ORDERS_SCHEMA = StructType([
    StructField("order_id", LongType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_time", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("discount_pct", DoubleType(), True),
    StructField("subtotal_usd", DoubleType(), True),
    StructField("total_usd", DoubleType(), True),
    StructField("country", StringType(), True),
    StructField("device", StringType(), True),
    StructField("source", StringType(), True),
])


ORDER_ITEMS_SCHEMA = StructType([
    StructField("order_id", LongType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("unit_price_usd", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("line_total_usd", DoubleType(), True),
])


PRODUCTS_SCHEMA = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("category", StringType(), True),
    StructField("name", StringType(), True),
    StructField("price_usd", DoubleType(), True),
    StructField("cost_usd", DoubleType(), True),
    StructField("margin_usd", DoubleType(), True),
])


REVIEWS_SCHEMA = StructType([
    StructField("review_id", LongType(), True),
    StructField("order_id", LongType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("review_text", StringType(), True),
    StructField("review_time", StringType(), True),
])


# ============================================================
# GENERAL CLEANING
# ============================================================

def clean_string_columns(df):
    """
    Trim leading/trailing whitespace from every string column.
    """

    for field in df.schema.fields:

        if isinstance(field.dataType, StringType):
            df = df.withColumn(
                field.name,
                F.trim(F.col(field.name))
            )

    return df


# ============================================================
# DATASET-SPECIFIC PREPROCESSING
# ============================================================

def preprocess_customers(df):

    df = clean_string_columns(df)

    df = df.withColumn(
        "signup_date",
        F.to_date("signup_date")
    )

    df = df.dropDuplicates()

    return df


def preprocess_sessions(df):

    df = clean_string_columns(df)

    df = df.withColumn(
        "start_time",
        F.to_timestamp("start_time")
    )

    # session_id identifies a session.
    df = df.dropDuplicates(["session_id"])

    return df


def preprocess_events(df):

    df = clean_string_columns(df)

    df = df.withColumn(
        "timestamp",
        F.to_timestamp("timestamp")
    )

    # event_id identifies an event.
    df = df.dropDuplicates(["event_id"])

    return df


def preprocess_orders(df):

    df = clean_string_columns(df)

    df = df.withColumn(
        "order_time",
        F.to_timestamp("order_time")
    )

    # order_id identifies an order.
    df = df.dropDuplicates(["order_id"])

    return df


def preprocess_order_items(df):

    df = clean_string_columns(df)

    # IMPORTANT:
    # Do not remove the 73 exact duplicate rows automatically.
    # They are preserved intentionally based on our dataset audit.

    return df


def preprocess_products(df):

    df = clean_string_columns(df)

    df = df.dropDuplicates(["product_id"])

    return df


def preprocess_reviews(df):

    df = clean_string_columns(df)

    df = df.withColumn(
        "review_time",
        F.to_timestamp("review_time")
    )

    df = df.dropDuplicates(["review_id"])

    return df


# ============================================================
# LOAD RAW DATA
# ============================================================

def load_raw_datasets(spark):
    """
    Load all seven raw datasets using explicit schemas.
    """

    datasets = {}

    datasets["customers"] = (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(CUSTOMERS_SCHEMA)
        .csv(str(RAW_DIR / "customers.csv"))
    )

    datasets["sessions"] = (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(SESSIONS_SCHEMA)
        .csv(str(RAW_DIR / "sessions.csv"))
    )

    datasets["events"] = (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(EVENTS_SCHEMA)
        .csv(str(RAW_DIR / "events.csv"))
    )

    datasets["orders"] = (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(ORDERS_SCHEMA)
        .csv(str(RAW_DIR / "orders.csv"))
    )

    datasets["order_items"] = (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(ORDER_ITEMS_SCHEMA)
        .csv(str(RAW_DIR / "order_items.csv"))
    )

    datasets["products"] = (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(PRODUCTS_SCHEMA)
        .csv(str(RAW_DIR / "products.csv"))
    )

    datasets["reviews"] = (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(REVIEWS_SCHEMA)
        .csv(str(RAW_DIR / "reviews.csv"))
    )

    return datasets


# ============================================================
# PREPROCESS ALL DATASETS
# ============================================================

def preprocess_all_datasets(datasets):
    """
    Apply all preprocessing rules.

    Returns:
        Dictionary containing seven processed Spark DataFrames.
    """

    processed = {}

    processed["customers"] = preprocess_customers(
        datasets["customers"]
    )

    processed["sessions"] = preprocess_sessions(
        datasets["sessions"]
    )

    processed["events"] = preprocess_events(
        datasets["events"]
    )

    processed["orders"] = preprocess_orders(
        datasets["orders"]
    )

    processed["order_items"] = preprocess_order_items(
        datasets["order_items"]
    )

    processed["products"] = preprocess_products(
        datasets["products"]
    )

    processed["reviews"] = preprocess_reviews(
        datasets["reviews"]
    )

    return processed


# ============================================================
# VALIDATION
# ============================================================

def validate_preprocessed_data(processed):
    """
    Basic preprocessing validation.
    """

    expected_datasets = {
        "customers",
        "sessions",
        "events",
        "orders",
        "order_items",
        "products",
        "reviews",
    }

    actual_datasets = set(processed.keys())

    if actual_datasets != expected_datasets:
        raise ValueError(
            "Preprocessing output does not contain all seven datasets."
        )

    # Required keys must not be missing.
    required_columns = {
        "customers": ["customer_id"],
        "sessions": ["session_id", "customer_id"],
        "events": [
            "event_id",
            "session_id",
            "timestamp",
            "event_type",
        ],
        "orders": ["order_id", "customer_id", "order_time"],
        "order_items": ["order_id", "product_id"],
        "products": ["product_id"],
        "reviews": ["review_id", "order_id", "product_id"],
    }

    for name, columns in required_columns.items():

        actual_columns = set(processed[name].columns)

        missing = set(columns) - actual_columns

        if missing:
            raise ValueError(
                f"{name} is missing required columns: {missing}"
            )

    return True


# ============================================================
# STANDALONE TEST
# ============================================================

def main():

    print()
    print("=" * 70)
    print("CLICKINSIGHT - PREPROCESSING")
    print("=" * 70)

    spark = create_spark_session()

    try:

        print("\n[1/3] Loading raw datasets...")

        raw_datasets = load_raw_datasets(spark)

        print("All 7 datasets loaded.")

        print("\n[2/3] Preprocessing...")

        processed = preprocess_all_datasets(
            raw_datasets
        )

        print("Preprocessing completed.")

        print("\n[3/3] Validating preprocessing output...")

        validate_preprocessed_data(processed)

        for name, df in processed.items():
            print(
                f"{name:<15} "
                f"rows={df.count():>8} "
                f"columns={len(df.columns):>2}"
            )

        print()
        print("=" * 70)
        print("PREPROCESSING PASSED")
        print("=" * 70)

    finally:

        spark.stop()


if __name__ == "__main__":
    main()