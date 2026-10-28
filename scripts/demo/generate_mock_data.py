import argparse
import datetime
import random
import uuid
from decimal import Decimal

from ecommerce_lakehouse.config import qualified_name


def generate_orders(spark, generator, data_date, seed, count=200):
    rows = []
    statuses = ["paid", "shipped", "delivered", "refunded"]
    for index in range(count):
        rows.append({
            "order_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"orders:{seed}:{data_date}:{index}")),
            "user_id": f"user_{generator.randint(1, 50)}",
            "product_id": f"prod_{generator.randint(1, 30)}",
            "quantity": generator.randint(1, 5),
            "unit_price": Decimal(generator.randint(2000, 80000)) / 100,
            "currency": "CNY",
            "status": generator.choice(statuses),
            "created_at": datetime.datetime.combine(
                data_date - datetime.timedelta(days=generator.randint(0, 25)),
                datetime.time(generator.randint(0, 23), generator.randint(0, 59)),
            ),
        })
    return spark.createDataFrame(rows, schema=(
        "order_id STRING, user_id STRING, product_id STRING, quantity BIGINT, "
        "unit_price DECIMAL(18,2), currency STRING, status STRING, created_at TIMESTAMP"
    ))


def generate_users(spark, generator, count=50):
    rows = []
    cities = ["Shanghai", "Beijing", "Shenzhen", "Hangzhou", "Chengdu"]
    for index in range(1, count + 1):
        rows.append({
            "user_id": f"user_{index}",
            "user_name": f"user_{index}",
            "city": generator.choice(cities),
            "member_level": generator.choice(["normal", "silver", "gold"]),
            "created_at": datetime.datetime(2025, generator.randint(1, 12), generator.randint(1, 28)),
        })
    return spark.createDataFrame(rows, schema=(
        "user_id STRING, user_name STRING, city STRING, member_level STRING, created_at TIMESTAMP"
    ))


def generate_products(spark, generator, count=30):
    rows = []
    categories = ["electronics", "clothing", "home", "beauty", "sports"]
    for index in range(1, count + 1):
        rows.append({
            "product_id": f"prod_{index}",
            "product_name": f"product_{index}",
            "category": generator.choice(categories),
            "brand": f"brand_{generator.randint(1, 6)}",
            "price": Decimal(generator.randint(3000, 100000)) / 100,
            "stock_qty": generator.randint(0, 500),
        })
    return spark.createDataFrame(rows, schema=(
        "product_id STRING, product_name STRING, category STRING, brand STRING, "
        "price DECIMAL(18,2), stock_qty BIGINT"
    ))


def generate_clickstream(spark, generator, data_date, seed, count=500):
    rows = []
    event_types = ["view", "add_to_cart", "purchase", "search"]
    for index in range(count):
        rows.append({
            "event_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"clicks:{seed}:{data_date}:{index}")),
            "user_id": f"user_{generator.randint(1, 50)}",
            "product_id": f"prod_{generator.randint(1, 30)}",
            "event_type": generator.choice(event_types),
            "event_time": datetime.datetime.combine(
                data_date - datetime.timedelta(days=generator.randint(0, 25)),
                datetime.time(generator.randint(0, 23), generator.randint(0, 59)),
            ),
            "platform": generator.choice(["app", "web"]),
        })
    return spark.createDataFrame(rows, schema=(
        "event_id STRING, user_id STRING, product_id STRING, event_type STRING, "
        "event_time TIMESTAMP, platform STRING"
    ))


def generate_demo_data(spark, catalog, schema, reset_demo_data="false", seed=42, data_date=None):
    if not (catalog.endswith("_dev") or schema.endswith("_dev")):
        raise ValueError("Mock data requires a catalog or schema ending in _dev")
    bronze = qualified_name(catalog, schema)
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    table_names = ["orders_raw", "users_raw", "products_raw", "clickstream_raw"]
    if reset_demo_data == "false":
        existing = [name for name in table_names if spark.catalog.tableExists(f"{bronze}.{name}")]
        if existing:
            raise ValueError(f"Demo tables already exist: {existing}; explicitly enable reset-demo-data")
    from pyspark.sql.functions import current_timestamp, lit

    data_date = data_date or datetime.datetime.now(datetime.timezone.utc).date()
    generator = random.Random(seed)
    datasets = [
        ("orders_raw", generate_orders(spark, generator, data_date, seed), "mock_oms"),
        ("users_raw", generate_users(spark, generator), "mock_crm"),
        ("products_raw", generate_products(spark, generator), "mock_pim"),
        ("clickstream_raw", generate_clickstream(spark, generator, data_date, seed), "mock_clickstream"),
    ]
    for table, dataset, source in datasets:
        (
            dataset.withColumn("ingested_at", current_timestamp())
            .withColumn("source_system", lit(source))
            .write.format("delta")
            .mode("overwrite" if reset_demo_data == "true" else "error")
            .saveAsTable(f"{bronze}.{table}")
        )


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--environment", choices=["dev"], required=True)
    parser.add_argument("--reset-demo-data", choices=["true", "false"], default="false")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--data-date", type=datetime.date.fromisoformat)
    args = parser.parse_args(argv)
    if not (args.catalog.endswith("_dev") or args.schema.endswith("_dev")):
        parser.error("Mock data requires a catalog or schema ending in _dev")
    from pyspark.sql import SparkSession

    generate_demo_data(
        SparkSession.builder.getOrCreate(), args.catalog, args.schema,
        args.reset_demo_data, args.seed, args.data_date,
    )


if __name__ == "__main__":
    main()