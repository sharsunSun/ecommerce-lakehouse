import datetime
import unittest
from decimal import Decimal

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from ecommerce_lakehouse.sql_resources import render_sql
from ecommerce_lakehouse.transforms.gold import (
    aggregate_daily_revenue,
    aggregate_product_popularity,
    build_order_wide,
)
from ecommerce_lakehouse.transforms.silver import ORDER_EXPECTATIONS, latest_records, prepare_orders


class TransformationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[2]").appName("lakehouse-integration")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def orders(self):
        timestamp = datetime.datetime(2026, 10, 25, 12)
        rows = [
            ("order_1", "user_1", "prod_1", 2, Decimal("10.25"), "CNY", "paid", timestamp, timestamp),
            ("order_1", "user_1", "prod_1", 3, Decimal("10.25"), "CNY", "paid", timestamp, timestamp + datetime.timedelta(hours=1)),
            ("order_2", "user_1", "prod_missing", 1, Decimal("99.99"), "CNY", "paid", timestamp + datetime.timedelta(days=1), timestamp),
            ("refund", "user_2", "prod_1", 1, Decimal("999.00"), "CNY", "refunded", timestamp, timestamp),
        ]
        return self.spark.createDataFrame(rows, schema=(
            "order_id STRING, user_id STRING, product_id STRING, quantity BIGINT, "
            "unit_price DECIMAL(18,2), currency STRING, status STRING, "
            "created_at TIMESTAMP, ingested_at TIMESTAMP"
        ))

    def test_latest_record_and_decimal_contract(self):
        orders = prepare_orders(self.orders())
        self.assertEqual(orders.count(), 3)
        order = orders.filter("order_id = 'order_1'").first()
        self.assertEqual(order.quantity, 3)
        self.assertEqual(order.total_amount, Decimal("30.75"))
        self.assertEqual(orders.schema["unit_price"].dataType, DecimalType(18, 2))
        self.assertEqual(orders.schema["total_amount"].dataType, DecimalType(28, 2))

    def test_equal_timestamps_have_deterministic_winner(self):
        rows = [("same", "alpha", datetime.datetime(2026, 10, 25)),
                ("same", "beta", datetime.datetime(2026, 10, 25))]
        source = self.spark.createDataFrame(rows, "id STRING, value STRING, ingested_at TIMESTAMP")
        first = latest_records(source.repartition(1), "id").first().value
        second = latest_records(source.repartition(2), "id").first().value
        self.assertEqual(first, second)

    def test_daily_refunds_and_unmatched_dimension_cardinality(self):
        orders = prepare_orders(self.orders())
        users = self.spark.createDataFrame(
            [("user_1", "Shanghai", "gold")], "user_id STRING, city STRING, member_level STRING"
        )
        products = self.spark.createDataFrame(
            [("prod_1", "electronics", "brand")], "product_id STRING, category STRING, brand STRING"
        )
        wide = build_order_wide(orders, users, products)
        self.assertEqual(wide.count(), orders.count())
        self.assertIsNone(wide.filter("order_id = 'order_2'").first().category)
        daily = aggregate_daily_revenue(orders).orderBy("sale_date").collect()
        self.assertEqual(len(daily), 2)
        self.assertEqual(daily[0].gmv, Decimal("30.75"))
        self.assertEqual(daily[1].gmv, Decimal("99.99"))
        self.assertEqual(daily[0].order_cnt, 1)

    def test_shared_expectations_and_semantic_expressions_execute_in_spark(self):
        orders = prepare_orders(self.orders())
        invalid = orders.withColumn("quantity", F.lit(0))
        predicate = " AND ".join(f"({rule})" for rule in ORDER_EXPECTATIONS.values())
        self.assertEqual(invalid.filter(predicate).count(), 0)
        definition = yaml.safe_load(render_sql(
            "revenue_metrics.sql", catalog="test_dev", gold_schema="gold", semantic_schema="semantic",
        ).split("$$")[1])
        result = orders.filter(definition["filter"]).agg(*[
            F.expr(measure["expr"]).alias(measure["name"]) for measure in definition["measures"]
        ]).first()
        self.assertEqual(result.order_cnt, 2)
        self.assertEqual(result.buyer_cnt, 1)
        self.assertEqual(result.gmv, Decimal("130.74"))
        self.assertEqual(result.avg_order_value, Decimal("65.37"))

    def test_product_popularity_counts_events(self):
        source = self.spark.createDataFrame(
            [("prod_1", "view"), ("prod_1", "view"), ("prod_1", "purchase")],
            "product_id STRING, event_type STRING",
        )
        result = {row.event_type: row.event_cnt for row in aggregate_product_popularity(source).collect()}
        self.assertEqual(result, {"view": 2, "purchase": 1})


if __name__ == "__main__":
    unittest.main()