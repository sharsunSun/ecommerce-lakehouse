from pyspark import pipelines as dp
from pyspark.sql import SparkSession

from ecommerce_lakehouse.config import pipeline_namespace
from ecommerce_lakehouse.transforms.gold import (
    aggregate_daily_revenue,
    aggregate_product_popularity,
    build_order_wide,
)


spark = SparkSession.builder.getOrCreate()
SILVER = pipeline_namespace(spark, "silver")
GOLD = pipeline_namespace(spark, "gold")


@dp.materialized_view(name=f"{GOLD}.fact_order_wide")
@dp.expect("known_user", "city IS NOT NULL")
@dp.expect("known_product", "category IS NOT NULL")
def fact_order_wide():
    return build_order_wide(
        spark.read.table(f"{SILVER}.orders"),
        spark.read.table(f"{SILVER}.users"),
        spark.read.table(f"{SILVER}.products"),
    )


@dp.materialized_view(name=f"{GOLD}.agg_daily_revenue")
def agg_daily_revenue():
    return aggregate_daily_revenue(spark.read.table(f"{SILVER}.orders"))


@dp.materialized_view(name=f"{GOLD}.agg_product_popularity")
def agg_product_popularity():
    return aggregate_product_popularity(spark.read.table(f"{SILVER}.clickstream"))