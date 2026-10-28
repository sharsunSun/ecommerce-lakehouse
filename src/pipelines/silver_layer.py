from pyspark import pipelines as dp
from pyspark.sql import SparkSession

from ecommerce_lakehouse.config import pipeline_namespace
from ecommerce_lakehouse.transforms.silver import (
    CLICK_EXPECTATIONS,
    ORDER_EXPECTATIONS,
    PRODUCT_EXPECTATIONS,
    USER_EXPECTATIONS,
    prepare_clickstream,
    prepare_orders,
    prepare_products,
    prepare_users,
)


spark = SparkSession.builder.getOrCreate()
BRONZE = pipeline_namespace(spark, "bronze")
SILVER = pipeline_namespace(spark, "silver")


@dp.materialized_view(name=f"{SILVER}.orders")
@dp.expect_all_or_drop(ORDER_EXPECTATIONS)
def silver_orders():
    return prepare_orders(spark.read.table(f"{BRONZE}.orders_raw"))


@dp.materialized_view(name=f"{SILVER}.users")
@dp.expect_all_or_drop(USER_EXPECTATIONS)
def silver_users():
    return prepare_users(spark.read.table(f"{BRONZE}.users_raw"))


@dp.materialized_view(name=f"{SILVER}.products")
@dp.expect_all_or_drop(PRODUCT_EXPECTATIONS)
def silver_products():
    return prepare_products(spark.read.table(f"{BRONZE}.products_raw"))


@dp.materialized_view(name=f"{SILVER}.clickstream")
@dp.expect_all_or_drop(CLICK_EXPECTATIONS)
def silver_clickstream():
    return prepare_clickstream(spark.read.table(f"{BRONZE}.clickstream_raw"))

