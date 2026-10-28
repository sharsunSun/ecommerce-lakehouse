from pyspark.sql import Window
from pyspark.sql import functions as F


ORDER_EXPECTATIONS = {
    "valid_order_id": "order_id IS NOT NULL AND trim(order_id) <> ''",
    "valid_user_id": "user_id IS NOT NULL AND trim(user_id) <> ''",
    "valid_product_id": "product_id IS NOT NULL AND trim(product_id) <> ''",
    "valid_quantity": "quantity > 0",
    "valid_price": "unit_price > 0 AND total_amount IS NOT NULL",
    "valid_created_at": "created_at IS NOT NULL",
    "valid_ingested_at": "ingested_at IS NOT NULL",
    "valid_currency": "currency = 'CNY'",
    "valid_status": "status IN ('paid', 'shipped', 'delivered', 'refunded')",
}
USER_EXPECTATIONS = {"valid_user_id": "user_id IS NOT NULL AND trim(user_id) <> ''"}
PRODUCT_EXPECTATIONS = {"valid_product_id": "product_id IS NOT NULL AND trim(product_id) <> ''"}
CLICK_EXPECTATIONS = {
    "valid_event_id": "event_id IS NOT NULL AND trim(event_id) <> ''",
    "valid_product_id": "product_id IS NOT NULL AND trim(product_id) <> ''",
    "valid_event_time": "event_time IS NOT NULL",
    "valid_event_type": "event_type IN ('view', 'add_to_cart', 'purchase', 'search')",
}


def latest_records(dataset, primary_key):
    fingerprint = F.sha2(
        F.to_json(F.struct(*[F.col(name) for name in sorted(dataset.columns)])), 256
    )
    ordering = Window.partitionBy(primary_key).orderBy(
        F.col("ingested_at").desc_nulls_last(), fingerprint.desc()
    )
    return (
        dataset.withColumn("_row_number", F.row_number().over(ordering))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )


def prepare_orders(orders):
    return (
        latest_records(orders, "order_id")
        .withColumn("unit_price", F.col("unit_price").cast("decimal(18, 2)"))
        .withColumn("total_amount", (F.col("unit_price") * F.col("quantity")).cast("decimal(28, 2)"))
        .select(
            "order_id", "user_id", "product_id", "quantity", "unit_price",
            "currency", "status", "total_amount", "created_at", "ingested_at",
        )
    )


def prepare_users(users):
    return latest_records(users, "user_id").select(
        "user_id", "user_name", "city", "member_level", "created_at"
    )


def prepare_products(products):
    return (
        latest_records(products, "product_id")
        .withColumn("price", F.col("price").cast("decimal(18, 2)"))
        .select("product_id", "product_name", "category", "brand", "price", "stock_qty")
    )


def prepare_clickstream(clickstream):
    return latest_records(clickstream, "event_id").select(
        "event_id", "user_id", "product_id", "event_type", "event_time", "platform"
    )