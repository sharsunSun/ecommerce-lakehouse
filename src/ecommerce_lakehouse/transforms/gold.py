from pyspark.sql import functions as F


def build_order_wide(orders, users, products):
    return (
        orders.alias("orders")
        .join(users.alias("users"), "user_id", "left")
        .join(products.alias("products"), "product_id", "left")
        .select(
            "order_id", "user_id", "city", "member_level", "product_id",
            "category", "brand", "quantity", "unit_price", "total_amount",
            "status", "currency", F.col("orders.created_at").alias("created_at"),
        )
        .withColumn("sale_date", F.to_date("created_at"))
    )


def aggregate_daily_revenue(orders):
    return (
        orders.filter(F.col("status") != "refunded")
        .groupBy(F.to_date("created_at").alias("sale_date"))
        .agg(
            F.countDistinct("order_id").alias("order_cnt"),
            F.countDistinct("user_id").alias("buyer_cnt"),
            F.sum("total_amount").alias("gmv"),
        )
        .withColumn("avg_order_value", F.expr("gmv / NULLIF(order_cnt, 0)"))
    )


def aggregate_product_popularity(clickstream):
    return (
        clickstream.groupBy("product_id", "event_type")
        .agg(F.count("*").alias("event_cnt"))
    )