import argparse

from ecommerce_lakehouse.sql_resources import render_sql


def publish_metrics(spark, catalog, gold_schema, semantic_schema):
    identifiers = {
        "catalog": catalog, "gold_schema": gold_schema, "semantic_schema": semantic_schema,
    }
    spark.sql(render_sql("revenue_metrics.sql", **identifiers))
    spark.sql(render_sql("revenue_metrics.sql", alter_metric_view=True, **identifiers))


def main(argv=None):
    parser = argparse.ArgumentParser(description="Publish metric definitions while preserving grants")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--gold-schema", default="gold")
    parser.add_argument("--semantic-schema", default="semantic")
    args = parser.parse_args(argv)
    from pyspark.sql import SparkSession

    publish_metrics(SparkSession.builder.getOrCreate(), **vars(args))


if __name__ == "__main__":
    main()