import argparse

import sqlparse

from ecommerce_lakehouse.sql_resources import render_sql


def initialize_catalog(spark, **identifiers):
    for statement in sqlparse.split(render_sql("initialize.sql", **identifiers)):
        spark.sql(statement)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Initialize lakehouse schemas in an existing catalog")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--bronze-schema", default="bronze")
    parser.add_argument("--silver-schema", default="silver")
    parser.add_argument("--gold-schema", default="gold")
    parser.add_argument("--semantic-schema", default="semantic")
    parser.add_argument("--rag-schema", default="rag")
    args = parser.parse_args(argv)
    from pyspark.sql import SparkSession

    initialize_catalog(SparkSession.builder.getOrCreate(), **vars(args))


if __name__ == "__main__":
    main()