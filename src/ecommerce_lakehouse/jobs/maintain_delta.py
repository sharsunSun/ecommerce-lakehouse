import argparse
import logging

from ecommerce_lakehouse.config import qualified_name


def maintain_tables(spark, catalog, schema, tables, vacuum=False, retention_hours=336):
    if retention_hours < 168:
        raise ValueError("VACUUM retention must be at least 168 hours")
    table_names = [qualified_name(catalog, schema, table) for table in tables]
    for table in table_names:
        details = spark.sql(f"DESCRIBE DETAIL {table}").first()
        properties = details["properties"] or {}
        if any(key.startswith("pipelines.") for key in properties):
            logging.info("Skipping pipeline-managed dataset: %s", table)
            continue
        if details["format"] != "delta":
            raise ValueError(f"Maintenance requires a Delta table: {table}")
        logging.info("Optimizing %s", table)
        spark.sql(f"OPTIMIZE {table}")
        if vacuum:
            spark.sql(f"VACUUM {table} RETAIN {retention_hours} HOURS")


def main(argv=None):
    parser = argparse.ArgumentParser(description="Maintain independent Delta tables")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--tables", nargs="+", required=True)
    parser.add_argument("--vacuum", action="store_true")
    parser.add_argument("--retention-hours", type=int, default=336)
    args = parser.parse_args(argv)
    if args.retention_hours < 168:
        parser.error("VACUUM retention must be at least 168 hours")
    logging.basicConfig(level=logging.INFO)
    from pyspark.sql import SparkSession

    maintain_tables(SparkSession.builder.getOrCreate(), **vars(args))


if __name__ == "__main__":
    main()