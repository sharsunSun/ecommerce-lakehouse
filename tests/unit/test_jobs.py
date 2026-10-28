import contextlib
import datetime
import io
import random
import sys
import types
import unittest
from decimal import Decimal
from unittest.mock import MagicMock, patch

from ecommerce_lakehouse.jobs import initialize, maintain_delta, publish_semantic
from scripts.demo import generate_mock_data


class JobTests(unittest.TestCase):
    def test_initialization_only_creates_schemas_in_existing_catalog(self):
        spark = MagicMock()
        initialize.initialize_catalog(
            spark, catalog="test_dev", bronze_schema="bronze", silver_schema="silver",
            gold_schema="gold", semantic_schema="semantic", rag_schema="rag",
        )
        self.assertEqual(spark.sql.call_count, 5)
        self.assertTrue(all("`test_dev`" in call.args[0] for call in spark.sql.call_args_list))
        self.assertTrue(all("CREATE SCHEMA" in call.args[0] for call in spark.sql.call_args_list))

    def test_publication_preserves_grants(self):
        spark = MagicMock()
        publish_semantic.publish_metrics(spark, "test_dev", "gold", "semantic")
        statements = [call.args[0] for call in spark.sql.call_args_list]
        self.assertEqual(len(statements), 2)
        self.assertTrue(statements[0].startswith("CREATE VIEW IF NOT EXISTS"))
        self.assertTrue(statements[1].startswith("ALTER VIEW"))
        self.assertFalse(any("CREATE OR REPLACE" in statement for statement in statements))

    def test_maintenance_has_no_zorder_or_implicit_vacuum(self):
        spark = MagicMock()
        spark.sql.return_value.first.return_value = {"properties": {}, "format": "delta"}
        maintain_delta.maintain_tables(spark, "test_dev", "bronze", ["orders_raw", "users_raw"])
        statements = [call.args[0] for call in spark.sql.call_args_list]
        self.assertEqual(sum(statement.startswith("OPTIMIZE") for statement in statements), 2)
        self.assertFalse(any("ZORDER" in statement or "VACUUM" in statement for statement in statements))

    def test_pipeline_managed_tables_are_skipped(self):
        spark = MagicMock()
        spark.sql.return_value.first.return_value = {
            "properties": {"pipelines.pipelineId": "pipeline"}, "format": "delta",
        }
        maintain_delta.maintain_tables(spark, "test_dev", "gold", ["fact_order_wide"], vacuum=True)
        self.assertEqual(spark.sql.call_count, 1)

    def test_explicit_vacuum_retains_requested_history(self):
        spark = MagicMock()
        spark.sql.return_value.first.return_value = {"properties": {}, "format": "delta"}
        maintain_delta.maintain_tables(spark, "test_dev", "bronze", ["orders_raw"], vacuum=True)
        self.assertEqual(spark.sql.call_args.args[0], "VACUUM `test_dev`.`bronze`.`orders_raw` RETAIN 336 HOURS")

    def test_short_retention_is_rejected_before_sql(self):
        spark = MagicMock()
        with self.assertRaises(ValueError):
            maintain_delta.maintain_tables(spark, "test_dev", "bronze", ["orders_raw"], retention_hours=1)
        spark.sql.assert_not_called()


class DemoTests(unittest.TestCase):
    def test_demo_write_modes_are_connect_compatible_and_safe(self):
        functions = types.ModuleType("pyspark.sql.functions")
        functions.current_timestamp = MagicMock()
        functions.lit = MagicMock()
        for reset, expected_mode in (("false", "error"), ("true", "overwrite")):
            with self.subTest(reset=reset):
                spark = MagicMock()
                spark.catalog.tableExists.return_value = False
                dataset = spark.createDataFrame.return_value
                dataset.withColumn.return_value = dataset
                writer = dataset.write
                writer.format.return_value = writer
                writer.mode.return_value = writer
                with patch.dict(sys.modules, {"pyspark.sql.functions": functions}):
                    generate_mock_data.generate_demo_data(
                        spark, "workspace", "ecommerce_bronze_dev",
                        reset_demo_data=reset, data_date=datetime.date(2026, 10, 27),
                    )
                self.assertEqual(writer.mode.call_count, 4)
                self.assertTrue(all(call.args == (expected_mode,) for call in writer.mode.call_args_list))
                self.assertEqual(
                    [call.args[0] for call in writer.saveAsTable.call_args_list],
                    [f"`workspace`.`ecommerce_bronze_dev`.{table}" for table in
                     ("orders_raw", "users_raw", "products_raw", "clickstream_raw")],
                )

    def test_production_is_rejected_before_spark(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            generate_mock_data.main([
                "--catalog", "ecommerce_lakehouse", "--schema", "bronze", "--environment", "dev",
            ])

    def test_existing_data_requires_explicit_reset(self):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        with self.assertRaisesRegex(ValueError, "already exist"):
            generate_mock_data.generate_demo_data(spark, "test_dev", "bronze")
        spark.createDataFrame.assert_not_called()

    def test_dev_schema_in_workspace_catalog_keeps_overwrite_protection(self):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        with self.assertRaisesRegex(ValueError, "already exist"):
            generate_mock_data.generate_demo_data(spark, "workspace", "ecommerce_bronze_dev")
        spark.createDataFrame.assert_not_called()

    def test_production_namespace_is_rejected_by_operation(self):
        spark = MagicMock()
        with self.assertRaisesRegex(ValueError, "ending in _dev"):
            generate_mock_data.generate_demo_data(spark, "ecommerce_lakehouse", "bronze")
        spark.catalog.tableExists.assert_not_called()

    def test_generator_is_reproducible_and_decimal(self):
        spark = MagicMock()
        data_date = datetime.date(2026, 10, 26)
        generate_mock_data.generate_orders(spark, random.Random(42), data_date, 42)
        first = spark.createDataFrame.call_args
        generate_mock_data.generate_orders(spark, random.Random(42), data_date, 42)
        self.assertEqual(first, spark.createDataFrame.call_args)
        rows = first.args[0]
        self.assertEqual(len({row["order_id"] for row in rows}), 200)
        self.assertTrue(all(isinstance(row["unit_price"], Decimal) for row in rows))
        self.assertIn("DECIMAL(18,2)", first.kwargs["schema"])


if __name__ == "__main__":
    unittest.main()