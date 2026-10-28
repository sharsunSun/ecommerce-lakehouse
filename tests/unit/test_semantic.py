import sqlite3
import unittest

import sqlparse
import yaml

from ecommerce_lakehouse.sql_resources import render_sql


class SemanticTests(unittest.TestCase):
    def setUp(self):
        self.identifiers = {
            "catalog": "test_dev", "bronze_schema": "bronze", "silver_schema": "silver",
            "gold_schema": "gold", "semantic_schema": "semantic", "rag_schema": "rag",
        }

    def test_packaged_resources_and_grant_preserving_statements(self):
        initialization = sqlparse.split(render_sql("initialize.sql", **self.identifiers))
        self.assertEqual(len(initialization), 5)
        self.assertTrue(all("`test_dev`" in statement for statement in initialization))
        creation = render_sql("revenue_metrics.sql", **self.identifiers)
        alteration = render_sql("revenue_metrics.sql", alter_metric_view=True, **self.identifiers)
        self.assertTrue(creation.startswith("CREATE VIEW IF NOT EXISTS"))
        self.assertTrue(alteration.startswith("ALTER VIEW"))
        self.assertNotIn("WITH METRICS", alteration)
        self.assertEqual(len(sqlparse.split(creation)), 1)

    def test_unknown_resource_is_rejected(self):
        with self.assertRaises(ValueError):
            render_sql("../pyproject.toml")

    def test_metrics_across_days_and_empty_data(self):
        sql = render_sql("revenue_metrics.sql", **self.identifiers)
        definition = yaml.safe_load(sql.split("$$")[1])
        self.assertIn("fact_order_wide", definition["source"])
        expressions = ", ".join(measure["expr"] for measure in definition["measures"])
        with sqlite3.connect(":memory:") as connection:
            connection.execute("CREATE TABLE orders(order_id TEXT, user_id TEXT, total_amount REAL, status TEXT, sale_date TEXT)")
            rows = [(str(index), "same_buyer", 10.0, "paid", "2026-10-25") for index in range(100)]
            rows += [("last", "same_buyer", 100.0, "paid", "2026-10-26"),
                     ("refund", "other", 999.0, "refunded", "2026-10-26")]
            connection.executemany("INSERT INTO orders VALUES (?, ?, ?, ?, ?)", rows)
            query = f"SELECT {expressions} FROM orders WHERE {definition['filter']}"
            result = connection.execute(query).fetchone()
            self.assertEqual(result[:3], (101, 1, 1100.0))
            self.assertAlmostEqual(result[3], 1100 / 101)
            connection.execute("DELETE FROM orders")
            self.assertIsNone(connection.execute(query).fetchone()[3])


if __name__ == "__main__":
    unittest.main()