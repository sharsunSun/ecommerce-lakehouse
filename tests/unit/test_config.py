import ast
import unittest
from pathlib import Path
from unittest.mock import MagicMock

import yaml

from ecommerce_lakehouse.config import pipeline_namespace, qualified_name


ROOT = Path(__file__).resolve().parents[2]


class ConfigurationTests(unittest.TestCase):
    def test_schemas_are_protected_deployment_dependencies(self):
        bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
        self.assertEqual(bundle["bundle"]["engine"], "direct")
        self.assertEqual(bundle["targets"]["dev"]["variables"]["catalog"], "workspace")
        for layer in ("bronze", "silver", "gold", "semantic", "rag"):
            self.assertEqual(bundle["targets"]["dev"]["variables"][f"{layer}_schema"], f"ecommerce_{layer}_dev")
        resources = yaml.safe_load((ROOT / "resources/schemas.yml").read_text())["resources"]
        self.assertNotIn("catalogs", resources)
        self.assertEqual(set(resources["schemas"]), {"bronze", "silver", "gold", "semantic", "rag"})
        for layer, schema in resources["schemas"].items():
            self.assertEqual(schema["name"], "${var." + layer + "_schema}")
            self.assertEqual(schema["catalog_name"], "${var.catalog}")
            self.assertIs(schema["lifecycle"]["prevent_destroy"], True)
        pipeline = yaml.safe_load((ROOT / "resources/pipeline.yml").read_text())["resources"]["pipelines"]["ecommerce_lakehouse_main"]
        self.assertEqual(pipeline["catalog"], "${resources.schemas.gold.catalog_name}")
        self.assertEqual(pipeline["schema"], "${resources.schemas.gold.name}")
        for layer in ("bronze", "silver", "gold"):
            self.assertEqual(pipeline["configuration"][f"ecommerce.{layer}_schema"], "${resources.schemas." + layer + ".name}")
        jobs = yaml.safe_load((ROOT / "resources/jobs.yml").read_text())["resources"]["jobs"]
        for job in jobs.values():
            for task in job["tasks"]:
                if "python_wheel_task" in task:
                    parameters = task["python_wheel_task"]["parameters"]
                    catalog_reference = parameters[parameters.index("--catalog") + 1]
                    self.assertRegex(catalog_reference, r"^\$\{resources\.schemas\.(gold|semantic|bronze)\.catalog_name\}$")

    def test_target_modes_replace_deprecated_pipeline_development(self):
        bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
        self.assertEqual(bundle["targets"]["dev"]["mode"], "development")
        self.assertEqual(bundle["targets"]["prod"]["mode"], "production")
        configs = [bundle]
        configs.extend(yaml.safe_load(path.read_text()) for path in (ROOT / "resources").glob("*.yml"))
        for config in configs:
            resource_sections = [config.get("resources", {})]
            resource_sections.extend(target.get("resources", {}) for target in config.get("targets", {}).values())
            for resources in resource_sections:
                for pipeline in resources.get("pipelines", {}).values():
                    self.assertNotIn("development", pipeline)

    def test_identifier_validation_and_pipeline_namespace(self):
        self.assertEqual(qualified_name("test_dev", "gold"), "`test_dev`.`gold`")
        for invalid in ("", "x; DROP TABLE orders", "x.y", "x`", "x\nsource: other"):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                qualified_name(invalid)
        spark = MagicMock()
        spark.conf.get.side_effect = {"ecommerce.catalog": "test_dev", "ecommerce.gold_schema": "gold"}.__getitem__
        self.assertEqual(pipeline_namespace(spark, "gold"), "`test_dev`.`gold`")

    def test_wheel_tasks_and_refresh_separation(self):
        bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
        jobs = yaml.safe_load((ROOT / "resources/jobs.yml").read_text())["resources"]["jobs"]
        self.assertIn("lakehouse", bundle["artifacts"])
        self.assertNotIn("generate_mock_data", jobs)
        self.assertEqual(len(jobs["ecommerce_refresh"]["tasks"]), 1)
        self.assertIn("pipeline_task", jobs["ecommerce_refresh"]["tasks"][0])
        entry_points = set()
        for job in jobs.values():
            self.assertEqual(job["max_concurrent_runs"], 1)
            task_keys = {task["task_key"] for task in job["tasks"]}
            for task in job["tasks"]:
                for dependency in task.get("depends_on", []):
                    self.assertIn(dependency["task_key"], task_keys)
                self.assertNotIn("notebook_task", task)
                self.assertNotIn("spark_python_task", task)
                if "python_wheel_task" in task:
                    wheel = task["python_wheel_task"]
                    self.assertEqual(wheel["package_name"], "ecommerce_lakehouse")
                    self.assertIn("--catalog", wheel["parameters"])
                    entry_points.add(wheel["entry_point"])
                    self.assertEqual(task["environment_key"], "default")
                    self.assertNotIn("libraries", task)
        self.assertEqual(entry_points, {"initialize", "publish_semantic", "maintain_delta"})

    def test_free_edition_jobs_use_serverless_environments(self):
        bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
        self.assertEqual(bundle["variables"]["serverless_environment_version"]["default"], "4")
        self.assertNotIn("job_cluster_spark_version", bundle["variables"])
        self.assertNotIn("job_cluster_node_type", bundle["variables"])
        jobs = yaml.safe_load((ROOT / "resources/jobs.yml").read_text())["resources"]["jobs"]
        demo = yaml.safe_load((ROOT / "resources/demo.yml").read_text())
        jobs = {**jobs, **demo["targets"]["dev"]["resources"]["jobs"]}
        forbidden = {
            "job_clusters", "job_cluster_key", "new_cluster", "existing_cluster_id",
            "spark_version", "node_type_id", "num_workers", "data_security_mode",
            "runtime_engine", "spark_conf", "libraries",
        }

        def check_no_classic_compute(value):
            if isinstance(value, dict):
                self.assertFalse(forbidden.intersection(value), forbidden.intersection(value))
                for child in value.values():
                    check_no_classic_compute(child)
            elif isinstance(value, list):
                for child in value:
                    check_no_classic_compute(child)

        for job in jobs.values():
            check_no_classic_compute(job)
            environments = {item["environment_key"]: item["spec"] for item in job.get("environments", [])}
            for task in job["tasks"]:
                if "python_wheel_task" in task or "spark_python_task" in task:
                    self.assertIn(task["environment_key"], environments)
                    environment = environments[task["environment_key"]]
                    self.assertEqual(environment["environment_version"], "${var.serverless_environment_version}")
                    self.assertEqual(environment["dependencies"], ["../dist/*.whl"])
                else:
                    self.assertNotIn("environment_key", task)

    def test_demo_is_dev_only_and_publishes_after_refresh(self):
        config = yaml.safe_load((ROOT / "resources/demo.yml").read_text())
        self.assertEqual(set(config), {"targets"})
        self.assertEqual(set(config["targets"]), {"dev"})
        demo = config["targets"]["dev"]["resources"]["jobs"]["generate_mock_data"]
        tasks = {task["task_key"]: task for task in demo["tasks"]}
        self.assertNotIn("initialize", tasks)
        self.assertNotIn("depends_on", tasks["generate"])
        parameters = tasks["generate"]["spark_python_task"]["parameters"]
        self.assertEqual(parameters[parameters.index("--catalog") + 1], "${resources.schemas.bronze.catalog_name}")
        self.assertEqual(parameters[parameters.index("--schema") + 1], "${resources.schemas.bronze.name}")
        self.assertEqual(tasks["refresh"]["depends_on"], [{"task_key": "generate"}])
        self.assertEqual(tasks["publish_semantic"]["depends_on"], [{"task_key": "refresh"}])
        source = tasks["generate"]["spark_python_task"]["python_file"]
        self.assertTrue((ROOT / "resources" / source).is_file())

    def test_pipeline_registration_is_thin_and_side_effect_free(self):
        pipeline = yaml.safe_load((ROOT / "resources/pipeline.yml").read_text())["resources"]["pipelines"]["ecommerce_lakehouse_main"]
        self.assertIs(pipeline["serverless"], True)
        self.assertNotIn("clusters", pipeline)
        self.assertEqual(pipeline["root_path"], "${workspace.file_path}/src")
        definitions = []
        for library in pipeline["libraries"]:
            path = ROOT / "resources" / library["file"]["path"]
            source = path.read_text()
            self.assertNotIn("${var.", source)
            self.assertNotIn("/Volumes/", source)
            tree = ast.parse(source)
            for node in ast.walk(tree):
                if isinstance(node, ast.Attribute):
                    self.assertNotIn(node.attr, {"saveAsTable", "save", "collect", "toPandas", "start"})
                if isinstance(node, ast.FunctionDef):
                    definitions.append(node)
                    self.assertEqual(len(node.body), 1)
                    self.assertIsInstance(node.body[0], ast.Return)
        self.assertEqual(len(definitions), 7)


if __name__ == "__main__":
    unittest.main()