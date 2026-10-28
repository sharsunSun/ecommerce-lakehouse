from databricks.connect import DatabricksSession

if __name__ == "__main__":
	spark = DatabricksSession.builder.profile("free").serverless().getOrCreate()
	spark.range(5).show()
	