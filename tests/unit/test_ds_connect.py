from databricks.connect import DatabricksSession

if __name__ == "__main__":
	spark = DatabricksSession.builder.profile("free").serverless().getOrCreate()
	print(spark.range(5))
	