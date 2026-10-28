import os

os.environ.setdefault("GRPC_DNS_RESOLVER", "native")

from databricks.connect import DatabricksSession

if __name__ == "__main__":
	print("Connecting to Databricks Serverless...", flush=True)
	spark = DatabricksSession.builder.profile("free").serverless().getOrCreate()
	print("Connected. Fetching rows...", flush=True)
	spark.range(5).show()
	