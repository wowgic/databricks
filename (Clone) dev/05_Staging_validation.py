# Databricks notebook source
# MAGIC %md
# MAGIC <b>Staging Validation</b>

# COMMAND ----------

import mlflow
from mlflow.tracking import MlflowClient

client = MlflowClient()
model_name = "mlops_demo_predict_stu_per"

# Get the latest version details
latest_version_details = client.get_latest_versions(model_name, stages=["None"])[0]

# Retrieve the latest version
model_version = latest_version_details.version

# Get run information for the latest version
run_info = client.get_run(run_id=latest_version_details.run_id)

print("Latest Model Version:",model_version)
print("Run ID:", run_info.info.run_id)

# COMMAND ----------

import mlflow
from databricks.feature_store import FeatureStoreClient
from pyspark.sql.functions import struct

from pyspark.sql.types import DoubleType

fs = FeatureStoreClient()
 
# Read from feature store 
data_source = run_info.data.tags['db_table']

features = fs.read_table(data_source)
 
# Load model as a Spark UDF
model_uri = f'models:/{model_name}/{model_version}'
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=model_uri, result_type="string")

# Select the feature table cols by model input schema
input_column_names = loaded_model.metadata.get_input_schema().input_names()
 
# Predict on a Spark DataFrame
try:
  display(features.withColumn('predictions', loaded_model(*input_column_names)))
  client.set_model_version_tag(name="mlops_demo_predict_stu_per", version=model_version, key="predict", value=1)

except Exception: 
  print("Unable to predict on features.")
  client.set_model_version_tag(name="mlops_demo_predict_stu_per", version=model_version, key="predict", value=0)
  pass

# COMMAND ----------

results = client.get_model_version(model_name, model_version)
results.tags

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Trigger Approval Webhook</b>

# COMMAND ----------

transition_request_body = {'name': model_name, 'version': model_version, 'stage': 'Staging', 'archive_existing_versions': 'true'}
mlflow_call_endpoint('transition-requests/approve', 'POST', json.dumps(transition_request_body))
