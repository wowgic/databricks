# Databricks notebook source
#Install Databricks webhooks utility
%pip install databricks-registry-webhooks

# COMMAND ----------

# MAGIC %md
# MAGIC Create Webhooks - 
# MAGIC Setting up webhooks is simple using the Databricks REST API. There are some helper functions in the ./_resources/API_Helpers notebook, so if you want to see additional details you can check there.

# COMMAND ----------


import mlflow
import numpy as np
model_name = "dbdemos_mlops"
model_version = 1

model_uri = f"models:/{model_name}/Staging"
model = mlflow.pyfunc.load_model(model_uri=model_uri)


# COMMAND ----------


new_data_point = {
    'Student_id': 'Js6Hg9Qb3Q',
    'participation': 0.8,
    'Project': 0.06,
    'Milestone': 0.4
}

# Make predictions
prediction = model.predict(new_data_point)
print(prediction)

# COMMAND ----------

predict = mlflow.pyfunc.spark_udf(spark, model_uri, result_type="double")

# COMMAND ----------

import mlflow
from mlflow.tracking import MlflowClient

client = MlflowClient()
model_name = "dbdemos_mlops"
model_details = client.get_model_version(model_name, model_version)
run_info = client.get_run(run_id=model_details.run_id)

# COMMAND ----------

import mlflow
from databricks.feature_store import FeatureStoreClient
 
fs = FeatureStoreClient()
 
# Read from feature store 
data_source = run_info.data.tags['delta_production']
features = fs.read_table(data_source)
 
# Load model as a Spark UDF
model_uri = f'models:/{model_name}/{model_version}'
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=model_uri)
 
# Select the feature table cols by model input schema
input_column_names = loaded_model.metadata.get_input_schema().input_names()
 
# Predict on a Spark DataFrame
try:
  display(features.withColumn('predictions', loaded_model(*input_column_names)))
  client.set_model_version_tag(name=model_name, version=model_version, key="predicts", value=1)
except Exception: 
  print("Unable to predict on features.")
  client.set_model_version_tag(name=model_name, version=model_version, key="predicts", value=0)
  pass
