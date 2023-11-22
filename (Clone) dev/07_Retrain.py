# Databricks notebook source
#monthly Training

from databricks.feature_store import FeatureStoreClient

fs = FeatureStoreClient()
features = fs.read_table(f'default.dbdemos_mlops_features')

# COMMAND ----------

import databricks.automl
model = databricks.automl.classify(features, target_col = "Status", data_dir= "dbfs:/tmp/", timeout_minutes=5) 

# COMMAND ----------

#register best run

import mlflow
from mlflow.tracking.client import MlflowClient
 
client = MlflowClient()
 
run_id = model.best_trial.mlflow_run_id
model_name = "mlops_demo_predict_stu_per"
model_uri = f"runs:/{run_id}/model"
 
client.set_tag(run_id, key='db_table', value=f'default.dbdemos_mlops_features') 
model_details = mlflow.register_model(model_uri, model_name)

# COMMAND ----------

import mlflow
from mlflow.utils.rest_utils import http_request
import json
def client():
  return mlflow.tracking.client.MlflowClient()

host_creds = client()._tracking_client.store.get_host_creds()
def mlflow_call_endpoint(endpoint, method, body='{}'):
  if method == 'GET':
      response = http_request(
          host_creds=host_creds, endpoint="/api/2.0/mlflow/{}".format(endpoint), method=method, params=json.loads(body))
  else:
      response = http_request(
          host_creds=host_creds, endpoint="/api/2.0/mlflow/{}".format(endpoint), method=method, json=json.loads(body))
  return response.json()

# COMMAND ----------

#staging request


staging_request = {'name': model_name, 'version': model_details.version, 'stage': 'Staging', 'archive_existing_versions': 'true'}
mlflow_call_endpoint('transition-requests/create', 'POST', json.dumps(staging_request))


