# Databricks notebook source
# MAGIC %md
# MAGIC # MLflow Model Registry Webhooks REST API Example

# COMMAND ----------

# MAGIC %md ## Install MLflow and setup Python-REST API Wrapper

# COMMAND ----------

# MAGIC %pip install mlflow

# COMMAND ----------

# This Python wrapper facilitates the use of webhooks by encapsulating the REST API calls.

import mlflow
import urllib
import json

class HttpClient:
  def __init__(self, base_url, token):
    self.base_url = base_url
    self.token = token
  
  def createWebhook(self, request):
    return self._post('api/2.0/mlflow/registry-webhooks/create', request)

  def updateWebhook(self, request):
    return self._patch('api/2.0/mlflow/registry-webhooks/update', request)

  def listWebhooks(self, request):
    return self._get('api/2.0/mlflow/registry-webhooks/list', request)

  def deleteWebhook(self, request):
    return self._delete('api/2.0/mlflow/registry-webhooks/delete', request)
  
  def testWebhook(self, request):
    return self._post('api/2.0/mlflow/registry-webhooks/test', request)
    
  def _get(self, uri, params):
    data = urllib.parse.urlencode(params)
    url = f'{self.base_url}/{uri}/?{data}'
    headers = { 'Authorization': f'Bearer {self.token}'}

    req = urllib.request.Request(url, headers=headers)
    response = urllib.request.urlopen(req)
    return json.load(response)

  def _post(self, uri, body):
    json_body = json.dumps(body)
    json_bytes = json_body.encode('utf-8')
    headers = { 'Authorization': f'Bearer {self.token}'}

    url = f'{self.base_url}/{uri}'
    req = urllib.request.Request(url, data=json_bytes, headers=headers)
    response = urllib.request.urlopen(req)
    return json.load(response)

  def _patch(self, uri, body):
    json_body = json.dumps(body)
    json_bytes = json_body.encode('utf-8')
    headers = { 'Authorization': f'Bearer {self.token}'}

    url = f'{self.base_url}/{uri}'
    req = urllib.request.Request(url, data=json_bytes, headers=headers, method='PATCH')
    response = urllib.request.urlopen(req)
    return json.load(response)

  def _delete(self, uri, body):
    json_body = json.dumps(body)
    json_bytes = json_body.encode('utf-8')
    headers = { 'Authorization': f'Bearer {self.token}'}

    url = f'{self.base_url}/{uri}'
    req = urllib.request.Request(url, data=json_bytes, headers=headers, method='DELETE')
    response = urllib.request.urlopen(req)
    return json.load(response)

# COMMAND ----------

# MAGIC %md ## Create webhooks, test them, and confirm their presence in the list of all webhooks

# COMMAND ----------

## SETUP: Fill in variables
TOKEN = 'dapia7ea79e2d7c1aba7e19826b75ce384c2-3'
DOGFOOD_URL = 'https://adb-8979854987478940.0.azuredatabricks.net'
model_name = 'mlops_demo_predict_stu_per'
job_id = 898125460839212 # INSERT ID OF PRE-DEFINED JOB
slack_url = 'https://hooks.slack.com/services/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' # can also use custom URL endpoint here

# COMMAND ----------

httpClient = HttpClient(DOGFOOD_URL, TOKEN)

# COMMAND ----------

# Create a Job webhook
job_webhook = httpClient.createWebhook({
  "model_name": model_name,
  "events": ["TRANSITION_REQUEST_CREATED"],
  "status": "ACTIVE",
  "job_spec": {
    "job_id": job_id,
    "workspace_url": DOGFOOD_URL,
    "access_token": TOKEN
  }
})

# COMMAND ----------

# Test the Job webhook
httpClient.testWebhook({
  "id": job_webhook['webhook']['id']
})

# COMMAND ----------

# Create a webhook
http_webhook = httpClient.createWebhook({
  "model_name": model_name,
  "events": ["TRANSITION_REQUEST_CREATED", "MODEL_VERSION_CREATED", "MODEL_VERSION_TRANSITIONED_STAGE"],
  "status": "ACTIVE",
  "http_url_spec": {"url": slack_url}
})

# Print the created webhook details
print(http_webhook)

# COMMAND ----------

# Test the HTTP webhook
httpClient.testWebhook({
  "id": http_webhook['webhook']['id']
})

# COMMAND ----------

# List all webhooks
webhooks = httpClient.listWebhooks({
  "events": ["MODEL_VERSION_CREATED", "MODEL_VERSION_TRANSITIONED_STAGE", "TRANSITION_REQUEST_CREATED"],
})
webhooks

# COMMAND ----------

# MAGIC %md ## Create a transition request to trigger webhooks and then clean up webhooks

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

# Create a transition request to staging and then approve the request
transition_request_body = {'name': model_name, 'version': 4, 'stage': 'Staging'}
mlflow_call_endpoint('transition-requests/create', 'POST', json.dumps(transition_request_body))


# COMMAND ----------

transition_request_body = {'name': model_name, 'version': 7, 'stage': 'Staging', 'archive_existing_versions': 'true'}
mlflow_call_endpoint('transition-requests/approve', 'POST', json.dumps(transition_request_body))

# COMMAND ----------

# Delete all webhooks
for webhook in webhooks['webhooks']:
  httpClient.deleteWebhook({"id": webhook['id']})

# COMMAND ----------

# Verify webhook deletion
webhooks = httpClient.listWebhooks({
  "events": ["MODEL_VERSION_CREATED", "MODEL_VERSION_TRANSITIONED_STAGE", "TRANSITION_REQUEST_CREATED"],
})
webhooks
