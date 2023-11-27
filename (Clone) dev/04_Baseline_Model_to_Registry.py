# Databricks notebook source
# MAGIC %md
# MAGIC <b>Building Baseline Model using Auto-ML</b> 
# MAGIC https://docs.databricks.com/en/machine-learning/automl/index.html
# MAGIC
# MAGIC 1. Create an Experiment
# MAGIC 2. Experiment Configuration
# MAGIC 3. Start Run
# MAGIC
# MAGIC Fetch :
# MAGIC
# MAGIC >experiment id
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

#Let's get our last auto ml run. This is specific to the demo, it just gets the experiment ID of the last Auto ML run.
experiment_id = '2628671495089050'
 
best_model = mlflow.search_runs(experiment_ids=[experiment_id], order_by=["metrics.val_f1_score DESC"], max_results=1, filter_string="status = 'FINISHED'")
best_model

# COMMAND ----------

run_id = best_model.iloc[0]['run_id']
 
#add some tags that we'll reuse later to validate the model
client = mlflow.tracking.MlflowClient()
client.set_tag(run_id, key='performance', value='participation,Project')
client.set_tag(run_id, key='db_table', value=f'default.dbdemos_mlops_features')
 
#Deploy our autoML run in MLFlow registry
model_details = mlflow.register_model(f"runs:/{run_id}/model", "mlops_demo_predict_stu_per")


# COMMAND ----------

print(model_details)
print(model_details.version)

# COMMAND ----------

# Create a transition request to staging
transition_request_body = {'name': model_name, 'version': model_details.version , 'stage': 'Staging'}
mlflow_call_endpoint('transition-requests/create', 'POST', json.dumps(transition_request_body))

print(model_details.version)

