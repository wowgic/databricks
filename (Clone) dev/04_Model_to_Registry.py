# Databricks notebook source
#Let's get our last auto ml run. This is specific to the demo, it just gets the experiment ID of the last Auto ML run.
experiment_id = '802245372688280'
 
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
