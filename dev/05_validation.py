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
