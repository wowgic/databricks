# Databricks notebook source
# MAGIC %md
# MAGIC MLOPS DEMO 

# COMMAND ----------

# MAGIC %md
# MAGIC Use Case
# MAGIC

# COMMAND ----------

# Read into Spark from Delta
df = spark.table("default.mlops_demo_2")
display(df)

# COMMAND ----------

#Using Pandas on spark
#Featurization function 

from databricks.feature_store import feature_table
import pyspark.pandas as ps
 
def features(data):
  
  # Convert to a dataframe compatible with the pandas API
  data = data.pandas_api()
  
  # Convert label to int and rename column
  data['Status'] = data['Status'].map({'Pass': 1, 'Fail': 0})
  data = data.astype({'Status': 'int32'})

  return data

# COMMAND ----------

#Write to Feature Store (Optional)

from databricks.feature_store import FeatureStoreClient
 
fs = FeatureStoreClient()
 
features_df = features(df)
display(features_df )
try:
  #drop table if exists
  fs.drop_table(f'default.dbdemos_mlops_features')
except:
  pass
#Note: You might need to delete the FS table using the UI
churn_feature_table = fs.create_table(
  name=f'default.dbdemos_mlops_features',
  primary_keys='Student_id',
  schema=features_df.spark.schema(),
  description='features are defined.'
)
 
fs.write_table(df=features_df.to_spark(), name=f'default.dbdemos_mlops_features', mode='overwrite')

# COMMAND ----------

force_refresh = dbutils.widgets.get("force_refresh_automl") == "true"
display_automl_churn_link(f'{dbName}.dbdemos_mlops_features', force_refresh = force_refresh)
