# Databricks notebook source
# MAGIC %md
# MAGIC MLOPS DEMO 

# COMMAND ----------

# MAGIC %md
# MAGIC Use Case
# MAGIC

# COMMAND ----------

# Read into Spark from Delta
df = spark.table("default.student_data_mlops_demo")
display(df)

# COMMAND ----------

#Using Pandas on spark
#Featurization function 

from databricks.feature_store import feature_table
import pyspark.pandas as ps
 
def features(data):
  
  # Convert to a dataframe compatible with the pandas API
  data = data.pandas_api()
  


  return data

# COMMAND ----------

#Write to Feature Store (Optional)

from databricks.feature_store import FeatureStoreClient
fs = FeatureStoreClient()

df_spark = features(df)
# Initialize Feature Store Client


# Try to drop the table if it exists
try:
    fs.drop_table(f'default.dbdemos_mlops_features')
except Exception as e:
    print(f"Error dropping table: {e}")

# Create the Feature Store table
feature_table = fs.create_table(
    name=f'default.dbdemos_mlops_features',
    primary_keys=['Student_id'],  # Use a list for primary keys
    schema=df_spark.spark.schema(),
    description='Derived features.'
)

# Write the DataFrame to the Feature Store table
fs.write_table(df=df_spark.to_spark(), name=f'default.dbdemos_mlops_features', mode='overwrite')
