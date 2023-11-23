# Databricks notebook source
# MAGIC %md
# MAGIC <b>Demo on Scaling and Automating MLOPS Workflow using Databricks </b>

# COMMAND ----------

# MAGIC %md
# MAGIC Pre-Requisites
# MAGIC
# MAGIC 1. Azure Databricks free trial is enabled https://www.databricks.com/try-databricks?scid=7018Y000001Fi1AQAS&utm_medium=paid+search&utm_source=google&utm_campaign=17883932635&utm_adgroup=142511579434&utm_content=trial&utm_offer=try-databricks&utm_ad=666090612860&utm_term=azure%20databricks%20free%20trial&gad_source=1&gclid=Cj0KCQiA6vaqBhCbARIsACF9M6nmOFI2-RUdstTFYCZDuKXCxdfHkb_dRdj0_o0cmYI4zGwUVotu7rkaAn9oEALw_wcB#account
# MAGIC 2. Databricks Repository set up and Git Databricks link account https://docs.databricks.com/en/repos/get-access-tokens-from-git-provider.html#:~:text=In%20Databricks%2C%20link%20your%20GitHub,Git%20account%2C%20and%20click%20Link.
# MAGIC 3. Slack App API- Activate Incoming webhooks https://api.slack.com/messaging/webhooks
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <u>Model & Code deployment has two approaches</u>
# MAGIC
# MAGIC 1. Deploy Code (Recommended) 
# MAGIC 2. Deploy Model
# MAGIC
# MAGIC Roles
# MAGIC 1. Data Scientist
# MAGIC 2. Data Engineer
# MAGIC 3. Ml Ops Engineer

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Dataset</b>
# MAGIC The dataset comprises scores reflecting students' participation and performance, determining whether they pass or fail. This augmented, cleaned dataset is tailored for the specific MLOps workflow. It's important to note that exploratory data analysis (EDA), feature engineering, and data cleaning, integral parts of the machine learning pipeline, are not covered here. However, detailed resources on these topics can be readily explored in Databricks documentation and videos.
# MAGIC
# MAGIC Dataset consists of Student id - String, Participation score - Double, Project score - Duble , adnd satuts- String.
# MAGIC
# MAGIC <b>Predict</b>
# MAGIC
# MAGIC We will be categorizing students as either Pass or Fail based on their participation and performance. While there are additional parameters influencing the pass/fail prediction, for the sake of simplicity and a focused approach on MLOps workflow using AutoML, we will concentrate on these specific aspects.
# MAGIC

# COMMAND ----------

#Create a Delta Table - Upload csv -  Data Injestion->create/modify table-> Drag & Drop
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

# COMMAND ----------

# MAGIC %md
# MAGIC Feature Store its optional but useful when multiple teams work on same features and tagging model meta data later for validation
