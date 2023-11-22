# Databricks notebook source
#Install Databricks webhooks utility
%pip install databricks-registry-webhooks

# COMMAND ----------

# MAGIC %md
# MAGIC Create Webhooks - 
# MAGIC Setting up webhooks is simple using the Databricks REST API. There are some helper functions in the ./_resources/API_Helpers notebook, so if you want to see additional details you can check there.

# COMMAND ----------

model_name = dbutils.widgets.get("dbdemos_mlops")

trigger_job =json.dump({
    "model_name" :

}
})


# COMMAND ----------

#DEMO SETUP
#For this demo, the job is programatically created if it doesn't exist. See ./_resources/API_Helpers for more details
job_id = get_staging_job_id()
#This should be run once. For the demo We'll reset other webhooks to prevent from duplicated call
reset_webhooks(model_name = "dbdemos_mlops")
 
#Once we have the id of the job running the tests, we add the hook:
create_job_webhook(model_name = "dbdemos_mlops", job_id = job_id)
