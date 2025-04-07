# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "fc271c1a-7ba0-440c-8ec2-0b6d318dbcc7",
"fs.azure.account.oauth2.client.secret": '0YQ8Q~qd3qkMyh.AoMaVOt._5hSYQksrlLCaAccK',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/0a87b18f-d41f-4a49-9e08-3eede1b57ba4/oauth2/token"}


dbutils.fs.mount(
source = "abfss://tokyoolympicsdata@tokyoolympicdata911.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/tokyoolympic",
extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/tokyoolympic"

# COMMAND ----------

spark

# COMMAND ----------

athletes = spark.read.format("csv").load("/mnt/tokyoolympic/raw-data/athletes.csv", header='true')
coaches = spark.read.format("csv").load("/mnt/tokyoolympic/raw-data/coaches.csv", header='true')
Entriesgender = spark.read.format("csv").load("/mnt/tokyoolympic/raw-data/Entriesgender.csv", header='true')
medals = spark.read.format("csv").load("/mnt/tokyoolympic/raw-data/medals.csv", header='true')
teams = spark.read.format("csv").load("/mnt/tokyoolympic/raw-data/teams.csv", header='true')

# COMMAND ----------

coaches.show()

# COMMAND ----------

Entriesgender = Entriesgender.withColumn("Female", col("Female").cast(IntegerType())) \
    .withColumn("Male", col("Male").cast(IntegerType())) \
    .withColumn("Total", col("Total").cast(IntegerType()))

# COMMAND ----------

top_gold_medal_countries = medals.orderBy("Gold", ascending=False).select("TeamCountry","Gold").show()

# COMMAND ----------

athletes.write.option("header", "true").mode("overwrite").csv("/mnt/tokyoolympic/transformed-data/athletes")
coaches.write.option("header", "true").mode("overwrite").csv("/mnt/tokyoolympic/transformed-data/coaches")
Entriesgender.write.option("header", "true").mode("overwrite").csv("/mnt/tokyoolympic/transformed-data/Entriesgender")
medals.write.option("header", "true").mode("overwrite").csv("/mnt/tokyoolympic/transformed-data/medals")
teams.write.option("header", "true").mode("overwrite").csv("/mnt/tokyoolympic/transformed-data/teams")