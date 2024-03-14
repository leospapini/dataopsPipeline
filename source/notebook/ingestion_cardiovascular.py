# Databricks notebook source
# MAGIC %md
# MAGIC # INGESTÃO DE DADOS ELT
# MAGIC
# MAGIC conjunto de dados de previsão de risco de doenças cardiovasculares

# COMMAND ----------

display(dbutils.fs)

# COMMAND ----------

display(dbutils.fs.ls("/"))

# COMMAND ----------

dbutils.fs.mkdirs("/tmp/")

# COMMAND ----------

display(dbutils.fs.ls("/"))

# COMMAND ----------

display(dbutils.fs.ls("/tmp/"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Extraindo Dados/Realizando a leitura

# COMMAND ----------

df = spark.read.format("csv").option("header", True).load("/tmp/cardiovascular-diseases-risk.csv")

# COMMAND ----------

df.display()

# COMMAND ----------

df.select("General_Health").distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Rename Cols

# COMMAND ----------

df = df.withColumnRenamed("Height_(cm)", "Height_cm").withColumnRenamed("Weight_(kg)", "Weight_kg")


# COMMAND ----------

# MAGIC %md
# MAGIC # Realizando o armazenamento de dados

# COMMAND ----------

df.write.format("delta").mode("overwrite").option("mergeSchema", True).partitionBy("General_Health").save("/hospital/rw/cardiovascular/")

# COMMAND ----------

# MAGIC %md
# MAGIC # Criando um Data Base e tabela pelo delta location

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS db_hospital

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS db_hospital.cardiovascular_diseases LOCATION "/hospital/rw/cardiovascular/"

# COMMAND ----------

spark.sql("CREATE DATABASE db_hospital")

# COMMAND ----------

df.write.format("delta").saveAsTable("db_hospital.cardiovascular_diseases")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM db_hospital.cardiovascular_diseases

# COMMAND ----------


