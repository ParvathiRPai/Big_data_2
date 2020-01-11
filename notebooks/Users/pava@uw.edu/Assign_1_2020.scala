// Databricks notebook source
import org.apache.spark.sql.functions._

// COMMAND ----------

val df =  spark.read.option("inferSchema","true").option("header","true").csv("dbfs:/autumn_2019/pava/Popular_Baby_Names.csv")

// COMMAND ----------

df.show()
df.count()

// COMMAND ----------

val result=df.groupBy("Child's First Name").agg(count("Count"))

// COMMAND ----------

result.show()
result.count()

// COMMAND ----------

val res=result.filter($"Child's First Name" === "JASON")

// COMMAND ----------

res.show()

// COMMAND ----------

