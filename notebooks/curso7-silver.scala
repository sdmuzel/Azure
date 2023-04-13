// Databricks notebook source
// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt/dados")

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt/dados/bronze")

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt/dados/bronze/zap_imoveis")

// COMMAND ----------

val path="dbfs:/mnt/dados/bronze/zap_imoveis"
val df = spark.read.format("delta").load(path)

// COMMAND ----------

display (df)

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

val df2= spark.read.json(df.select($"value").as[String])

// COMMAND ----------

display(df2)

// COMMAND ----------

val columnNames: Array[String] = df2.columns

// Imprimir os nomes das colunas
columnNames.foreach(println)

// COMMAND ----------

columnNames

// COMMAND ----------

//expr() é uma função usada para criar uma expressão SQL em Scala
//o * é o comando SQL que seleciona tudo, como em SELECT *, que traz todas as colunas de uma tabela.
//Então expr("*") pode ser usada para selecionar todas as colunas em um DataFrame.


val df_silver = df2.select(
                          expr("*"),
                          $"address.*",
                          $"address.geoLocation.location.*",
                          $"pricingInfos.*")

display(df_silver)

// COMMAND ----------

val df_silver = df2.select(
                          expr("*"),
                          $"address.*",
                          $"address.geoLocation.location.*",
                          $"pricingInfos.*")
                         .drop(
                           "address","images","pricingInfos","geoLocation"
                         )

display(df_silver)

// COMMAND ----------



// COMMAND ----------

val dbfs_path = "dbfs:/mnt/dados/silver/zap_imoveis"
df_silver.write.format("delta").save(dbfs_path)

// COMMAND ----------


