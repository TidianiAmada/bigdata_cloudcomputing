import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, sum as _sum, when

spark = SparkSession.builder.appName("AtelierSparkDF").getOrCreate()

schema = "pdate DATE, ptime STRING, store STRING, product STRING, cost DOUBLE, payment STRING"
df = spark.read.csv("hdfs://namenode:8020/data/raw/purchases.txt", sep="\t", schema=schema)
df.printSchema()
df.show(5)

# Préchauffage (lecture + count, hors chronométrage des questions)
print("TOTAL_ROWS:", df.count())

# Question 1 — montant total des ventes par magasin
t0 = time.time()
df.groupBy("store") \
  .agg(_sum("cost").alias("total_ventes")) \
  .orderBy(col("total_ventes").desc()) \
  .show()
print("Q1_TIME:", round(time.time() - t0, 2), "s")

# Question 2 — nombre de ventes par tranche horaire
t0 = time.time()
df.withColumn("heure", substring("ptime", 1, 2)) \
  .groupBy("heure") \
  .count() \
  .orderBy("heure") \
  .show(24)
print("Q2_TIME:", round(time.time() - t0, 2), "s")

# Question 3 — montant total par mode de paiement
t0 = time.time()
df.groupBy("payment") \
  .agg(_sum("cost").alias("montant_total")) \
  .orderBy(col("montant_total").desc()) \
  .show()
print("Q3_TIME:", round(time.time() - t0, 2), "s")

# Question 4 — taux de paiement cash / électronique
t0 = time.time()
nb_total = df.count()
df.withColumn("categorie", when(col("payment") == "Cash", "cash").otherwise("electronique")) \
  .groupBy("categorie") \
  .count() \
  .withColumn("taux_pct", (col("count") / nb_total * 100)) \
  .show()
print("Q4_TIME:", round(time.time() - t0, 2), "s")

spark.stop()
