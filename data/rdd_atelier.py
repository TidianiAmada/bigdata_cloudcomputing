from pyspark import SparkContext
import time

sc = SparkContext(appName="AtelierSparkRDD")

purchases = sc.textFile("hdfs://namenode:8020/data/raw/purchases.txt")
parsed = purchases.map(lambda line: line.split("\t")).cache()

print("FIRST_ROW:", parsed.first())
print("NUM_PARTITIONS:", parsed.getNumPartitions())
print("TOTAL_ROWS:", parsed.count())

# Question 1 — montant total des ventes par magasin
t0 = time.time()
sales_by_store = parsed.map(lambda f: (f[2], float(f[4]))) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False)
result_q1 = sales_by_store.collect()
print("Q1 (top 10):", result_q1[:10])
print("Q1_TIME:", round(time.time() - t0, 2), "s")

# Question 2 — nombre de ventes par tranche horaire
t0 = time.time()
sales_by_hour = parsed.map(lambda f: (f[1][:2], 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortByKey()
result_q2 = sales_by_hour.collect()
print("Q2:", result_q2)
print("Q2_TIME:", round(time.time() - t0, 2), "s")

# Question 3 — montant total des achats par mode de paiement
t0 = time.time()
amount_by_payment = parsed.map(lambda f: (f[5], float(f[4]))) \
    .reduceByKey(lambda a, b: a + b)
result_q3 = amount_by_payment.collect()
print("Q3:", result_q3)
print("Q3_TIME:", round(time.time() - t0, 2), "s")

# Question 4 — taux de paiement cash / électronique
t0 = time.time()
total = parsed.count()
taux_paiement = parsed.map(lambda f: ("cash" if f[5].lower() == "cash" else "electronique", 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .mapValues(lambda nb: round(nb / total * 100, 2))
result_q4 = taux_paiement.collect()
print("Q4:", result_q4)
print("Q4_TIME:", round(time.time() - t0, 2), "s")

# Extension — inversion clé/valeur avec sortByKey()
inverted = parsed.map(lambda f: (float(f[4]), f[2])).sortByKey(ascending=False)
print("INVERTED_TOP5:", inverted.take(5))

sc.stop()
