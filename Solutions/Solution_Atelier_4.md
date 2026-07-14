# Solution — Atelier 4 : Traitement distribué avec Apache Spark

Corrigé de l'atelier pratique (parties 3 et 4 de [Atelier_4_Apache_Spark_PySpark.md](../Atelier_4_Apache_Spark_PySpark.md)), exécuté réellement avec `spark-submit` dans le conteneur `spark-master` de la plateforme Docker de l'[Atelier 3](Solution_Atelier_3.md), sur l'intégralité de `purchases.txt` (4 138 476 lignes, montées sur `/data/purchases.txt`).

> Remarque : les scripts ci-dessous sont exécutés avec `spark-submit --master local[*]` plutôt que collés dans le shell interactif `pyspark`, pour pouvoir chronométrer chaque question précisément et obtenir une sortie reproductible — le code Spark (RDD/DataFrame) est strictement identique à celui de l'énoncé.

---

## Partie A — API RDD

```python
purchases = sc.textFile("/data/purchases.txt")
parsed = purchases.map(lambda line: line.split("\t"))
parsed.first()
```

```text
FIRST_ROW: ['2012-01-01', '09:00', 'San Jose', "Men's Clothing", '214.05', 'Amex']
TOTAL_ROWS: 4138476
```

### Question 1 — Montant total des ventes par magasin

```python
sales_by_store = parsed.map(lambda f: (f[2], float(f[4]))) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False)
sales_by_store.collect()
```

**103 magasins** au total. Top 10 :

| Rang | Magasin | Total des ventes |
|---|---|---|
| 1 | Philadelphia | 10 190 080,26 $ |
| 2 | Durham | 10 153 890,21 $ |
| 3 | Laredo | 10 144 604,98 $ |
| 4 | Newark | 10 144 052,80 $ |
| 5 | Cincinnati | 10 139 505,74 $ |
| 6 | Washington | 10 139 363,39 $ |
| 7 | Irving | 10 133 944,08 $ |
| 8 | Fort Wayne | 10 132 594,02 $ |
| 9 | Baton Rouge | 10 131 273,23 $ |
| 10 | Sacramento | 10 123 468,18 $ |

Temps d'exécution : **2,46 s**.

→ Remarque : les totaux sont très proches les uns des autres (~10,1 à 10,2 millions), ce qui suggère un jeu de données pédagogique généré avec une distribution volontairement homogène entre magasins plutôt qu'un vrai déséquilibre commercial.

### Question 2 — Nombre de ventes par tranche horaire

```python
sales_by_hour = parsed.map(lambda f: (f[1][:2], 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortByKey()
sales_by_hour.collect()
```

| Heure | Nb ventes |
|---|---|
| 09 | 459 672 |
| 10 | 459 886 |
| 11 | 459 332 |
| 12 | 459 906 |
| 13 | 459 772 |
| 14 | 459 873 |
| 15 | 459 531 |
| 16 | 460 466 |
| 17 | 460 038 |

Temps d'exécution : **3,71 s**.

→ Le magasin n'ouvre visiblement qu'entre 9h et 17h (9 tranches horaires seulement) avec un volume quasiment constant (~459-460k ventes/heure) — cohérent avec un jeu de données simulé plutôt qu'un vrai flux réel (où l'on attendrait des pics à l'heure du déjeuner ou en fin de journée).

### Question 3 — Montant total des achats par mode de paiement

```python
amount_by_payment = parsed.map(lambda f: (f[5], float(f[4]))) \
    .reduceByKey(lambda a, b: a + b)
amount_by_payment.collect()
```

| Moyen de paiement | Montant total |
|---|---|
| Cash | 207 245 078,69 $ |
| MasterCard | 207 011 524,35 $ |
| Discover | 206 869 621,48 $ |
| Visa | 206 703 361,97 $ |
| Amex | 206 628 366,77 $ |

Temps d'exécution : **2,35 s**.

→ Répartition quasi parfaitement équilibrée entre les 5 moyens de paiement (moins de 0,3 % d'écart entre le plus haut et le plus bas).

### Question 4 — Taux de paiement cash / électronique

```python
total = parsed.count()
taux_paiement = parsed.map(lambda f: ("cash" if f[5].lower()=="cash" else "electronique", 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .mapValues(lambda nb: round(nb/total*100, 2))
taux_paiement.collect()
```

```text
[('cash', 20.03), ('electronique', 79.97)]
```

Temps d'exécution : **3,54 s** — **temps total Partie A (lecture + 4 questions) : 20,42 s**.

→ 1 achat sur 5 seulement est réglé en espèces ; cohérent avec 5 moyens de paiement équiprobables (1/5 = 20 % attendu pour le cash).

---

## Partie B — API DataFrame

```python
schema = "pdate DATE, ptime STRING, store STRING, product STRING, cost DOUBLE, payment STRING"
df = spark.read.csv("/data/purchases.txt", sep="\t", schema=schema)
df.printSchema()
```

```text
root
 |-- pdate: date (nullable = true)
 |-- ptime: string (nullable = true)
 |-- store: string (nullable = true)
 |-- product: string (nullable = true)
 |-- cost: double (nullable = true)
 |-- payment: string (nullable = true)
```

### Résultats

Les quatre questions produisent des résultats **rigoureusement identiques** à la Partie A (aux erreurs d'arrondi flottant près, de l'ordre de 10⁻⁵, dues à l'ordre de sommation différent entre les deux moteurs d'exécution) :

| Question | Résultat DataFrame | Temps DataFrame | Temps RDD (Partie A) |
|---|---|---|---|
| Q1 (top magasin) | Philadelphia — 10 190 080,26 $ | **1,56 s** | 2,46 s |
| Q2 (tranche 09h) | 459 672 ventes | **0,81 s** | 3,71 s |
| Q3 (Cash) | 207 245 078,69 $ | **0,57 s** | 2,35 s |
| Q4 (cash / électronique) | 828 770 (20,03 %) / 3 309 706 (79,97 %) | **0,60 s** | 3,54 s |
| **Total (lecture + 4 questions)** | | **13,62 s** | **20,42 s** |

---

## Comparaison RDD vs DataFrame (consignes 2 et 3)

### Lisibilité et longueur du code

| | RDD | DataFrame |
|---|---|---|
| Q1 | `parsed.map(lambda f: (f[2], float(f[4]))).reduceByKey(...).sortBy(...)` — manipulation manuelle de tuples, indices positionnels (`f[2]`, `f[4]`) | `df.groupBy("store").agg(_sum("cost")).orderBy(...)` — noms de colonnes explicites, proche du SQL |
| Q4 | Fonction Python dédiée (`categorie_paiement`) + `mapValues` | `when(...).otherwise(...)` intégré à l'expression Spark SQL |

Le code DataFrame est systématiquement plus court, plus lisible (colonnes nommées au lieu d'indices `f[2]`/`f[4]`), et plus proche du vocabulaire métier (`groupBy`, `agg`, `orderBy` évoquent directement un `GROUP BY` SQL).

### Temps d'exécution

Sur ce même jeu de données et les mêmes 4 questions, le DataFrame est **environ 1,5x plus rapide** au global (13,6 s contre 20,4 s), et jusqu'à **4,5x plus rapide** sur certaines questions prises isolément (Q2 : 0,81 s contre 3,71 s). Cet écart illustre concrètement l'intérêt de l'optimiseur **Catalyst** : contrairement à l'API RDD où chaque transformation `map`/`reduceByKey` s'exécute telle qu'écrite, l'API DataFrame construit un plan logique que Spark réorganise et optimise avant exécution (élagage de colonnes, choix du type d'agrégation, etc.), sans effort supplémentaire côté développeur.

### Extension — inversion clé/valeur avec `sortByKey()`

```python
inverted = parsed.map(lambda f: (float(f[4]), f[2])).sortByKey(ascending=False)
inverted.take(5)
```

```text
[(499.99, 'Birmingham'), (499.99, 'San Jose'), (499.99, 'Mesa'), (499.99, 'Baltimore'), (499.99, 'Chula Vista')]
```

**Point pédagogique important** : cette variante ne répond pas à la même question que Q1. Ici, la clé devient le **montant d'un ticket individuel** (`cost`), pas le total agrégé par magasin — `sortByKey()` trie donc les tickets un par un, et révèle que le montant maximal d'un ticket dans ce jeu de données plafonne à **499,99 $** (plusieurs magasins différents atteignent ce même plafond, signe d'une borne supérieure fixée lors de la génération du jeu de données). Pour retrouver le résultat de Q1 avec cette technique, il faudrait d'abord agréger (`reduceByKey`) **avant** d'inverser clé et valeur.

---

## Synthèse

- Sur `purchases.txt` (4,1 millions de lignes), les 4 questions métier donnent des résultats identiques entre RDD et DataFrame, ce qui confirme que le choix d'API n'est pas un compromis sur la correction du résultat mais bien sur l'ergonomie et la performance.
- Le DataFrame l'emporte sur les deux plans dans cet atelier : code plus court et plus lisible, et environ 1,5x plus rapide grâce à l'optimiseur Catalyst — d'où la recommandation de la synthèse de l'atelier de préférer le DataFrame en usage général.
- Le classement des magasins (top 3 : Philadelphia, Durham, Laredo) servira de référence de comparaison à l'Atelier 5 (Spark SQL / Hive sur le même jeu de données).
- Piège identifié avec `sortByKey()` après inversion clé/valeur : trier sur une valeur individuelle (le prix d'un ticket) n'est pas équivalent à trier sur un total agrégé (le total des ventes par magasin) — l'ordre des opérations (agréger puis trier, ou l'inverse) change complètement la question à laquelle on répond.
