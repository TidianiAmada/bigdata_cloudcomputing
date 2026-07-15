# Atelier 4 — Traitement distribué avec Apache Spark

**Module :** Introduction au Big Data et au Cloud Computing

**Formation :** Licence Informatique 2 — SupDeCo

**Enseignant :** M. TOP

**Durée :** 3 heures

---

## Objectifs de l'atelier

- comprendre les limites du traitement classique (mono-machine) face à de gros volumes ;
- comprendre l'architecture générale d'Apache Spark (Driver, Executors, cluster manager) ;
- distinguer RDD et DataFrame et savoir passer de l'un à l'autre ;
- écrire, sur le jeu de données fil rouge `purchases.txt`, les mêmes traitements d'abord avec l'API **RDD**, puis avec l'API **DataFrame**, afin de comparer les deux approches.

> Cet atelier reprend, avec Spark, exactement les traitements réalisés avec Hive à l'Atelier 5 — l'objectif est de mesurer, sur un même jeu de données et un même besoin métier, ce qu'apportent différents niveaux d'abstraction (RDD bas niveau, DataFrame, puis SQL).

---

## 1. Rappel théorique (30 min)

### 1.1 Limites du traitement classique

Un script Python classique (pandas, par exemple) charge l'intégralité des données en mémoire sur **une seule machine**. Cette approche atteint rapidement ses limites :

- la RAM disponible plafonne le volume de données traitable ;
- le traitement est **séquentiel** : une seule unité de calcul traite l'ensemble des données, sans parallélisme réel ;
- en cas de panne pendant le traitement, tout est à recommencer.

### 1.2 Du paradigme MapReduce à Spark

Le paradigme **MapReduce**, popularisé par Hadoop, a été la première réponse largement adoptée à ce problème : il découpe un traitement en deux phases enchaînées sur le cluster — **Map** (transformation ligne à ligne, en parallèle sur chaque partition de données) et **Reduce** (agrégation des résultats intermédiaires regroupés par clé). Entre chaque étape Map/Reduce, les résultats intermédiaires sont écrits sur disque (HDFS), ce qui garantit la tolérance aux pannes mais pénalise fortement les traitements enchaînant plusieurs étapes.

Apache Spark reprend cette même logique de distribution (données + calcul répartis sur le cluster, tolérance aux pannes) mais privilégie le traitement **en mémoire** entre les étapes, ce qui le rend nettement plus rapide que le MapReduce historique — en particulier pour les traitements itératifs ou enchaînant plusieurs transformations, comme ceux réalisés dans cet atelier.

### 1.3 Architecture Spark

```text
                ┌───────────────┐
                │     Driver     │  ← programme principal, planifie les tâches
                └───────┬───────┘
                        │
                ┌───────┴────────┐
                │ Cluster Manager │  ← alloue les ressources (Standalone, YARN, Kubernetes)
                └───────┬────────┘
        ┌───────────────┼───────────────┐
        │               │               │
  ┌──────────┐    ┌──────────┐    ┌──────────┐
  │ Executor 1│    │ Executor 2│    │ Executor 3│   ← exécutent les tâches en parallèle
  └──────────┘    └──────────┘    └──────────┘
```

- **Driver** : processus qui exécute le code principal (`main`), construit le plan d'exécution et distribue les tâches.
- **Executors** : processus déployés sur les nœuds du cluster, qui exécutent réellement les tâches et stockent les données en mémoire ou sur disque.
- **Cluster Manager** : composant responsable de l'allocation des ressources (CPU, mémoire) aux executors. Dans notre environnement Docker, on utilise le mode **Standalone** ; sur Amazon EMR (Atelier 7), on retrouvera Spark piloté par **YARN**.

### 1.4 RDD (Resilient Distributed Dataset)

Le RDD est la structure de données fondamentale historique de Spark : une collection d'éléments répartie sur les nœuds du cluster, immuable, et **résiliente** (capable de se reconstruire automatiquement en cas de perte d'une partition, grâce à son lignage d'opérations). C'est l'API la plus proche du modèle Map/Reduce : on manipule explicitement des paires clé-valeur avec des opérations comme `map`, `filter`, `reduceByKey`.

```python
rdd = sc.parallelize([1, 2, 3, 4, 5])
rdd_carre = rdd.map(lambda x: x * x)
print(rdd_carre.collect())  # [1, 4, 9, 16, 25]
```

### 1.5 DataFrame

Le DataFrame est une abstraction de plus haut niveau : une collection de données organisée en colonnes nommées et typées, comparable à une table SQL — mais distribuée sur le cluster. Spark optimise automatiquement l'exécution des opérations sur DataFrame grâce à son moteur d'optimisation (Catalyst), ce qui le rend en général au moins aussi performant qu'un traitement RDD écrit à la main, pour un code plus concis.

| | RDD | DataFrame |
|---|---|---|
| Niveau d'abstraction | Bas niveau, proche MapReduce | Haut niveau (proche SQL) |
| Optimisation automatique | Non | Oui (Catalyst) |
| Facilité d'usage | Plus verbeux | Plus concis |
| Usage recommandé | Cas spécifiques, contrôle fin | Usage général (recommandé) |

Dans cet atelier, les deux API sont mises en œuvre sur le **même jeu de données et les mêmes questions**, afin d'observer concrètement cet écart de niveau d'abstraction.

### 1.6 Commandes et API à connaître

**Lancement (ligne de commande) :**

| Commande | Rôle |
|---|---|
| `pyspark` | Ouvrir un shell interactif Python avec Spark déjà initialisé (variables `sc` et `spark` disponibles) |
| `spark-submit <script.py>` | Exécuter un script Spark de façon non interactive (le mode utilisé en production, et pour chronométrer un traitement) |
| `spark-submit --master local[*] <script.py>` | Idem, en forçant l'exécution locale en utilisant tous les cœurs disponibles (`*`) |

**API RDD (bas niveau, clé-valeur) :**

| Méthode | Rôle |
|---|---|
| `sc.textFile("chemin")` | Charger un fichier texte en RDD (une ligne = un élément) |
| `sc.parallelize([...])` | Créer un RDD à partir d'une collection Python en mémoire |
| `.map(fonction)` | Transformer chaque élément (1 entrée → 1 sortie) |
| `.filter(fonction)` | Ne garder que les éléments vérifiant une condition |
| `.reduceByKey(fonction)` | Agréger les valeurs par clé (équivalent d'un `GROUP BY` + agrégat) |
| `.sortBy(fonction)` / `.sortByKey()` | Trier un RDD selon une fonction, ou selon sa clé |
| `.collect()` | Rapatrier tous les résultats vers le driver (⚠️ à éviter sur un très gros RDD non agrégé) |
| `.take(n)` | Rapatrier seulement les `n` premiers éléments — alternative sûre à `.collect()` |
| `.count()` | Compter les éléments du RDD |
| `.cache()` | Garder le RDD en mémoire entre plusieurs actions, pour éviter de relire/reparser les données à chaque fois |

**API DataFrame (haut niveau, proche SQL) :**

| Méthode | Rôle |
|---|---|
| `spark.read.csv("chemin", sep="\t", schema="...")` | Charger un fichier délimité dans un DataFrame, avec un schéma explicite |
| `.printSchema()` | Afficher les colonnes et leurs types |
| `.show(n)` | Afficher les `n` premières lignes sous forme de tableau |
| `.groupBy("colonne")` | Regrouper par valeur de colonne |
| `.agg(_sum("colonne"), ...)` | Calculer un ou plusieurs agrégats (`sum`, `avg`, `count`, `min`, `max`) sur les groupes |
| `.orderBy(col("colonne").desc())` | Trier (`.desc()` pour un tri décroissant) |
| `.withColumn("nouvelle_colonne", expression)` | Ajouter une colonne calculée (ex. `substring`, `when(...).otherwise(...)`) |
| `.filter(condition)` / `.where(condition)` | Filtrer les lignes |

**Point clé à retenir** : `.collect()` et `.take(n)` sont les seules opérations qui rapatrient réellement des données vers le driver — toutes les autres (`.map`, `.filter`, `.groupBy`, `.agg`...) ne font que décrire des transformations, évaluées **paresseusement** (*lazy evaluation*) et exécutées uniquement au moment où une action (`.collect()`, `.count()`, `.show()`...) est appelée.

---

## 2. Le jeu de données : `purchases.txt`

Fichier texte, valeurs séparées par des **tabulations**, sans ligne d'en-tête, une ligne = un ticket d'achat :

| Position | Champ | Type | Exemple |
|---|---|---|---|
| 0 | `pdate` | date | 2012-01-01 |
| 1 | `ptime` | heure (`HH:MM`) | 09:00 |
| 2 | `store` | chaîne | San Jose |
| 3 | `product` | chaîne | Men's T-Shirt |
| 4 | `cost` | flottant | 16.98 |
| 5 | `payment` | chaîne | Cash / Visa / MasterCard / Amex / Discover |

Le fichier doit être accessible depuis le conteneur Spark avant de démarrer l'atelier :

- **En local (plateforme Docker de l'Atelier 3)** : placer `purchases.txt` dans un dossier monté en volume (par exemple `./data:/data` ajouté au service `spark-master` du `docker-compose.yml`), puis y accéder via `/data/purchases.txt`.
- **Sur Amazon EMR (Atelier 7)** : déposer le fichier sur HDFS :
  ```bash
  hdfs dfs -put purchases.txt /user/hadoop/purchases.txt
  ```

Dans les exemples de code ci-dessous, le chemin `"purchases.txt"` est à adapter selon l'environnement (`/data/purchases.txt` en local, ou chemin HDFS/S3 sur EMR).

---

## 3. Partie A — Traitements avec les RDD (shell `pyspark`)

Lancement du shell interactif :

```bash
pyspark
```

### 3.1 Lecture et parsing

```python
purchases = sc.textFile("purchases.txt")

def parse_line(line):
    return line.split("\t")   # [pdate, ptime, store, product, cost, payment]

parsed = purchases.map(parse_line)
parsed.first()
```

### 3.2 Question 1 — Montant total des ventes par magasin, trié par meilleures ventes

```python
sales_by_store = parsed.map(lambda f: (f[2], float(f[4]))) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False)

sales_by_store.collect()
```

### 3.3 Question 2 — Nombre total de ventes par tranche horaire (00, 01, …, 23)

```python
sales_by_hour = parsed.map(lambda f: (f[1][:2], 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortByKey()

sales_by_hour.collect()
```

### 3.4 Question 3 — Montant total des achats par mode de paiement

```python
amount_by_payment = parsed.map(lambda f: (f[5], float(f[4]))) \
    .reduceByKey(lambda a, b: a + b)

amount_by_payment.collect()
```

### 3.5 Question 4 — Taux de paiement par cash et par paiement électronique

```python
def categorie_paiement(mode):
    return "cash" if mode.lower() == "cash" else "electronique"

total = parsed.count()

taux_paiement = parsed.map(lambda f: (categorie_paiement(f[5]), 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .mapValues(lambda nb: round(nb / total * 100, 2))

taux_paiement.collect()
```

---

## 4. Partie B — Mêmes traitements avec l'API DataFrame (Zeppelin ou notebook)

### 4.1 Lecture avec un schéma explicite

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, sum as _sum, count, when

spark = SparkSession.builder.appName("AtelierSparkDF").getOrCreate()

schema = "pdate DATE, ptime STRING, store STRING, product STRING, cost DOUBLE, payment STRING"

df = spark.read.csv("purchases.txt", sep="\t", schema=schema)
df.printSchema()
df.show(5)
```

### 4.2 Question 1 — Montant total des ventes par magasin

```python
df.groupBy("store") \
  .agg(_sum("cost").alias("total_ventes")) \
  .orderBy(col("total_ventes").desc()) \
  .show()
```

### 4.3 Question 2 — Nombre de ventes par tranche horaire

```python
df.withColumn("heure", substring("ptime", 1, 2)) \
  .groupBy("heure") \
  .count() \
  .orderBy("heure") \
  .show(24)
```

### 4.4 Question 3 — Montant total par mode de paiement

```python
df.groupBy("payment") \
  .agg(_sum("cost").alias("montant_total")) \
  .orderBy(col("montant_total").desc()) \
  .show()
```

### 4.5 Question 4 — Taux de paiement cash / électronique

```python
nb_total = df.count()

df.withColumn("categorie", when(col("payment") == "Cash", "cash").otherwise("electronique")) \
  .groupBy("categorie") \
  .count() \
  .withColumn("taux_pct", (col("count") / nb_total * 100)) \
  .show()
```

### Consignes

1. Exécuter les questions 1 à 4 en RDD (Partie A), puis en DataFrame (Partie B).
2. Comparer la longueur et la lisibilité du code entre les deux approches.
3. Comparer le temps d'exécution (`%time` dans le shell, ou l'onglet *Jobs* de l'interface Spark) entre RDD et DataFrame sur la même question.
4. Noter les résultats de la question 1 (top 3 magasins) — ils serviront de référence de comparaison à l'Atelier 5 (Hive et Spark SQL).

---

## 5. Synthèse

- Spark distribue données et calculs sur un cluster (Driver + Executors), en prolongeant la logique du MapReduce historique tout en travaillant autant que possible en mémoire.
- Le RDD est la structure bas niveau, proche du modèle Map/Reduce (clé-valeur, `reduceByKey`) ; le DataFrame, optimisé automatiquement par Catalyst, est la structure recommandée pour la pratique courante.
- Sur les quatre mêmes questions métier appliquées à `purchases.txt`, l'API DataFrame produit un code sensiblement plus court et plus lisible que l'API RDD, pour un résultat identique.
- Ces mêmes traitements seront repris en syntaxe SQL pure à l'Atelier 5 (Spark SQL), puis comparés à leur équivalent Hive, avant d'être exécutés à grande échelle sur Amazon EMR à l'Atelier 7.

---

## Pour aller plus loin

- Explorer l'interface web Spark (`http://localhost:8080` puis l'UI du job sur le port 4040) pour visualiser le plan d'exécution (DAG) et comparer visuellement les jobs RDD et DataFrame.
- Écrire la question 1 avec `sortByKey()` après avoir inversé clé et valeur, pour observer une autre façon de trier un RDD.
