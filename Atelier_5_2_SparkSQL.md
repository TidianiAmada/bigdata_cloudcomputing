# Atelier 5.2 — Spark SQL : une quatrième façon d'interroger `purchases.txt`

**Module :** Introduction au Big Data et au Cloud Computing
**Formation :** Licence Informatique 2 — SupDeCo
**Enseignant :** M. TOP
**Durée :** 2 heures

---

## Objectifs de l'atelier

- utiliser **Spark SQL** pour réaliser, en syntaxe SQL standard, les mêmes traitements déjà écrits en RDD et en DataFrame à l'Atelier 4.2, et en HiveQL à l'Atelier 5.1 ;
- comprendre le rôle d'un métastore dans un écosystème Big Data, et son lien avec le Hive Metastore vu à l'Atelier 5.1 ;
- comparer les résultats obtenus en Spark SQL à ceux obtenus en HiveQL à l'Atelier 5.1, sur la même donnée `purchases.txt` ;
- réaliser une requête pivot pour croiser deux dimensions (magasin × mode de paiement).

> **Rappel du fil rouge.** La table Hive `achatdb.purchases` (et ses variantes Managed/External/partitionnée/bucketisée) a été créée à l'Atelier 5.1, et les traitements RDD/DataFrame ont été écrits sur `purchases.txt` à l'Atelier 4.2. Cet atelier ajoute la **quatrième** façon d'exprimer les mêmes quatre questions métier : en SQL, directement dans Spark. Il ne recrée pas de table Hive — il s'appuie sur un DataFrame chargé depuis le même fichier `purchases.txt`.

> **Environnement.** Aucun nouveau service Docker n'est nécessaire : Spark SQL n'est qu'une interface SQL au-dessus du même moteur Spark déjà utilisé à l'Atelier 4.2. Cet atelier s'exécute donc dans le même conteneur `spark-master` de la plateforme Docker de l'Atelier 3 (`docker exec -it spark-master pyspark`, ou un script `spark-submit`). Le chemin `"purchases.txt"` des exemples ci-dessous est à adapter selon l'environnement, comme indiqué à l'Atelier 4.2 (`/data/purchases.txt` en local Docker, `/data/raw/purchases.txt` sur HDFS).

---

## 1. Rappel théorique (15–20 min)

### 1.1 Spark SQL

Spark SQL est le module de Spark qui permet d'interroger des DataFrames avec une syntaxe SQL standard, en plus (ou à la place) de l'API programmatique vue à l'Atelier 4.2. Les deux approches sont strictement équivalentes en performance, car elles passent par le même moteur d'optimisation (Catalyst) : le choix entre API DataFrame et SQL est une question de préférence et de lisibilité.

### 1.2 Le métastore

Un **métastore** est un catalogue centralisé qui décrit les données disponibles : noms des tables, colonnes, types, emplacement physique des fichiers, format de stockage. Sans métastore, chaque traitement devrait redéfinir manuellement la structure des données à chaque exécution.

Le métastore est ce qui permet de dire : *« la table `purchases` existe, ses colonnes sont `pdate`, `ptime`, `store`… et ses fichiers sont stockés à tel emplacement »*, indépendamment du moteur de traitement (Spark, Hive, ou un autre) qui vient ensuite l'interroger. C'est le **Hive Metastore**, présenté en détail à l'Atelier 5.1 (Partie II), aujourd'hui partagé et réutilisé par Spark SQL lui-même.

### 1.3 Rappel : Hive vs Spark SQL

| | Hive (Atelier 5.1) | Spark SQL (cet atelier) |
|---|---|---|
| Interface | SQL déclaratif (HiveQL) | SQL déclaratif et API DataFrame |
| Usage principal | Analytique et entrepôt de données | Traitements analytiques, ETL et Machine Learning |
| Connexion requise | Serveur séparé (`beeline`/HiveServer2) | Aucune — s'exécute dans la session Spark déjà ouverte |
| Optimisation | Pour les requêtes sur données stockées (CBO, partition pruning, vu à l'Atelier 5.1) | Pour les traitements distribués en mémoire (Catalyst) |
| Metastore | S'appuie sur le Hive Metastore | Peut réutiliser le même Hive Metastore |
| Exécution | Via Tez, Spark ou MapReduce | Via le moteur Spark |

**Point clé du cours** : dans les architectures Big Data actuelles, Spark est devenu le moteur de calcul principal, tandis que Hive (et son metastore) reste utile comme **catalogue de métadonnées** partagé entre plusieurs outils, et comme moteur analytique par lots pour les requêtes ad hoc sur l'historique. C'est cette articulation qui sera retrouvée sur Amazon EMR à l'Atelier 7.

### 1.4 Commandes Spark SQL à connaître

À la différence de Hive (aide-mémoire `beeline`/`hive` de l'Atelier 5.1), Spark SQL ne nécessite aucune connexion réseau ni serveur séparé : les requêtes SQL s'exécutent directement dans la session Spark déjà ouverte (`pyspark` ou un script `spark-submit`).

| Commande | Rôle |
|---|---|
| `df.createOrReplaceTempView("nom_table")` | Enregistrer un DataFrame sous un nom de table utilisable en SQL, le temps de la session |
| `spark.sql("SELECT ...")` | Exécuter une requête SQL et récupérer le résultat sous forme de DataFrame |
| `spark.sql("...").show(n)` | Exécuter la requête et afficher les `n` premières lignes |
| `spark.catalog.listTables()` | Lister les vues/tables enregistrées dans la session courante |

Clauses SQL mobilisées dans cet atelier : `GROUP BY`, `ORDER BY`, `CASE WHEN ... THEN ... ELSE ... END` (équivalent SQL du `when().otherwise()` vu en DataFrame à l'Atelier 4.2), sous-requête scalaire (`(SELECT COUNT(*) FROM ...)`), et `PIVOT` (transformer des valeurs de ligne en colonnes — ici, une valeur par mode de paiement, comme vu avec la requête `TABLESAMPLE`/bucketing à l'Atelier 5.1).

**Point clé à retenir** : `spark.sql("...")` renvoie un DataFrame comme un autre — rien n'empêche d'enchaîner ensuite des méthodes DataFrame (`.filter()`, `.orderBy()`...) sur le résultat d'une requête SQL, les deux API étant interchangeables à tout moment de la chaîne de traitement.

---

## 2. Atelier pratique (90 min) — Spark SQL sur `purchases.txt`

### 2.1 Création de la vue temporaire

```python
schema = "pdate DATE, ptime STRING, store STRING, product STRING, cost DOUBLE, payment STRING"
df = spark.read.csv("purchases.txt", sep="\t", schema=schema)

df.createOrReplaceTempView("purchases")
```

### 2.2 Question 1 — Total des ventes par magasin

```python
spark.sql("""
    SELECT store, SUM(cost) AS total_ventes
    FROM purchases
    GROUP BY store
    ORDER BY total_ventes DESC
""").show()
```

### 2.3 Question 2 — Nombre de ventes par tranche horaire

```python
spark.sql("""
    SELECT SUBSTRING(ptime, 1, 2) AS heure, COUNT(*) AS nb_ventes
    FROM purchases
    GROUP BY SUBSTRING(ptime, 1, 2)
    ORDER BY heure
""").show(24)
```

### 2.4 Question 3 — Montant total par mode de paiement

```python
spark.sql("""
    SELECT payment, SUM(cost) AS montant_total
    FROM purchases
    GROUP BY payment
    ORDER BY montant_total DESC
""").show()
```

### 2.5 Question 4 — Taux de paiement cash / électronique

```python
spark.sql("""
    SELECT
        CASE WHEN payment = 'Cash' THEN 'cash' ELSE 'electronique' END AS categorie,
        COUNT(*) * 100.0 / (SELECT COUNT(*) FROM purchases) AS taux_pct
    FROM purchases
    GROUP BY CASE WHEN payment = 'Cash' THEN 'cash' ELSE 'electronique' END
""").show()
```

### 2.6 Bonus — Requête pivot : nombre de ventes par magasin et par mode de paiement

```python
spark.sql("""
    SELECT *
    FROM (
        SELECT store, payment FROM purchases
    )
    PIVOT (
        COUNT(*) FOR payment IN ('Cash', 'Amex', 'Visa', 'MasterCard', 'Discover')
    )
    ORDER BY store
""").show()
```

Résultat attendu (extrait) :

| Magasin | Cash | Amex | Visa | MasterCard | Discover |
|---|---|---|---|---|---|
| Tampa | 14368 | 2000 | 3500 | 1256 | 800 |
| … | … | … | … | … | … |

### Consignes

1. Réaliser les requêtes 1 à 4 en Spark SQL.
2. Réaliser la requête bonus (pivot).
3. Comparer les résultats de la question 1 obtenus ici avec ceux obtenus en HiveQL à l'Atelier 5.1 et en RDD/DataFrame à l'Atelier 4.2 — ils doivent être strictement identiques.
4. Tenir un tableau récapitulatif (une ligne par méthode : HiveQL, RDD, DataFrame, Spark SQL) avec, pour chacune, le nombre de lignes de code et le temps d'exécution observé sur la question 1.

---

## 3. Synthèse

- Spark SQL et l'API DataFrame sont deux façons équivalentes d'exprimer le même traitement, optimisées par le même moteur — le choix entre les deux est une question de lisibilité, pas de performance.
- Le metastore est le catalogue qui décrit les tables disponibles, indépendamment du moteur qui les interroge ; Spark SQL peut réutiliser le Hive Metastore vu à l'Atelier 5.1.
- Sur un même jeu de données et un même besoin métier (`purchases.txt`), HiveQL (Atelier 5.1), RDD et DataFrame (Atelier 4.2), et Spark SQL (cet atelier) produisent des résultats identiques avec des niveaux d'abstraction et de performance différents — c'est la synthèse du fil rouge Big Data du chapitre.

---

## Pour aller plus loin

- Comparer les formats de stockage colonnaires (Parquet, ORC — vus à l'Atelier 5.1) et leur intérêt pour les performances de lecture par rapport au format texte utilisé ici.
- Étudier le rôle d'AWS Glue Data Catalog comme équivalent managé du Hive Metastore sur AWS.
- Créer une table Spark SQL persistante (`CREATE TABLE ... USING PARQUET`) à partir du DataFrame `purchases`, et vérifier qu'elle apparaît dans `spark.catalog.listTables()` d'une session à l'autre.
