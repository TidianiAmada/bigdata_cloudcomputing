# Atelier 5 — Spark SQL et introduction à Hive

**Module :** Introduction au Big Data et au Cloud Computing
**Formation :** Licence Informatique 2 — SupDeCo
**Enseignant :** M. TOP
**Durée :** 2 heures

---

## Objectifs de l'atelier

- utiliser **Hive** pour interroger en HiveQL le jeu de données `purchases.txt` stocké sur HDFS ;
- utiliser **Spark SQL** pour réaliser les mêmes traitements avec une syntaxe SQL standard ;
- comprendre le rôle d'un métastore dans un écosystème Big Data ;
- comprendre la différence entre Hive et Spark et leur complémentarité.

> **Remarque :** conformément à l'approche du cours, Hive est ici présenté comme **une brique de l'écosystème Hadoop**, et non comme un chapitre à part entière. L'objectif est de comprendre son rôle et son positionnement — pas d'entrer dans son administration. Cet atelier reprend, en HiveQL puis en Spark SQL, exactement les traitements déjà réalisés en RDD et en DataFrame à l'Atelier 4, sur le même jeu de données `purchases.txt`.

---

## 1. Rappel théorique (20–30 min)

### 1.1 Spark SQL

Spark SQL est le module de Spark qui permet d'interroger des DataFrames avec une syntaxe SQL standard, en plus (ou à la place) de l'API programmatique vue à l'Atelier 4. Les deux approches sont strictement équivalentes en performance, car elles passent par le même moteur d'optimisation (Catalyst) : le choix entre API DataFrame et SQL est une question de préférence et de lisibilité.

### 1.2 Le métastore

Un **métastore** est un catalogue centralisé qui décrit les données disponibles : noms des tables, colonnes, types, emplacement physique des fichiers, format de stockage. Sans métastore, chaque traitement devrait redéfinir manuellement la structure des données à chaque exécution.

Le métastore est ce qui permet de dire : *« la table `purchases` existe, ses colonnes sont `pdate`, `ptime`, `store`… et ses fichiers sont stockés à tel emplacement »*, indépendamment du moteur de traitement (Spark, Hive, ou un autre) qui vient ensuite l'interroger. C'est un composant central de l'écosystème Hadoop, historiquement introduit par Hive (**Hive Metastore**), mais aujourd'hui partagé et réutilisé par Spark SQL lui-même.

### 1.3 Hive : rôle dans l'écosystème Hadoop

Apache Hive a été conçu pour permettre d'interroger de grands volumes de données stockées sur HDFS (le système de fichiers distribué de Hadoop) à l'aide d'une syntaxe proche du SQL (HiveQL), sans avoir à écrire de code MapReduce bas niveau.

```text
Écosystème Hadoop (vue simplifiée)

┌────────────────────────────────────────────┐
│                  Hive                        │  ← interface SQL (HiveQL)
├────────────────────────────────────────────┤
│              Hive Metastore                  │  ← catalogue des tables
├──────────────────┬───────────────────────────┤
│      YARN         │        HDFS                │  ← gestion ressources / stockage distribué
└──────────────────┴───────────────────────────┘
```

### 1.4 Hive vs Spark : quelle différence ?

| | Hive | Spark |
|---|---|---|
| Rôle historique | Interroger HDFS en SQL | Moteur de traitement distribué généraliste |
| Moteur d'exécution | Historiquement MapReduce (disque) | En mémoire |
| Vitesse | Plus lent sur les traitements itératifs | Nettement plus rapide en mémoire |
| Usage aujourd'hui | Catalogue de métadonnées (metastore), requêtes ad hoc sur l'historique | Moteur principal de traitement (ETL, SQL, machine learning, streaming) |

**Point clé du cours** : dans les architectures Big Data actuelles, Spark est devenu le moteur de calcul principal, tandis que Hive (et son metastore) reste utile comme **catalogue de métadonnées** partagé entre plusieurs outils, plutôt que comme moteur d'exécution. C'est cette articulation qui sera retrouvée sur Amazon EMR à l'Atelier 7.

---

## 2. Partie 1 — Hive sur `purchases.txt`

Cette partie se déroule sur le cluster EMR (application Hive installée), en réutilisant `purchases.txt` chargé sur HDFS à l'Atelier 4.

### 2.1 Connexion et création de la base

Connexion au nœud maître, puis lancement du client Hive (ou `beeline`) :

```sql
!record tp_hive.txt   -- enregistre le flux du terminal (beeline)

CREATE DATABASE achatdb;
USE achatdb;
```

### 2.2 Création de la table `purchases`

```sql
CREATE TABLE purchases (
    pdate    DATE,
    ptime    CHAR(5),
    store    STRING,
    product  STRING,
    cost     FLOAT,
    payment  STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/hadoop/purchases_data';
```

`purchases.txt` doit au préalable avoir été déposé dans le répertoire HDFS référencé par `LOCATION` (`hdfs dfs -put purchases.txt /user/hadoop/purchases_data/`).

### 2.3 Requête 1 — Total des ventes par magasin (meilleures ventes en tête)

```sql
INSERT OVERWRITE DIRECTORY 'rq5'
SELECT store, SUM(cost) AS total_ventes
FROM purchases
GROUP BY store
ORDER BY total_ventes DESC;
```

### 2.4 Requête 2 — Nombre total de ventes par tranche horaire

```sql
SELECT SUBSTR(ptime, 1, 2) AS heure, COUNT(*) AS nb_ventes
FROM purchases
GROUP BY SUBSTR(ptime, 1, 2)
ORDER BY heure;
```

Export du résultat vers `rq7.csv` (depuis Hue, ou via `INSERT OVERWRITE LOCAL DIRECTORY`), puis représentation sous forme d'**histogramme** (ventes en ordonnée, tranche horaire en abscisse) à l'aide de Python (`matplotlib`) ou de l'outil de visualisation de Hue.

### 2.5 Requête 3 — Montant total des achats par mode de paiement

```sql
SELECT payment, SUM(cost) AS montant_total
FROM purchases
GROUP BY payment
ORDER BY montant_total DESC;
```

Export vers `rq8.csv`, puis représentation sous forme de **diagramme circulaire**.

### 2.6 Requête 4 — Taux de paiement cash vs paiement électronique (en une seule requête)

```sql
SELECT
    CASE WHEN payment = 'Cash' THEN 'cash' ELSE 'electronique' END AS categorie,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM purchases) AS taux_pct
FROM purchases
GROUP BY CASE WHEN payment = 'Cash' THEN 'cash' ELSE 'electronique' END;
```

Export vers `rq9.csv`, puis représentation sous forme de **diagramme circulaire**.

### 2.7 Interface Hue

Hue offre une interface web pour écrire et exécuter des requêtes HQL sans passer par la ligne de commande. Sur un cluster EMR, les principales interfaces web accessibles depuis le nœud maître sont :

| Interface | URI |
|---|---|
| Hue | `http://<master-public-dns>:8888/` |
| Hadoop HDFS NameNode (EMR 6.x) | `http://<master-public-dns>:9870/` |
| JupyterHub | `https://<master-public-dns>:9443/` |
| Livy | `http://<master-public-dns>:8998/` |
| Ganglia | `http://<master-public-dns>/ganglia/` |

### 2.8 Aide-mémoire HiveQL (beeline / hive)

`jdbc_url` = `jdbc:hive2://localhost:10000`

| Action | beeline (hiveserver2) | hive (hiveserver1) |
|---|---|---|
| Se connecter | `beeline -u <jdbc_url>` | `hive -h <hostname> -p <port>` |
| Entrer en mode interactif | `beeline` | `hive` |
| Lister les tables | `!table` / `show tables;` | `show tables;` |
| Décrire une table | `!column table_name` / `desc table_name;` | `desc table_name;` |
| Exécuter une requête | `select * from table_name;` | `select * from table_name;` |
| Enregistrer le résultat | `!record fichier.dat` | N/A |
| Commande shell | `!sh ls` | `!ls;` |
| Commande HDFS | `dfs -ls;` | `dfs -ls;` |
| Exécuter un fichier `.hql` | `!run fichier.hql` | `source fichier.hql;` |
| Quitter | `!quit` | `quit;` |

---

## 3. Partie 2 — Spark SQL sur `purchases.txt`

### 3.1 Création de la vue temporaire

```python
schema = "pdate DATE, ptime STRING, store STRING, product STRING, cost DOUBLE, payment STRING"
df = spark.read.csv("purchases.txt", sep="\t", schema=schema)

df.createOrReplaceTempView("purchases")
```

### 3.2 Question 1 — Total des ventes par magasin

```python
spark.sql("""
    SELECT store, SUM(cost) AS total_ventes
    FROM purchases
    GROUP BY store
    ORDER BY total_ventes DESC
""").show()
```

### 3.3 Question 2 — Nombre de ventes par tranche horaire

```python
spark.sql("""
    SELECT SUBSTRING(ptime, 1, 2) AS heure, COUNT(*) AS nb_ventes
    FROM purchases
    GROUP BY SUBSTRING(ptime, 1, 2)
    ORDER BY heure
""").show(24)
```

### 3.4 Question 3 — Montant total par mode de paiement

```python
spark.sql("""
    SELECT payment, SUM(cost) AS montant_total
    FROM purchases
    GROUP BY payment
    ORDER BY montant_total DESC
""").show()
```

### 3.5 Question 4 — Taux de paiement cash / électronique

```python
spark.sql("""
    SELECT
        CASE WHEN payment = 'Cash' THEN 'cash' ELSE 'electronique' END AS categorie,
        COUNT(*) * 100.0 / (SELECT COUNT(*) FROM purchases) AS taux_pct
    FROM purchases
    GROUP BY CASE WHEN payment = 'Cash' THEN 'cash' ELSE 'electronique' END
""").show()
```

### 3.6 Bonus — Requête pivot : nombre de ventes par magasin et par mode de paiement

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

1. Réaliser les requêtes 1 à 4 en HiveQL (Partie 1), en exportant chaque résultat dans le fichier CSV demandé et en produisant les visualisations associées (histogramme, diagrammes circulaires).
2. Réaliser les mêmes requêtes 1 à 4 en Spark SQL (Partie 2).
3. Réaliser la requête bonus (pivot) en Spark SQL.
4. Comparer les résultats de la question 1 obtenus ici avec ceux obtenus en RDD et en DataFrame à l'Atelier 4 — ils doivent être strictement identiques.

---

## 4. Synthèse

- Spark SQL et l'API DataFrame sont deux façons équivalentes d'exprimer le même traitement, optimisées par le même moteur.
- Le metastore est le catalogue qui décrit les tables disponibles, indépendamment du moteur qui les interroge.
- Hive est présenté comme une brique historique de l'écosystème Hadoop (HDFS + YARN + Hive), dont le rôle aujourd'hui est surtout celui de catalogue de métadonnées, Spark étant devenu le moteur de calcul principal.
- Sur un même jeu de données et un même besoin métier (`purchases.txt`), RDD, DataFrame, Spark SQL et HiveQL produisent des résultats identiques avec des niveaux d'abstraction et de performance différents — c'est la synthèse du fil rouge Big Data de ce module.

---

## Pour aller plus loin

- Comparer les formats de stockage colonnaires (Parquet, ORC) et leur intérêt pour les performances de lecture par rapport au format texte utilisé ici.
- Étudier le rôle d'AWS Glue Data Catalog comme équivalent managé du Hive Metastore sur AWS.
