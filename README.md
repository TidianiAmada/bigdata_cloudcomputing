# Introduction au Big Data et au Cloud Computing

**Formation :** Licence Informatique 2 — SupDeCo

**Enseignant :** M. TOP

**Volume horaire :** 20 heures

**Approche pédagogique :** apprentissage par ateliers (*workshop-based learning*)

---

## 1. Présentation du cours

Ce module introduit les concepts fondamentaux du Big Data et du Cloud Computing à travers une succession d'ateliers pratiques, plutôt que par l'étude isolée de technologies. Les étudiants suivent un fil conducteur unique qui illustre l'évolution des architectures de données modernes :

> **Bases de données → NoSQL → Big Data → Traitement distribué → Conteneurisation → Cloud Computing → Plateforme Big Data sur AWS.**

Docker sert de socle technique transversal dès l'atelier 2 : MongoDB, puis Spark, y sont exécutés dans le même environnement conteneurisé, ce qui prépare naturellement la transition vers Amazon EMR, où l'on retrouve les mêmes briques logicielles dans le Cloud. Hadoop n'est pas traité comme un simple aparté théorique : l'atelier 4.1 lui est entièrement consacré (HDFS, YARN), de sorte que Spark (atelier 4.2) apparaisse ensuite naturellement comme le moteur de calcul d'un cluster Hadoop moderne. Hive est ensuite approfondi dans un atelier dédié (5.1), positionné juste avant Spark SQL (5.2), pour bien distinguer l'entrepôt de données analytique (Hive) du moteur de calcul en mémoire (Spark).

Les notions théoriques sont volontairement limitées à ce qui est indispensable à la compréhension des ateliers.

## 2. Compétences visées

À l'issue du module, l'étudiant est capable de :

- expliquer les enjeux du Big Data et du Cloud Computing ;
- identifier les limites des bases de données relationnelles face aux données massives ;
- comprendre les principaux modèles NoSQL et manipuler MongoDB dans un environnement conteneurisé ;
- comprendre l'architecture de Hadoop (HDFS, YARN) ;
- comprendre les principes du traitement distribué avec Apache Spark et développer des traitements PySpark (RDD, DataFrame) ;
- interroger des données stockées sur HDFS avec Hive (architecture interne, types de données, Managed/External, Partitioning, Bucketing) et avec Spark SQL ;
- utiliser Docker pour exécuter une plateforme Big Data ;
- expliquer les concepts fondamentaux du Cloud Computing (IaaS/PaaS/SaaS, élasticité, haute disponibilité) ;
- déployer un cluster Spark/Hadoop sur Amazon EMR et manipuler les principaux services AWS liés au calcul et au stockage (EC2, S3, IAM, VPC).

## 3. Prérequis

- bases de données relationnelles et SQL ;
- bases de Python ;
- notions élémentaires de systèmes d'exploitation.

## 4. Méthodes pédagogiques

Chaque séance combine généralement un rappel théorique (20–30 min), une démonstration guidée, un atelier pratique et une synthèse collective. Les étudiants construisent progressivement une plateforme Big Data complète, d'abord sur Docker, puis sur AWS.

## 5. Programme et supports de référence

| Atelier | Thème | Durée | Support |
|---|---|---|---|
| 1 | Pourquoi le Big Data ? (5V, limites du relationnel, architectures distribuées) | 2 h | [Atelier_1_Pourquoi_le_Big_Data.md](Atelier_1_Pourquoi_le_Big_Data.md) |
| 2 | NoSQL avec MongoDB | 2 h | [Atelier_2_NoSQL_MongoDB.md](Atelier_2_NoSQL_MongoDB.md) |
| 3 | Docker pour les plateformes Big Data | 2 h | [Atelier_3_Docker_Plateformes_BigData.md](Atelier_3_Docker_Plateformes_BigData.md) |
| 4.1 | Stockage distribué et gestion des ressources avec Hadoop (HDFS, YARN) | 2 h | [Atelier_4_1_Hadoop_HDFS_YARN.md](Atelier_4_1_Hadoop_HDFS_YARN.md) |
| 4.2 | Traitement distribué avec Apache Spark (RDD, DataFrame) | 3 h | [Atelier_4_2_Apache_Spark_RDD_DataFrame.md](Atelier_4_2_Apache_Spark_RDD_DataFrame.md) |
| 5.1 | Apache Hive : le data warehouse distribué | 3 h | [Atelier_5_1_Hive_DataWarehouse.md](Atelier_5_1_Hive_DataWarehouse.md) |
| 5.2 | Spark SQL | 2 h | [Atelier_5_2_SparkSQL.md](Atelier_5_2_SparkSQL.md) |
| 6 | Introduction au Cloud Computing (IaaS/PaaS/SaaS, AWS : EC2, S3, IAM, VPC) | 3 h | [Atelier_6_Introduction_Cloud_Computing.md](Atelier_6_Introduction_Cloud_Computing.md) |
| 7 | Big Data sur AWS avec Amazon EMR | 4 h | [Atelier_7_BigData_AWS_EMR.md](Atelier_7_BigData_AWS_EMR.md) |

Chaque support suit la même structure : objectifs, rappel théorique, démonstration guidée, atelier pratique (avec commandes et code), synthèse et pistes pour aller plus loin. L'atelier 4 a été scindé en deux séances (4.1 puis 4.2) afin que les étudiants comprennent l'architecture Hadoop — où sont stockées les données (HDFS), qui orchestre leur traitement (YARN) — avant de manipuler Spark comme moteur de calcul de ce même cluster. L'atelier 5 est scindé de la même façon (5.1 puis 5.2) : Hive est traité en profondeur comme un entrepôt de données à part entière (architecture interne, Managed/External, Partitioning, Bucketing) avant que Spark SQL ne vienne, en 5.2, offrir une quatrième façon d'interroger les mêmes données. Le volume horaire total du programme ci-dessus (23 h) dépasse légèrement les 20 h indicatives ; à ajuster selon le rythme du groupe (par exemple en allégeant la Partie pratique de l'atelier 5.1 si le temps manque).

## 6. Jeux de données du projet fil rouge

Deux jeux de données réels servent de support pratique tout au long du module :

- **`countries.json`** (250 documents) — fiches descriptives de pays (nom, capitale, région, population, devises, langues). Utilisé à l'**atelier 2** pour illustrer le modèle documents de MongoDB (`geodb.countries`) : listes de valeurs distinctes, filtrage sur champs imbriqués, opérations CRUD complètes, agrégations par région.
- **`purchases.txt`** — historique de tickets de caisse (`pdate`, `ptime`, `store`, `product`, `cost`, `payment`). Utilisé comme fil rouge des **ateliers 4.1, 4.2, 5.1, 5.2 et 7** : les mêmes questions métier (ventes par magasin, par tranche horaire, par mode de paiement, taux cash/électronique) y sont traitées successivement sur HDFS, en RDD, en DataFrame, en HiveQL, en Spark SQL, puis à l'échelle sur un cluster Amazon EMR.

```text
countries.json ──► Atelier 2 (MongoDB / NoSQL)

purchases.txt ──► Atelier 4.1 (HDFS + YARN)
               ──► Atelier 4.2 (Spark : RDD puis DataFrame)
               ──► Atelier 5.1 (Hive : Managed/External, Partitioning, Bucketing)
               ──► Atelier 5.2 (Spark SQL)
               ──► Atelier 6 (dépôt sur S3)
               ──► Atelier 7 (traitement à l'échelle sur Amazon EMR)
```

L'ensemble repose sur la plateforme Docker construite à l'**atelier 3** (MongoDB + Spark), reproduite dans le Cloud à l'**atelier 7** (Amazon EMR).



## 7. Évaluation

| Évaluation | Pondération |
|---|---:|
| Ateliers (participation, comptes rendus, exercices) | 30 % |
| Projet fil rouge | 30 % |
| Examen final (concepts et étude de cas) | 40 % |

## 8. Supports pédagogiques

- Supports de cours illustrés (fichiers `Atelier_*.md` de ce dossier) ;
- Notebooks Jupyter / Zeppelin (PySpark, Spark SQL) ;
- Fichiers Docker Compose ;
- Jeux de données `countries.json` et `purchases.txt` ;
- Documentation AWS ;
- Guides d'installation et de prise en main.
