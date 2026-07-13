# Introduction au Big Data et au Cloud Computing

**Formation :** Licence Informatique 2 — SupDeCo
**Enseignant :** M. TOP
**Volume horaire :** 20 heures
**Approche pédagogique :** apprentissage par ateliers (*workshop-based learning*)

---

## 1. Présentation du cours

Ce module introduit les concepts fondamentaux du Big Data et du Cloud Computing à travers une succession d'ateliers pratiques, plutôt que par l'étude isolée de technologies. Les étudiants suivent un fil conducteur unique qui illustre l'évolution des architectures de données modernes :

> **Bases de données → NoSQL → Big Data → Traitement distribué → Conteneurisation → Cloud Computing → Plateforme Big Data sur AWS.**

Docker sert de socle technique transversal dès l'atelier 2 : MongoDB, puis Spark, y sont exécutés dans le même environnement conteneurisé, ce qui prépare naturellement la transition vers Amazon EMR, où l'on retrouve les mêmes briques logicielles dans le Cloud. Hadoop, de son côté, n'est pas traité comme un chapitre autonome : il est présenté comme l'écosystème historique (HDFS, YARN, Hive) dans lequel Spark s'est imposé comme le moteur de calcul principal.

Les notions théoriques sont volontairement limitées à ce qui est indispensable à la compréhension des ateliers.

## 2. Compétences visées

À l'issue du module, l'étudiant est capable de :

- expliquer les enjeux du Big Data et du Cloud Computing ;
- identifier les limites des bases de données relationnelles face aux données massives ;
- comprendre les principaux modèles NoSQL et manipuler MongoDB dans un environnement conteneurisé ;
- comprendre les principes du traitement distribué avec Apache Spark et développer des traitements PySpark (RDD et DataFrame) ;
- comprendre le rôle des principaux composants de l'écosystème Hadoop, dont Hive ;
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
| 4 | Traitement distribué avec Apache Spark (RDD, DataFrame) | 3 h | [Atelier_4_Apache_Spark_PySpark.md](Atelier_4_Apache_Spark_PySpark.md) |
| 5 | Spark SQL et introduction à Hive | 2 h | [Atelier_5_SparkSQL_Hive.md](Atelier_5_SparkSQL_Hive.md) |
| 6 | Introduction au Cloud Computing (IaaS/PaaS/SaaS, AWS : EC2, S3, IAM, VPC) | 3 h | [Atelier_6_Introduction_Cloud_Computing.md](Atelier_6_Introduction_Cloud_Computing.md) |
| 7 | Big Data sur AWS avec Amazon EMR | 4 h | [Atelier_7_BigData_AWS_EMR.md](Atelier_7_BigData_AWS_EMR.md) |

Chaque support suit la même structure : objectifs, rappel théorique, démonstration guidée, atelier pratique (avec commandes et code), synthèse et pistes pour aller plus loin.

## 6. Jeux de données du projet fil rouge

Deux jeux de données réels servent de support pratique tout au long du module :

- **`countries.json`** (250 documents) — fiches descriptives de pays (nom, capitale, région, population, devises, langues). Utilisé à l'**atelier 2** pour illustrer le modèle documents de MongoDB (`geodb.countries`) : listes de valeurs distinctes, filtrage sur champs imbriqués, agrégations par région.
- **`purchases.txt`** — historique de tickets de caisse (`pdate`, `ptime`, `store`, `product`, `cost`, `payment`). Utilisé comme fil rouge des **ateliers 4, 5 et 7** : les mêmes questions métier (ventes par magasin, par tranche horaire, par mode de paiement, taux cash/électronique) y sont traitées successivement en RDD, en DataFrame, en Spark SQL, en HiveQL, puis à l'échelle sur un cluster Amazon EMR.

```text
countries.json ──► Atelier 2 (MongoDB / NoSQL)

purchases.txt ──► Atelier 4 (RDD puis DataFrame)
               ──► Atelier 5 (Spark SQL puis Hive)
               ──► Atelier 6 (dépôt sur S3)
               ──► Atelier 7 (traitement à l'échelle sur Amazon EMR)
```

L'ensemble repose sur la plateforme Docker construite à l'**atelier 3** (MongoDB + Spark), reproduite dans le Cloud à l'**atelier 7** (Amazon EMR).

## 7. Origine des travaux pratiques

Les supports d'atelier s'appuient sur trois TP de référence, dont ils reprennent les consignes et les jeux de données :

- **TP2 — MongoDB** : import de `countries.json` dans `geodb.countries` et dix requêtes (régions, sous-régions, Sénégal, pays d'Afrique, Franc CFA, Gabon, populations agrégées) → repris dans l'Atelier 2.
- **TP3 — Analyse de données d'achat avec Hadoop Hive** : cluster Amazon EMR, table `achatdb.purchases`, requêtes HiveQL (ventes par magasin, par tranche horaire, par mode de paiement, taux cash/électronique) via Hue/beeline → repris dans l'Atelier 5.
- **TP4 — Analyse de données d'achats avec Hadoop Spark** : mêmes traitements que le TP3, réalisés en RDD (pyspark), en DataFrame et en Spark SQL (Zeppelin), avec requête pivot bonus → repris dans les Ateliers 4 et 5.

## 8. Évaluation

| Évaluation | Pondération |
|---|---:|
| Ateliers (participation, comptes rendus, exercices) | 30 % |
| Projet fil rouge | 30 % |
| Examen final (concepts et étude de cas) | 40 % |

## 9. Supports pédagogiques

- Supports de cours illustrés (fichiers `Atelier_*.md` de ce dossier) ;
- Notebooks Jupyter / Zeppelin (PySpark, Spark SQL) ;
- Fichiers Docker Compose ;
- Jeux de données `countries.json` et `purchases.txt` ;
- Documentation AWS ;
- Guides d'installation et de prise en main.
