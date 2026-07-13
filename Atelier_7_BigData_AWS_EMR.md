# Atelier 7 — Big Data sur AWS avec Amazon EMR

**Module :** Introduction au Big Data et au Cloud Computing
**Formation :** Licence Informatique 2 — SupDeCo
**Enseignant :** M. TOP
**Durée :** 4 heures

---

## Objectifs de l'atelier

- comprendre pourquoi un traitement Big Data nécessite un cluster plutôt qu'une machine unique ;
- comprendre l'architecture d'Amazon EMR (nœud maître, nœuds de calcul) ;
- déployer un cluster EMR intégrant Spark et Zeppelin (ou Hive) ;
- exécuter, sur un cluster réel, les traitements RDD, DataFrame, Spark SQL (Atelier 4-5) et Hive (Atelier 5) déjà écrits sur `purchases.txt` ;
- comprendre les grands principes de coût et les bonnes pratiques d'utilisation ;
- situer le rôle d'un outil de visualisation (Amazon QuickSight) dans le pipeline Big Data complet.

Cet atelier constitue l'aboutissement du projet fil rouge : il reprend, sur un cluster Cloud managé, exactement les mêmes traitements écrits en local avec Docker (Atelier 3) sur le jeu de données `purchases.txt`.

---

## 1. Rappel théorique (45–60 min)

### 1.1 Pourquoi un cluster plutôt qu'une machine unique ?

Même une instance EC2 très puissante reste une seule machine : sa RAM et son nombre de cœurs sont plafonnés, et une panne la rend totalement indisponible. Un **cluster** répartit le stockage et le calcul sur plusieurs machines, ce qui apporte trois bénéfices complémentaires à ce qui a été vu dans le module :

- **scalabilité horizontale** : on augmente la capacité en ajoutant des nœuds plutôt qu'en changeant de machine (cf. Atelier 1) ;
- **parallélisme réel** : les traitements Spark et Hive (Ateliers 4-5) s'exécutent simultanément sur plusieurs nœuds ;
- **résilience** : la perte d'un nœud ne compromet pas l'ensemble du traitement.

### 1.2 Amazon EMR : présentation

**Amazon EMR** (Elastic MapReduce) est un service managé qui permet de déployer, en quelques minutes, un cluster préconfiguré avec les principaux outils de l'écosystème Big Data : Spark, Hadoop (HDFS, YARN), Hive, Zeppelin, Hue, et d'autres. EMR se charge du provisionnement des instances EC2 sous-jacentes, de l'installation des logiciels et de leur configuration réseau — ce qui, en environnement local (Atelier 3), demandait de tout assembler manuellement avec Docker Compose.

### 1.3 Architecture d'un cluster EMR

```text
                    Amazon EMR Cluster
        ┌─────────────────────────────────────┐
        │                                       │
        │   ┌───────────────┐                   │
        │   │  Nœud maître   │  ← YARN ResourceManager,
        │   │  (Primaire)     │    Spark Driver, Hue, Zeppelin
        │   └───────┬───────┘
        │           │
        │   ┌───────┴────────────────────┐      │
        │   │                            │      │
        │ ┌────────────┐         ┌────────────┐  │
        │ │ Nœud core   │   …     │ Nœud core   │  │  ← stockage HDFS +
        │ │ (Unité princ.)│         │ (Unité princ.)│  │    calcul (Executors)
        │ └────────────┘         └────────────┘  │
        └─────────────────────────────────────┘
                       │
                       ▼
                 Amazon S3 (purchases.txt en entrée / résultats en sortie)
```

- **Nœud maître (Primaire)** : coordonne le cluster — YARN ResourceManager, gestionnaire HDFS (NameNode), et héberge le Driver Spark ainsi que les interfaces Hue/Zeppelin. Un seul nœud maître par cluster.
- **Nœuds core (Unités principales)** : exécutent les tâches (Executors Spark) et stockent des données sur HDFS local.
- **Nœuds task (optionnels)** : apportent de la puissance de calcul supplémentaire sans stocker de données ; souvent utilisés en instances Spot pour réduire les coûts.

### 1.4 Intégration avec Spark, Hive et S3

Sur EMR, Spark s'exécute avec **YARN** comme gestionnaire de cluster (au lieu du mode Standalone utilisé en local à l'Atelier 4), et Hive partage le même Hive Metastore que celui potentiellement utilisé par Spark SQL (Atelier 5). Les traitements lisent typiquement leurs données depuis **HDFS** (pour les traitements internes au cluster) ou directement depuis **Amazon S3** (pour séparer durablement le stockage du calcul) : on peut ainsi supprimer un cluster EMR sans perdre les données, puisqu'elles résident sur S3 indépendamment du cluster.

```text
S3 (purchases.txt)  →  Cluster EMR (Spark / Hive)  →  S3 (résultats : rq5, rq7, rq8, rq9)
```

---

## 2. Atelier pratique (2h30–3h)

### 2.1 Récupération du jeu de données

Le fichier `purchases.txt` a été déposé sur S3 à l'Atelier 6 (`s3://atelier-supdeco-<votre-nom>/entrees/purchases.txt`). Depuis le nœud maître du cluster EMR, on le copie sur HDFS pour le traitement :

```bash
aws s3 cp s3://atelier-supdeco-<votre-nom>/entrees/purchases.txt .
hdfs dfs -put purchases.txt /user/hadoop/purchases.txt
```

### 2.2 Déploiement guidé d'un cluster EMR

Deux variantes de cluster sont utilisées selon l'outil ciblé (elles peuvent être déployées successivement ou en parallèle par des groupes différents) :

**Cluster « Spark + Zeppelin »** (reprise des traitements RDD/DataFrame/SQL de l'Atelier 4) :
- Nom : `TP Spark EMR`, version EMR 7.1 ou supérieure.
- Applications : **Spark**, **Zeppelin**.
- Matériel : 1 nœud maître (Primaire) + 2 nœuds core (Unités principales), type d'instance `m4.large`.

**Cluster « Hive »** (reprise des traitements HiveQL de l'Atelier 5) :
- Nom : `TP Hive EMR`.
- Applications : **Hive**, **Hue** (pour l'interface de requêtage).
- Matériel : 1 nœud maître + 2 nœuds core.

Étapes communes dans la console AWS (EMR) :

1. **Créer un cluster** : nom, version d'EMR, applications à installer.
2. **Configuration matérielle** : type d'instance, nombre de nœuds core.
3. **Paire de clés EC2** : réutiliser celle créée à l'Atelier 6 pour se connecter en SSH au nœud maître.
4. **Rôle IAM** : sélectionner (ou laisser créer automatiquement) les rôles nécessaires (`EMR_DefaultRole`, `EMR_EC2_DefaultRole`), en cohérence avec le principe du moindre privilège vu à l'Atelier 6.
5. **Lancer le cluster** et observer les étapes de démarrage (*Starting* → *Bootstrapping* → *Waiting*).

### 2.3 Interfaces des applications Hadoop

Une fois le cluster actif, plusieurs interfaces web sont accessibles (en remplaçant `master-public-dns` par l'adresse DNS publique du nœud maître, visible dans la console EMR) :

| Interface | URI |
|---|---|
| Hue | `http://master-public-dns:8888/` |
| Zeppelin | `https://master-public-dns:8890/` |
| Hadoop HDFS NameNode (EMR 6.x et +) | `http://master-public-dns:9870/` |
| JupyterHub | `https://master-public-dns:9443/` |
| Livy | `http://master-public-dns:8998/` |
| Ganglia (supervision) | `http://master-public-dns/ganglia/` |

### 2.4 Reprise des traitements RDD / DataFrame (cluster Spark + Zeppelin)

Connexion SSH au nœud maître, puis lancement du shell PySpark pour la partie RDD (identique à l'Atelier 4, Partie A) :

```bash
pyspark
```

```python
purchases = sc.textFile("purchases.txt")
parsed = purchases.map(lambda line: line.split("\t"))

sales_by_store = parsed.map(lambda f: (f[2], float(f[4]))) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False)
sales_by_store.collect()
```

Dans un notebook **Zeppelin**, reprise de la partie DataFrame et Spark SQL (Ateliers 4 et 5, Parties B et 3) :

```python
schema = "pdate DATE, ptime STRING, store STRING, product STRING, cost DOUBLE, payment STRING"
df = spark.read.csv("purchases.txt", sep="\t", schema=schema)
df.createOrReplaceTempView("purchases")

spark.sql("""
    SELECT store, SUM(cost) AS total_ventes
    FROM purchases
    GROUP BY store
    ORDER BY total_ventes DESC
""").show()
```

### 2.5 Reprise des traitements Hive (cluster Hive)

Sur le cluster Hive, connexion à `beeline` (ou à Hue) et exécution des requêtes de l'Atelier 5, Partie 1 (création de `achatdb.purchases`, requêtes rq5, rq7, rq8, rq9).

### 2.6 Écriture des résultats sur S3

```python
resultat = df.groupBy("store").agg({"cost": "sum"})

resultat.write.csv(
    "s3://atelier-supdeco-<votre-nom>/resultats/",
    header=True,
    mode="overwrite"
)
```

```bash
aws s3 ls s3://atelier-supdeco-<votre-nom>/resultats/
```

### 2.7 Terminaison du cluster

**Étape indispensable** pour éviter toute facturation inutile :

```bash
aws emr terminate-clusters --cluster-ids <ID-du-cluster>
```

Ou depuis la console : bouton *Terminate*.

### Consignes

1. Déployer un cluster EMR (Spark + Zeppelin, ou Hive selon le groupe).
2. Charger `purchases.txt` depuis S3 vers HDFS.
3. Reproduire au moins deux des quatre traitements déjà écrits aux Ateliers 4 et 5 (RDD ou DataFrame ou SQL ou HiveQL) et vérifier que les résultats sont identiques à ceux obtenus en local.
4. Écrire un résultat sur S3 et vérifier son contenu.
5. **Terminer le cluster** dès l'exercice achevé.

---

## 3. Vers un pipeline Big Data complet sur AWS

Le pipeline introduit à l'Atelier 1 (ingestion → stockage/traitement → analyse et visualisation) se retrouve intégralement sur AWS :

```text
Amazon S3                  Cluster EMR                 Amazon S3
(purchases.txt,     →      (Spark / Hive :        →    (résultats :
 données brutes)            traitement)                 rq5, rq7, rq8, rq9)
                                                                │
                                                                ▼
                                                    Amazon QuickSight / Athena
                                                    (analyse et visualisation)
```

1. Les données brutes (`purchases.txt`) résident sur S3, indépendamment de tout cluster.
2. Un cluster EMR est démarré à la demande pour exécuter le traitement (Spark ou Hive).
3. Les résultats transformés sont réécrits dans un autre emplacement S3.
4. Le cluster EMR est arrêté (fin de la facturation calcul).
5. Un outil d'analyse — **Amazon QuickSight** (visualisation, tableaux de bord) ou **Amazon Athena** (requêtes SQL directement sur S3, sans cluster) — vient exploiter les résultats stockés sur S3.

Cette séparation stricte entre stockage (S3, permanent) et calcul (cluster EMR, temporaire et facturé à l'usage) est le principe économique et architectural central de tout pipeline Big Data dans le Cloud.

---

## 4. Coûts et bonnes pratiques

### Facturation

Un cluster EMR est facturé à l'heure (ou à la seconde selon les instances) pour :
- le coût des instances EC2 sous-jacentes ;
- un coût additionnel EMR par instance.

Le stockage S3 est facturé séparément, indépendamment de la durée de vie du cluster.

### Bonnes pratiques

- **Terminer systématiquement** un cluster EMR dès la fin des traitements — un cluster oublié continue de facturer même sans traitement en cours.
- **Séparer stockage et calcul** : conserver les données sur S3 plutôt que sur le HDFS local du cluster, pour pouvoir supprimer le cluster sans perdre les données.
- **Utiliser des instances Spot** pour les nœuds task lorsque la tolérance à l'interruption le permet, afin de réduire significativement les coûts.
- **Dimensionner le cluster** au plus près du besoin réel (nombre de nœuds, type d'instance) plutôt que de sur-provisionner par précaution.
- **Automatiser via des scripts ou des « Steps »** plutôt que des manipulations manuelles répétées, pour limiter le temps du cluster actif.

---

## 5. Synthèse

- Un cluster répond aux limites d'une machine unique en distribuant stockage et calcul, avec une meilleure résilience.
- Amazon EMR automatise le déploiement d'un cluster Big Data managé (Spark, Zeppelin, Hive, Hue), reproduisant dans le Cloud l'environnement construit manuellement en local depuis l'Atelier 3.
- Les traitements RDD, DataFrame, Spark SQL et HiveQL écrits aux Ateliers 4 et 5 sur `purchases.txt` s'exécutent, sans modification de logique, sur un cluster EMR réel.
- La séparation entre stockage (S3, durable) et calcul (cluster EMR, temporaire et facturé à l'usage) est le principe économique central du Big Data dans le Cloud, complété par des outils d'analyse et de visualisation (QuickSight, Athena) qui referment le pipeline ouvert à l'Atelier 1.

---

## Projet fil rouge — bilan de la progression

```text
purchases.txt
      │
      ▼
Docker (Atelier 3, socle transversal) ──► MongoDB (Atelier 2, NoSQL)
      │
      ▼
Spark : RDD puis DataFrame (Atelier 4)
      │
      ▼
Spark SQL et Hive (Atelier 5)
      │
      ▼
Amazon S3 (Atelier 6)
      │
      ▼
Amazon EMR : mêmes traitements à l'échelle (Atelier 7)
      │
      ▼
Résultats (rq5, rq7, rq8, rq9) → Analyse et visualisation (QuickSight / Athena)
```

Chaque étudiant doit être en mesure d'expliquer, pour chaque brique du projet, quelle limite technique elle vient résoudre par rapport à l'étape précédente, et de retrouver un même résultat métier (par exemple le classement des magasins par chiffre d'affaires) obtenu par quatre méthodes différentes : RDD, DataFrame, Spark SQL, HiveQL.

---

## Pour aller plus loin

- Comparer le coût et le temps d'exécution d'un même traitement sur le cluster Spark+Zeppelin versus le cluster Hive.
- Explorer EMR Serverless comme alternative sans gestion explicite du cluster.
- Explorer Amazon Athena pour interroger directement `purchases.txt` sur S3 en SQL, sans déployer de cluster EMR.
