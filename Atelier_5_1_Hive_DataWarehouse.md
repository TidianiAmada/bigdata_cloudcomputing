# Atelier 5.1 — Apache Hive : le Data Warehouse distribué

**Module :** Introduction au Big Data et au Cloud Computing
**Formation :** Licence Informatique 2 — SupDeCo
**Enseignant :** M. TOP
**Durée :** 3 heures
**Environnement pratique :** Docker (Hive + HDFS + YARN + Metastore PostgreSQL/MySQL)

---

## Objectifs de l'atelier

À l'issue de cet atelier, l'étudiant sera capable de :

- expliquer pourquoi Hive a été créé ;
- distinguer Hive d'un SGBD relationnel classique ;
- comprendre l'architecture interne de Hive (Driver, Compiler, Optimizer, Metastore, Execution Engine) ;
- comprendre le cycle complet d'exécution d'une requête HiveQL ;
- modéliser une table Hive adaptée à un cas métier, avec les types primitifs et complexes appropriés ;
- choisir entre table **Managed** et **External** ;
- optimiser une table grâce au **Partitioning** et au **Bucketing** ;
- créer des tables Hive et charger des données depuis HDFS ;
- exécuter des requêtes analytiques et observer leur exécution.

> **Où se situe cet atelier.** HDFS et YARN ont été vus à l'Atelier 4.1, Spark (RDD/DataFrame) à l'Atelier 4.2. Cet atelier introduit Hive en profondeur, avant l'Atelier 5.2 (Spark SQL), afin de comparer, en connaissance de cause, deux façons d'interroger les mêmes données en SQL sur un cluster Hadoop.

---

## Partie I — Introduction à Hive (20 min)

### 1. Pourquoi Hive ?

Avant Hive, analyser plusieurs téraoctets de données stockées sur HDFS imposait d'écrire du code MapReduce en Java :

```text
Mapper.java
    │
Reducer.java
    │
Compilation
    │
    JAR
    │
Soumission
    │
Résultat
```

Plusieurs centaines de lignes de Java, pour un besoin aussi simple que « le total des ventes par magasin ». Hive permet de remplacer tout ce cycle par :

```sql
SELECT store, SUM(cost) AS total_ventes
FROM purchases
GROUP BY store;
```

**Point essentiel à retenir avant tout le reste** : Hive ne réalise **jamais** lui-même les calculs. Hive **traduit** une requête SQL en un plan d'exécution distribué, confié ensuite à un moteur de calcul (Tez, Spark ou MapReduce).

### 2. Place de Hive dans l'écosystème Hadoop

```text
                Utilisateur
                     │
                  HiveQL
                     │
                   Hive
                     │
            Tez / Spark / MapReduce
                     │
                    YARN
                     │
                    HDFS
```

À retenir dans l'ordre :

- **Hive** = couche SQL (traduit la requête) ;
- **HDFS** (Atelier 4.1) = stockage des données ;
- **YARN** (Atelier 4.1) = gestion des ressources ;
- **Tez / Spark / MapReduce** = moteurs qui exécutent réellement le calcul.

---

## Partie II — Architecture interne (30 min)

Cinq composants gouvernent le cycle de vie d'une requête HiveQL :

```text
                    Requête HiveQL
                          │
                          ▼
                       Driver ───────────────►  Hive Metastore
                          │                     (bases, tables, colonnes,
                          ▼                      partitions, buckets,
                       Compiler                  statistiques, formats —
                 (construction de l'AST,          jamais les données)
                  validation syntaxique
                  et sémantique)
                          │
                          ▼
                       Optimizer
        (Cost-Based Optimizer, Predicate Pushdown,
              Map Join, Partition Pruning)
                          │
                          ▼
                   Execution Engine
                  (Tez / Spark / MapReduce)
                          │
                          ▼
                        YARN
                          │
                          ▼
                        HDFS
```

**Driver.** Le chef d'orchestre : il reçoit la requête, pilote son cycle de vie complet (compilation, optimisation, exécution) et renvoie le résultat.

**Compiler.** Construit l'*Abstract Syntax Tree* (AST) de la requête, puis effectue une validation syntaxique (la requête est-elle grammaticalement correcte ?) et une validation sémantique (les tables et colonnes référencées existent-elles réellement, en interrogeant le Metastore ?).

**Optimizer.** Améliore le plan d'exécution avant qu'il ne soit exécuté, notamment via :

- le **Cost-Based Optimizer (CBO)** : choisit le plan le moins coûteux parmi plusieurs possibles, à partir des statistiques du Metastore ;
- le **Predicate Pushdown** : applique les filtres (`WHERE`) le plus tôt possible, au plus près des données, plutôt qu'après avoir tout lu ;
- le **Map Join** : charge une petite table en mémoire pour éviter un `JOIN` distribué coûteux ;
- le **Partition Pruning** : élimine dès la planification les partitions qui ne peuvent pas contenir de résultat (cf. Partie V.3).

**Execution Engine.** Exécute réellement le plan optimisé, sur le cluster :

| Moteur | Statut |
|---|---|
| **Apache Tez** | Moteur recommandé par défaut depuis Hive 2.x |
| **Apache Spark** | Moteur alternatif |
| **MapReduce** | Historique, conservé principalement pour compatibilité |

> Depuis Hive 2.x, Apache Tez est généralement le moteur recommandé pour les déploiements Hadoop, tandis que Spark peut être utilisé comme moteur alternatif. MapReduce reste principalement présent pour des raisons de compatibilité.

**Hive Metastore.** Catalogue central des métadonnées : bases, tables, colonnes, partitions, buckets, statistiques, formats de fichiers. **Il ne contient jamais les données elles-mêmes** — c'est ce même composant qui sera retrouvé, réutilisé comme catalogue partagé avec Spark SQL, à l'Atelier 5.2.

---

## Partie III — Cycle de vie d'une requête (Data Flow) (25 min)

Le passage d'une requête HiveQL à un résultat suit sept étapes bien identifiées :

```text
1. Execute Query
   L'utilisateur (via l'UI, beeline ou Hue) envoie la requête au Driver.
        │
        ▼
2. Get Plan
   Le Driver transmet la requête au Compiler, qui vérifie la syntaxe
   et construit un plan préliminaire.
        │
        ▼
3. Get Metadata
   Le Compiler interroge le Metastore : les tables et colonnes
   référencées existent-elles ? Quel est leur schéma ?
        │
        ▼
4. Send Metadata
   Le Metastore répond avec les métadonnées demandées.
        │
        ▼
5. Send Plan
   Le Compiler valide sémantiquement la requête et renvoie
   le plan d'exécution complet au Driver.
        │
        ▼
6. Execute Plan
   Le Driver transmet le plan à l'Execution Engine (Tez/Spark/MR).
        │
        ▼
7. Execute Job puis Fetch Result
   L'Execution Engine soumet le job à YARN, qui l'exécute sur les
   DataNodes ; les résultats remontent au Driver, qui les renvoie
   à l'utilisateur.
```

*Exercice* : sans regarder le schéma ci-dessus, redessiner de mémoire les sept étapes et leurs flèches, puis comparer avec un binôme.

---

## Partie IV — Types de données (20 min)

### Types primitifs

| Type | Description |
|---|---|
| `TINYINT` / `SMALLINT` / `INT` / `BIGINT` | Entiers de précision croissante (1, 2, 4, 8 octets) |
| `FLOAT` | Nombre à virgule flottante, simple précision |
| `DOUBLE` | Nombre à virgule flottante, double précision |
| `DECIMAL` | Nombre décimal de précision fixe (calculs financiers) |
| `STRING` | Chaîne de caractères de longueur non déclarée |
| `VARCHAR(n)` | Chaîne de longueur maximale déclarée (1 à 65 355) |
| `CHAR(n)` | Chaîne de longueur fixe |
| `BOOLEAN` | Valeur `TRUE` / `FALSE` |
| `DATE` | Date seule (`yyyy-MM-dd`) |
| `TIMESTAMP` | Date et heure, précision à la nanoseconde |
| `BINARY` | Données binaires brutes |

Points de vigilance :

- **`STRING` vs `VARCHAR`** : `STRING` ne nécessite pas de déclarer de longueur maximale et est généralement préféré ; `VARCHAR` existe surtout pour la compatibilité avec d'autres systèmes SQL.
- **`FLOAT` vs `DOUBLE`** : `DOUBLE` offre une précision supérieure, au prix d'un espace de stockage plus important — à privilégier dès que la précision compte (montants, par exemple).
- **`DATE`** ne contient aucune information d'heure ; **`TIMESTAMP`** combine date et heure.
- **`BOOLEAN`** est stocké de façon compacte mais reste peu utilisé dans les entrepôts analytiques, où l'on préfère souvent des indicateurs numériques agrégeables.

### Types complexes

| Type | Description | Exemple |
|---|---|---|
| `ARRAY<type>` | Collection ordonnée d'éléments de même type | `ARRAY<STRING>` |
| `MAP<clé, valeur>` | Ensemble de paires clé-valeur | `MAP<STRING, STRING>` |
| `STRUCT<champ:type, ...>` | Regroupement de champs nommés, éventuellement de types différents | `STRUCT<ville:STRING, code_postal:STRING>` |
| `UNIONTYPE<...>` | Valeur pouvant prendre l'un de plusieurs types (présentation uniquement — rarement utilisé en pratique) | — |

```sql
CREATE TABLE clients (
    id INT,
    nom STRING,
    historique_achats ARRAY<STRING>,
    contacts MAP<STRING, STRING>,
    adresse STRUCT<rue:STRING, ville:STRING, code_postal:STRING>
);

-- Accéder au premier élément d'un tableau
SELECT historique_achats[0] FROM clients;

-- Accéder à un champ d'une structure
SELECT adresse.ville FROM clients;

-- Accéder à une valeur d'une map par sa clé
SELECT contacts["email"] FROM clients;
```

Ces exemples montrent que les types complexes restent réellement exploitables en SQL, avec une syntaxe d'accès dédiée (`[index]` pour un `ARRAY`, `.champ` pour un `STRUCT`, `["clé"]` pour une `MAP`).

---

## Partie V — Data Modeling (40 min)

C'est la partie la plus importante de l'atelier.

### 1. Managed Table (table interne)

Hive **possède** les données : elles sont stockées dans le répertoire de l'entrepôt Hive (par défaut `/user/hive/warehouse/<base>.db/<table>`).

```text
CREATE TABLE (Managed)
        │
        ▼
Répertoire de l'entrepôt Hive
(/user/hive/warehouse/achatdb.db/purchases_managed)
        │
        ▼
DROP TABLE → supprime les métadonnées ET les données
```

```sql
CREATE TABLE achatdb.purchases_managed (
    pdate    DATE,
    ptime    STRING,
    store    STRING,
    product  STRING,
    cost     FLOAT,
    payment  STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;
```

### 2. External Table

Hive **référence** des données situées ailleurs sur HDFS (clause `LOCATION`), sans en devenir propriétaire — c'est l'approche utilisée dans les *data lakes*, où un même fichier brut est partagé entre plusieurs outils (Hive, Spark, autres).

> **Rappel.** À l'Atelier 4.1, la table `achatdb.purchases` a été créée avec une clause `LOCATION` pointant directement vers des données déjà présentes sur HDFS (`/data/raw`) — sans le nommer explicitement à l'époque, c'était déjà une External Table.

```sql
CREATE EXTERNAL TABLE achatdb.purchases_external (
    pdate    DATE,
    ptime    STRING,
    store    STRING,
    product  STRING,
    cost     FLOAT,
    payment  STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/data/raw';
```

| | Managed | External |
|---|---|---|
| Propriété des données | Hive possède les données | Hive référence les données |
| Effet de `DROP TABLE` | Supprime les métadonnées **et** les données | Supprime uniquement les métadonnées, les données restent |
| Usage typique | Tables de travail, résultats intermédiaires | Données brutes partagées (*data lake*), sources externes |

### 3. Partitioning

**Sans partition**, une requête filtrant sur `store` doit lire l'intégralité du fichier, magasin par magasin, pour ne retenir que les lignes voulues :

```text
Sans partition :
/data/raw/purchases.txt   (un seul fichier — scan complet à chaque requête,
                            même pour ne récupérer qu'un seul magasin)
```

**Avec partition** (par exemple par `store`), Hive organise physiquement les données en un sous-répertoire par valeur de la colonne de partition :

```text
Avec partition (par store) :
/warehouse/purchases_partitioned/store=Tampa/...
/warehouse/purchases_partitioned/store=San Jose/...
/warehouse/purchases_partitioned/store=Chicago/...
```

```sql
CREATE TABLE achatdb.purchases_partitioned (
    pdate    DATE,
    ptime    STRING,
    product  STRING,
    cost     FLOAT,
    payment  STRING
)
PARTITIONED BY (store STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT INTO TABLE achatdb.purchases_partitioned PARTITION (store)
SELECT pdate, ptime, product, cost, payment, store
FROM achatdb.purchases_managed;
```

Une requête filtrant sur `store` (`WHERE store = 'Tampa'`) ne lit alors **que** le sous-répertoire correspondant — c'est le **Partition Pruning** évoqué au §Optimizer (Partie II).

*Exercice* : comparer, avec `EXPLAIN`, le plan d'exécution de la même requête filtrée par magasin sur `purchases_managed` (non partitionnée) et sur `purchases_partitioned` — repérer, dans la sortie, la mention des partitions effectivement lues.

### 4. Bucketing

Le bucketing intervient **à l'intérieur** d'une partition (ou de la table entière si elle n'est pas partitionnée) : il répartit les lignes dans un nombre fixe de fichiers, selon une fonction de hachage appliquée à une colonne.

```text
Hash(store)
     │
     ▼
  Bucket (0 à N-1)
     │
     ▼
Fichier ORC
```

```sql
CREATE TABLE achatdb.purchases_bucketed (
    pdate    DATE,
    ptime    STRING,
    store    STRING,
    product  STRING,
    cost     FLOAT,
    payment  STRING
)
CLUSTERED BY (store) INTO 4 BUCKETS
STORED AS ORC;

SET hive.enforce.bucketing = true;

INSERT INTO TABLE achatdb.purchases_bucketed
SELECT * FROM achatdb.purchases_managed;
```

Le bucketing améliore les performances de jointure (deux tables bucketées sur la même clé et le même nombre de buckets peuvent être jointes bucket par bucket, sans mélanger l'ensemble des données) et permet un échantillonnage efficace (`TABLESAMPLE`) sans lire la table entière.

### 5. Formats de stockage

| Format | Avantage principal |
|---|---|
| **TEXTFILE** | Simplicité, lisible directement, aucun outil requis pour l'inspecter |
| **CSV** (via un SerDe dédié) | Format d'échange universel avec d'autres outils |
| **AVRO** | Sérialisation orientée ligne, bonne gestion de l'évolution de schéma |
| **ORC** | Format colonnaire optimisé spécifiquement pour Hive (compression, statistiques intégrées) |
| **PARQUET** | Format colonnaire optimisé pour l'écosystème Spark, largement interopérable |

---

## Partie VI — Modes d'exécution (20 min)

```text
Local
  │
  └── Pas de YARN — exécution directe sur le nœud maître,
      adaptée aux petits jeux de données de test.

Distributed
  │
  └── YARN
        │
        └── Tez
              │
              └── HDFS
```

*Exercice* : exécuter la même requête d'agrégation en forçant le mode local (`SET hive.exec.mode.local.auto = true;` sur un échantillon réduit) puis en mode distribué, et comparer les temps d'exécution affichés en fin de requête.

---

## Partie VII — Hive vs RDBMS (20 min)

| | Hive | PostgreSQL (RDBMS classique) |
|---|---|---|
| Rôle historique | Interroger HDFS en SQL | Moteur de traitement distribué généraliste |
| Clés primaires / étrangères | Non imposées | Imposées et vérifiées |
| Accès aux données | Scan massif (parfois atténué par partitioning/bucketing) | Index |
| Traitement | Par lots (*batch*) | Temps réel |
| Scalabilité | Horizontale (*scale-out*) | Verticale (*scale-up*) |

**À retenir avant tout le reste de cette section** : Hive est un moteur **analytique**, pas un moteur **transactionnel**. Il n'est pas conçu pour des mises à jour ligne à ligne fréquentes ni pour garantir des contraintes d'intégrité référentielle — c'est un entrepôt de données interrogé par lots, pas une base de données de production.

---

## Partie VIII — Atelier pratique Docker (1 h 30)

> **Environnement.** Faute d'accès à Amazon EMR à ce stade, cet atelier utilise un environnement Docker dédié à Hive (distinct de la plateforme MongoDB + Spark de l'Atelier 3, et du cluster HDFS+YARN de l'Atelier 4.1 — chaque atelier ayant sa propre pile Docker autonome), comprenant HDFS (NameNode, DataNode) et Hive (Metastore adossé à PostgreSQL, HiveServer2). Orchestration basée sur les images `bde2020/hadoop-*` et `bde2020/hive`, fournie à la racine du dépôt sous le nom `docker-compose-hive.yml` :

```yaml
services:
  namenode:
    image: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
    container_name: namenode
    environment:
      - CLUSTER_NAME=hive-cluster
      - CORE_CONF_fs_defaultFS=hdfs://namenode:8020
    ports:
      - "9870:9870"
    networks:
      - hive_net

  datanode:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    container_name: datanode
    environment:
      - CORE_CONF_fs_defaultFS=hdfs://namenode:8020
      - SERVICE_PRECONDITION=namenode:9870
    depends_on:
      - namenode
    networks:
      - hive_net

  hive-metastore-postgresql:
    image: bde2020/hive-metastore-postgresql:2.3.0
    container_name: hive-metastore-postgresql
    networks:
      - hive_net

  hive-metastore:
    image: bde2020/hive:2.3.2-postgresql-metastore
    container_name: hive-metastore
    command: /opt/hive/bin/hive --service metastore
    environment:
      - SERVICE_PRECONDITION=hive-metastore-postgresql:5432
    depends_on:
      - hive-metastore-postgresql
    networks:
      - hive_net

  hive-server:
    image: bde2020/hive:2.3.2-postgresql-metastore
    container_name: hive-server
    environment:
      - SERVICE_PRECONDITION=hive-metastore:9083
    ports:
      - "10000:10000"
    volumes:
      - ./data:/data   # purchases.txt, réutilisé par TP6 (External Table) et TP7 (LOAD DATA LOCAL)
    depends_on:
      - hive-metastore
    networks:
      - hive_net

networks:
  hive_net:
    driver: bridge
```

> **Note.** `CORE_CONF_fs_defaultFS` est indispensable : sans cette variable, le DataNode ne sait pas vers quel hôte annoncer ses blocs et reste indéfiniment en attente de connexion au NameNode (`hdfs dfsadmin -report` afficherait alors `Live datanodes (0)`) — un piège de configuration bien réel, identique à celui rencontré et corrigé pour de vrai sur le cluster HDFS+YARN de l'[Atelier 4.1](Solutions/Solution_Atelier_4_1.md). Les tags d'image évoluent par ailleurs avec le temps ; vérifier leur disponibilité avant la séance et ajuster si nécessaire (cf. la remarque similaire sur les images Spark à l'Atelier 3).

Démarrage de la plateforme depuis la racine du dépôt (`purchases.txt` placé au préalable dans le dossier `data/`) :

```bash
docker compose -f docker-compose-hive.yml up -d
```

### TP1 — Découvrir le cluster

```bash
docker compose -f docker-compose-hive.yml ps
```

### TP2 — Entrer dans le conteneur Hive et déposer `purchases.txt` sur HDFS

```bash
docker exec -it hive-server bash
hdfs dfs -mkdir -p /data/raw
hdfs dfs -put /data/purchases.txt /data/raw/
```

`/data/purchases.txt` correspond au fichier monté en volume dans le conteneur `hive-server` (§ compose ci-dessus) ; `/data/raw` est le répertoire HDFS qui sera référencé par la clause `LOCATION` de la table External (TP6).

### TP3 — Se connecter à Hive

```bash
beeline -u jdbc:hive2://localhost:10000
```

### TP4 — Créer une base

```sql
CREATE DATABASE IF NOT EXISTS achatdb;
USE achatdb;
```

### TP5 — Créer une table Managed

```sql
CREATE TABLE purchases_managed (
    pdate    DATE,
    ptime    STRING,
    store    STRING,
    product  STRING,
    cost     FLOAT,
    payment  STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;
```

### TP6 — Créer une External Table

```sql
CREATE EXTERNAL TABLE purchases_external (
    pdate    DATE,
    ptime    STRING,
    store    STRING,
    product  STRING,
    cost     FLOAT,
    payment  STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/data/raw';
```

### TP7 — Charger `purchases.txt` dans la table Managed

Contrairement à l'External Table (TP6), qui référence des données déjà présentes sur HDFS, la table Managed reçoit ici une **copie** du fichier local (`/data/purchases.txt`, le même fichier monté en volume qu'au TP2, mais lu cette fois depuis le système de fichiers local du conteneur plutôt que depuis HDFS — d'où `LOCAL` dans la clause) :

```sql
LOAD DATA LOCAL INPATH '/data/purchases.txt'
INTO TABLE purchases_managed;
```

### TP8 — Créer une table partitionnée

```sql
CREATE TABLE purchases_partitioned (
    pdate    DATE,
    ptime    STRING,
    product  STRING,
    cost     FLOAT,
    payment  STRING
)
PARTITIONED BY (store STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT INTO TABLE purchases_partitioned PARTITION (store)
SELECT pdate, ptime, product, cost, payment, store
FROM purchases_managed;
```

### TP9 — Créer une table bucketisée

```sql
CREATE TABLE purchases_bucketed (
    pdate    DATE,
    ptime    STRING,
    store    STRING,
    product  STRING,
    cost     FLOAT,
    payment  STRING
)
CLUSTERED BY (store) INTO 4 BUCKETS
STORED AS ORC;

SET hive.enforce.bucketing = true;

INSERT INTO TABLE purchases_bucketed
SELECT * FROM purchases_managed;
```

### TP10 — Comparer les performances

**Sans partition** (table `purchases_managed`, scan complet) :

```sql
SELECT SUM(cost) FROM purchases_managed WHERE store = 'Tampa';
```

**Avec partition** (table `purchases_partitioned`, un seul sous-répertoire lu) :

```sql
SELECT SUM(cost) FROM purchases_partitioned WHERE store = 'Tampa';
```

**Avec bucketing** (table `purchases_bucketed`, jointure ou échantillonnage sur `store`) :

```sql
SELECT SUM(cost)
FROM purchases_bucketed TABLESAMPLE(BUCKET 1 OUT OF 4 ON store)
WHERE store = 'Tampa';
```

*Exercice* : comparer, pour chacune des trois requêtes, le temps d'exécution affiché par `beeline` en fin de requête, et — si le moteur le permet — le nombre de fichiers/partitions effectivement lus (`EXPLAIN` devant chaque requête). Les étudiants doivent constater directement pourquoi le partitioning et le bucketing existent, plutôt que de l'admettre sur parole.

---

## Synthèse

```text
                    HDFS
              stocke les fichiers
                     │
──────────────────────────────────────
                    Hive
           ajoute un schéma SQL
                     │
──────────────────────────────────────
                Metastore
        mémorise les métadonnées
                     │
──────────────────────────────────────
            Driver + Optimizer
          construisent le plan
                     │
──────────────────────────────────────
              Tez / Spark / MR
             exécutent le calcul
                     │
──────────────────────────────────────
                    YARN
           fournit CPU et mémoire
```

- Hive ne calcule jamais lui-même : il traduit HiveQL en un plan d'exécution confié à un moteur (Tez, Spark ou MapReduce), lui-même exécuté via YARN sur des données stockées sur HDFS.
- Driver, Compiler, Optimizer, Metastore et Execution Engine sont les cinq composants dont il faut connaître le rôle précis — en particulier que le Metastore ne stocke jamais les données, seulement leurs métadonnées.
- Managed et External Tables se distinguent par la propriété des données et l'effet de `DROP TABLE` ; Partitioning et Bucketing sont deux mécanismes d'optimisation complémentaires (l'un au niveau des répertoires, l'autre au niveau des fichiers à l'intérieur d'une partition).
- Hive reste un moteur analytique par lots, pas un moteur transactionnel : pas de clés primaires/étrangères imposées, pas d'index au sens SGBDR classique, scalabilité horizontale plutôt que verticale.

### Ouverture — Hive et Spark : deux approches complémentaires (10 min)

| | Hive | Spark SQL (Atelier 5.2) |
|---|---|---|
| Interface | SQL déclaratif | SQL et API DataFrame |
| Usage principal | Analytique et entrepôt de données | Traitements analytiques, ETL et Machine Learning |
| Optimisation | Pour les requêtes sur données stockées | Pour les traitements distribués en mémoire |
| Metastore | S'appuie sur le Hive Metastore | Peut réutiliser le même Hive Metastore |
| Exécution | Via Tez, Spark ou MapReduce | Via le moteur Spark |

Ce tableau prépare directement l'Atelier 5.2 : les mêmes questions métier posées sur `purchases.txt` (ventes par magasin, par tranche horaire, par mode de paiement, taux cash/électronique) y seront reformulées en Spark SQL, sur le même Metastore.

### Complément théorique — l'architecture Spark en bref

Avant de retrouver Spark SQL en détail à l'Atelier 5.2, un bref rappel de l'architecture interne de Spark permet de comprendre *pourquoi* certaines de ses règles existent — un parallèle direct peut être fait avec des notions déjà connues en programmation orientée objet.

**Driver program et RDD.** Toute application Spark repose sur un **driver program**, qui exécute la fonction `main` de l'utilisateur et pilote des opérations parallèles sur un cluster (même rôle d'orchestrateur que le Driver Hive de la Partie II, mais pour Spark). L'abstraction centrale que manipule ce driver est le **RDD (*Resilient Distributed Dataset*)** : une collection d'éléments partitionnée entre les nœuds du cluster, sur laquelle on peut opérer en parallèle. Un RDD se crée soit à partir d'un fichier HDFS (ou tout système de fichiers supporté par Hadoop — c'est exactement `purchases.txt`, manipulé aux Ateliers 4.1/4.2), soit à partir d'une collection existante côté driver, puis se **transforme** (`map`, `filter`, `reduceByKey`...). Spark permet aussi de **persister** un RDD en mémoire (`.cache()`, déjà vu à l'Atelier 4.2) pour le réutiliser efficacement d'une opération à l'autre, et **récupère automatiquement** d'une panne de nœud grâce à la lignée (*lineage*) des transformations, qui permet de recalculer uniquement la partition perdue plutôt que de tout refaire.

**Variables partagées.** Par défaut, quand Spark exécute une fonction en parallèle sous forme de tâches sur différents nœuds, il envoie **une copie indépendante** de chaque variable utilisée à chaque tâche — comme un attribut d'instance dupliqué dans chaque objet, sans aucun état commun. Mais parfois, une variable doit au contraire être **partagée** entre les tâches, ou entre les tâches et le driver. Spark propose alors deux mécanismes dédiés, chacun avec un équivalent direct en POO :

| Mécanisme Spark | Rôle | Équivalent conceptuel en POO |
|---|---|---|
| **Broadcast variable** | Met en cache une valeur en mémoire sur **tous** les nœuds, une seule fois, plutôt que de la réenvoyer à chaque tâche | Une **variable statique** (`static`) de classe : une seule copie partagée par toutes les instances, plutôt qu'un attribut dupliqué dans chaque objet |
| **Accumulator** | Variable sur laquelle on ne peut qu'« ajouter » (compteurs, sommes), agrégée depuis toutes les tâches vers le driver | Un **compteur synchronisé** (`synchronized` en Java, verrou/mutex en général) : plusieurs threads incrémentent une même variable partagée sans se marcher dessus, l'écriture concurrente étant gérée par le framework plutôt que par le développeur |

**Pourquoi cette distinction compte.** Un attribut d'instance classique, dupliqué dans chaque objet, correspond au comportement par défaut de Spark (une copie par tâche) : chaque tâche modifie sa propre copie sans jamais affecter les autres. Une variable `static`, elle, est un état **unique et partagé** — c'est le rôle d'une **broadcast variable** : diffuser une seule fois une donnée volumineuse (une table de correspondance, un modèle entraîné...) que toutes les tâches liront sans la retélécharger à chaque fois. Un compteur `synchronized`, lui, est un état partagé en **écriture** contrôlée — c'est le rôle d'un **accumulator** : additionner en toute sécurité des valeurs produites par des centaines de tâches concurrentes, sans que le développeur n'ait à gérer lui-même la synchronisation (verrous, sections critiques) que ce type de partage exigerait en programmation concurrente classique.

*Exercice de réflexion* : dans le programme RDD de l'Atelier 4.2 (Question 4 — taux de paiement cash/électronique), la variable `total` est calculée une fois par le driver (`parsed.count()`) puis réutilisée dans une transformation exécutée sur chaque tâche. S'agit-il d'une variable partagée au sens de Spark (broadcast/accumulator), ou d'une simple valeur capturée par fermeture (*closure*) et copiée à chaque tâche ? Justifier, puis identifier un cas, parmi les traitements déjà écrits (Ateliers 4.2/5.2), où l'utilisation explicite d'une broadcast variable ou d'un accumulator apporterait un gain réel par rapport au code actuel.

---

## Pour aller plus loin

- Comparer le temps d'exécution d'une même agrégation entre le format `TEXTFILE` et le format `ORC` sur `purchases_bucketed`.
- Explorer `EXPLAIN FORMATTED` pour visualiser le plan d'exécution complet (y compris le choix du CBO) d'une requête avec jointure.
- Étudier les vues matérialisées Hive (*materialized views*) comme mécanisme d'accélération complémentaire au partitioning/bucketing.
- Lire la section « Shared Variables » du *RDD Programming Guide* officiel de Spark, et repérer dans le code déjà écrit à l'Atelier 4.2 un endroit où une broadcast variable remplacerait avantageusement une donnée recalculée ou retransmise à chaque tâche.

### Bibliographie

- Capriolo, E., Wampler, D., & Rutherglen, J. (2012). *Programming Hive*. O'Reilly.
- Apache Hive. *Hive Language Manual*. https://cwiki.apache.org/confluence/display/Hive/LanguageManual
- White, T. (2021). *Hadoop: The Definitive Guide*. O'Reilly.
- Apache Spark. *RDD Programming Guide*. https://spark.apache.org/docs/latest/rdd-programming-guide.html
