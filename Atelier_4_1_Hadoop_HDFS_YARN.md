# Atelier 4.1 — Stockage distribué et gestion des ressources avec Hadoop (HDFS et YARN)

**Module :** Introduction au Big Data et au Cloud Computing
**Formation :** Licence Informatique 2 — SupDeCo
**Enseignant :** M. TOP
**Durée :** 2 heures

---

## Objectifs de l'atelier

- comprendre les limites du stockage traditionnel face aux données massives ;
- comprendre pourquoi Hadoop a été créé, et le principe *scale-out* qu'il généralise ;
- distinguer les rôles de **HDFS** (stockage) et **YARN** (gestion des ressources) au sein de l'écosystème Hadoop ;
- comprendre l'architecture Hadoop (NameNode/DataNodes, ResourceManager/NodeManagers) ;
- comprendre le découpage en blocs, la réplication, les *heartbeats* et les *block reports* ;
- comprendre comment une application est exécutée via YARN (ApplicationMaster, Containers) ;
- manipuler HDFS depuis le terminal.

> **Pourquoi cet atelier précède Spark (Atelier 4.2) et Hive (Atelier 5.1).** Spark et Hive ne remplacent ni l'un ni l'autre HDFS et YARN : ce sont deux moteurs qui s'exécutent **au-dessus** de l'écosystème Hadoop, pilotés par YARN et lisant des données stockées sur HDFS. Manipuler Spark ou Hive sans avoir vu HDFS et YARN revient à utiliser une API sans savoir *où vivent les données* ni *qui orchestre l'exécution* — c'est cet atelier qui pose ces fondations, avant que l'Atelier 4.2 (Spark) puis l'Atelier 5.1 (Hive) n'introduisent chacun leur moteur sur ce même cluster.

---

## 1. Rappel théorique (45 min)

### 1.1 Les limites du stockage classique

Un ordinateur personnel stocke toutes ses données sur un disque local. Cette approche fonctionne très bien pour quelques Go, voire quelques dizaines de Go. Elle devient rapidement insuffisante lorsqu'il faut traiter plusieurs centaines de Go, plusieurs To, voire plusieurs Po de données.

Les principales limites d'une machine unique sont :

- une **capacité disque** limitée par la taille physique d'un ou quelques disques ;
- une **mémoire insuffisante** pour charger l'ensemble des données à traiter ;
- une **absence de parallélisme** : un seul disque, un seul processeur, un seul débit d'entrée-sortie ;
- un **point unique de panne** (*single point of failure*) : la perte du disque emporte les données avec elle.

### 1.2 Pourquoi Hadoop ?

Hadoop répond simultanément à deux problèmes : **stocker** de très gros volumes de données, et **traiter** ces données sur plusieurs machines. Plutôt que d'utiliser un serveur très puissant (*scale-up*, cf. Atelier 1), Hadoop répartit les données sur un grand nombre de serveurs standards, peu coûteux (*scale-out*) — le même principe de scalabilité horizontale qui sous-tend l'ensemble du module.

### 1.3 Architecture Hadoop

```text
                          Utilisateur
                              │
            ┌─────────────────┴─────────────────┐
            │                                     │
          HDFS                                  YARN
     (stockage distribué)              (gestion des ressources)
            │                                     │
     ┌──────┴──────┐                     ┌────────┴────────┐
     │  NameNode    │                     │ ResourceManager │
     └──────┬──────┘                     └────────┬────────┘
            │                                     │
     ┌──────┴─────────────────────────────────────┴──────┐
     │                                                     │
     │   DataNode + NodeManager      DataNode + NodeManager │
     │   DataNode + NodeManager      DataNode + NodeManager │
     └─────────────────────────────────────────────────────┘
```

Chaque brique de l'écosystème a un rôle précis, qu'il faut savoir distinguer immédiatement :

| Composant | Rôle |
|---|---|
| **HDFS** | Stockage distribué des données brutes |
| **YARN** | Gestion des ressources de calcul (CPU, mémoire) et orchestration des exécutions |
| **Hive** (Atelier 5.1) | Interrogation SQL des données stockées sur HDFS |
| **Spark** (Atelier 4.2) | Moteur de calcul distribué, exécuté via YARN, lisant/écrivant sur HDFS |

### 1.4 HDFS : principes

**HDFS** (*Hadoop Distributed File System*) est le système de fichiers distribué de Hadoop. Il repose sur trois idées simples :

- **découpage en blocs** : chaque fichier est fractionné en blocs de taille fixe — **128 Mo par défaut** (configurable) — plutôt que stocké comme un fichier unique sur un seul disque ;
- **répartition des blocs** : les blocs d'un même fichier sont dispersés sur plusieurs DataNodes du cluster, et non regroupés sur une seule machine ;
- **réplication** : chaque bloc est dupliqué sur plusieurs DataNodes (facteur de réplication par défaut : 3), pour tolérer la panne d'une ou plusieurs machines.

**Pourquoi découper les fichiers en blocs ?**

- cela permet de stocker des fichiers **plus gros que la capacité d'un disque unique**, en les répartissant sur plusieurs machines ;
- cela permet de **lire et écrire un même fichier en parallèle**, plusieurs DataNodes travaillant simultanément sur des blocs différents ;
- cela **répartit la charge** de lecture/écriture entre les nœuds du cluster plutôt que de la concentrer sur un seul disque.

### 1.5 Le NameNode

Le NameNode est le nœud maître de HDFS. Point essentiel, à retenir avant tout le reste : **il ne stocke jamais les données elles-mêmes**.

| Le NameNode stocke (métadonnées) | Le NameNode ne stocke jamais |
|---|---|
| L'arborescence des fichiers et répertoires | Le contenu des blocs de données |
| Les permissions et propriétaires des fichiers | — |
| La liste des blocs qui composent chaque fichier | — |
| La localisation de chaque bloc (en mémoire, reconstruite au démarrage à partir des rapports des DataNodes) | — |

Cette distinction explique pourquoi le NameNode est un composant relativement léger en stockage (ses métadonnées tiennent en mémoire) mais critique : sa perte rend l'ensemble du cluster inutilisable, même si les données elles-mêmes sont intactes sur les DataNodes (d'où l'existence, dans les versions modernes de Hadoop, de mécanismes de haute disponibilité du NameNode, hors périmètre de cet atelier).

### 1.6 Les DataNodes

Les DataNodes sont les nœuds qui stockent **réellement** les données. Ils :

- stockent les blocs de données (et leurs répliques) sur leur disque local ;
- exécutent les opérations effectives de lecture et d'écriture demandées par les clients ;
- créent de nouvelles répliques d'un bloc à la demande du NameNode (par exemple si un DataNode tombe en panne et que le facteur de réplication n'est plus respecté).

### 1.7 Heartbeats

Notion souvent négligée mais essentielle au bon fonctionnement du cluster : chaque DataNode envoie régulièrement un signal de vie au NameNode.

```text
DataNode
   │
   │  Heartbeat (toutes les ~3 secondes)
   ▼
NameNode
```

Grâce à ce signal périodique, le NameNode sait en permanence quels DataNodes sont actifs. Si un DataNode cesse d'émettre des heartbeats pendant une durée définie, le NameNode le considère comme défaillant et déclenche la recréation des répliques des blocs qu'il hébergeait, sur d'autres DataNodes.

### 1.8 Block Report

Il faut distinguer clairement deux mécanismes qui circulent tous deux du DataNode vers le NameNode, mais qui ne portent pas la même information — une distinction régulièrement demandée en examen :

- **Heartbeat** : *« Je suis vivant. »* — un simple signal de disponibilité, sans détail sur les données.
- **Block Report** : *« Voici la liste complète des blocs que je possède actuellement. »* — envoyé moins fréquemment, il permet au NameNode de reconstruire (ou de vérifier) sa connaissance de la localisation de chaque bloc dans le cluster.

### 1.9 Réplication

```text
Bloc A
   │
   ├──► DataNode 1
   ├──► DataNode 3
   └──► DataNode 8

Facteur de réplication = 3
```

Chaque bloc est copié sur plusieurs DataNodes distincts (3 par défaut). L'objectif est la **tolérance aux pannes** : si un DataNode tombe (panne disque, coupure réseau, maintenance), le bloc reste disponible via ses autres répliques, et le NameNode déclenche la recréation d'une nouvelle réplique ailleurs pour revenir au facteur de réplication cible.

### 1.10 Lecture d'un fichier

```text
Client
   │
   ▼
NameNode  ──►  liste des DataNodes possédant chaque bloc
   │
   ▼
Lecture directe des blocs, en parallèle, depuis les DataNodes
```

Point clé : le NameNode ne transporte **jamais** les données lui-même. Il indique seulement au client où se trouvent les blocs demandés ; le client va ensuite lire les blocs directement auprès des DataNodes concernés, ce qui évite au NameNode de devenir un goulot d'étranglement.

### 1.11 Écriture d'un fichier

```text
Client
   │
   ▼
DataNode 1  ──►  DataNode 4  ──►  DataNode 8
        (pipeline de réplication)
```

Le client écrit le bloc vers un premier DataNode, qui le retransmet immédiatement au deuxième, qui le retransmet au troisième — un **pipeline de réplication** plutôt qu'un envoi séparé du client vers chaque réplique, ce qui économise la bande passante côté client.

### 1.12 YARN

YARN (*Yet Another Resource Negotiator*) est le gestionnaire de ressources de Hadoop : il alloue CPU et mémoire aux applications qui s'exécutent sur le cluster (Hive, Spark, MapReduce...), indépendamment du moteur de traitement utilisé.

```text
Client
   │
   ▼
ResourceManager
   │
   ▼
ApplicationMaster
   │
   ▼
Containers
   │
   ▼
NodeManagers
```

| Composant | Rôle |
|---|---|
| **ResourceManager** | Composant central, unique par cluster : reçoit les demandes d'exécution et négocie l'allocation globale des ressources. |
| **NodeManager** | Un par nœud du cluster : surveille les ressources disponibles localement et gère les containers exécutés sur son nœud. |
| **ApplicationMaster** | Un par application soumise : négocie les ressources nécessaires auprès du ResourceManager et coordonne l'exécution des tâches de cette application. |
| **Container** | Unité d'allocation de ressources (CPU + mémoire) dans laquelle s'exécute effectivement une tâche. |

C'est ce même ResourceManager qui, à l'Atelier 4.2, négociera les ressources nécessaires à l'exécution d'un programme Spark sur le cluster — et qui, à l'Atelier 5.1, fera de même pour une requête Hive traduite en job Tez ou MapReduce.

---

## 2. Partie pratique (1 h)

> **Environnement.** Cette partie pratique s'exécute sur un cluster Hadoop réel — en séance, le cluster Amazon EMR mis à disposition (mêmes commandes que sur tout cluster Hadoop standard ; déploiement complet, coûts et bonnes pratiques détaillés à l'Atelier 7). Pour travailler en autonomie sans EMR, un cluster Hadoop minimal (HDFS + YARN) peut être monté localement avec Docker Compose, distinct de la plateforme MongoDB + Spark de l'Atelier 3 :

```yaml
version: "3.8"

services:
  namenode:
    image: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
    container_name: namenode
    environment:
      - CLUSTER_NAME=hdfs-yarn-cluster
    ports:
      - "9870:9870"   # interface web du NameNode
    networks:
      - hadoop_net

  datanode1:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    container_name: datanode1
    environment:
      - SERVICE_PRECONDITION=namenode:9870
    depends_on:
      - namenode
    networks:
      - hadoop_net

  datanode2:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    container_name: datanode2
    environment:
      - SERVICE_PRECONDITION=namenode:9870
    depends_on:
      - namenode
    networks:
      - hadoop_net

  resourcemanager:
    image: bde2020/hadoop-resourcemanager:2.0.0-hadoop3.2.1-java8
    container_name: resourcemanager
    environment:
      - SERVICE_PRECONDITION=namenode:9870 datanode1:9864 datanode2:9864
    ports:
      - "8088:8088"   # interface web du ResourceManager
    depends_on:
      - namenode
    networks:
      - hadoop_net

  nodemanager:
    image: bde2020/hadoop-nodemanager:2.0.0-hadoop3.2.1-java8
    container_name: nodemanager
    environment:
      - SERVICE_PRECONDITION=resourcemanager:8088
    depends_on:
      - resourcemanager
    networks:
      - hadoop_net

networks:
  hadoop_net:
    driver: bridge
```

> **Note.** Deux DataNodes (facteur de réplication effectif limité à 2, contre 3 par défaut sur un cluster EMR de séance — un point à signaler explicitement en classe) et un seul NodeManager suffisent pour observer réellement la répartition et la réplication des blocs (§1.4-1.9) sans surcharger une machine de travail. Les tags d'image évoluent avec le temps ; vérifier leur disponibilité avant la séance et ajuster si nécessaire (même remarque qu'aux Ateliers 3 et 5.1).

Une fois la plateforme démarrée (`docker compose up -d`), les commandes ci-dessous s'exécutent depuis un terminal ouvert dans le conteneur `namenode` :

```bash
docker exec -it namenode bash
```

L'interface web du NameNode est accessible sur `http://localhost:9870`, celle du ResourceManager sur `http://localhost:8088` (permet de vérifier que le NodeManager y apparaît bien rattaché, en écho au ResourceManager/NodeManager du §1.12).

### Manipulation HDFS

**Vérifier l'état du cluster :**

```bash
hdfs dfsadmin -report
```

**Explorer HDFS :**

```bash
hdfs dfs -ls /
```

**Créer une arborescence :**

```bash
hdfs dfs -mkdir -p /data/raw
```

**Copier `purchases.txt` dans HDFS :**

```bash
hdfs dfs -put purchases.txt /data/raw/
```

**Vérifier la copie :**

```bash
hdfs dfs -ls /data/raw
```

**Lire le fichier directement depuis HDFS :**

```bash
hdfs dfs -cat /data/raw/purchases.txt | head -20
```

**Afficher le découpage en blocs et leur localisation :**

```bash
hdfs fsck /data/raw/purchases.txt -files -blocks -locations
```

*Exercice — répondre à partir de la sortie de `fsck` :*

1. Combien de blocs composent `purchases.txt` ?
2. Quel est le facteur de réplication observé (combien de copies par bloc) ?
3. Quels DataNodes possèdent chaque bloc ? Est-ce le même DataNode pour tous les blocs ?

Cet exercice oblige à observer réellement le comportement de HDFS décrit au §1.4-1.9, plutôt que de le tenir pour acquis.

### Consignes

1. Vérifier l'état du cluster (`hdfs dfsadmin -report`) et explorer l'arborescence HDFS existante.
2. Copier `purchases.txt` dans HDFS, vérifier la copie et lire le fichier directement depuis HDFS.
3. Exécuter `hdfs fsck` sur `purchases.txt` et répondre aux trois questions sur les blocs et la réplication.
4. Conserver `purchases.txt` sur HDFS (`/data/raw/purchases.txt`) : il sera directement réutilisé par Hive à l'Atelier 5.1 et par Spark à l'Atelier 4.2.

---

## 3. Synthèse

- Le stockage sur une seule machine atteint vite ses limites (capacité, mémoire, absence de parallélisme, point unique de panne) ; Hadoop y répond par la scalabilité horizontale plutôt que par une machine plus puissante.
- HDFS découpe chaque fichier en blocs (128 Mo par défaut), les répartit sur plusieurs DataNodes et les réplique (facteur 3 par défaut) pour la tolérance aux pannes.
- Le NameNode ne stocke que des métadonnées (arborescence, permissions, localisation des blocs) — jamais les données elles-mêmes ; les DataNodes stockent réellement les blocs et exécutent les lectures/écritures.
- Heartbeat (« je suis vivant ») et Block Report (« voici mes blocs ») sont deux mécanismes distincts et complémentaires par lesquels les DataNodes informent le NameNode.
- YARN sépare la négociation globale des ressources (ResourceManager) de leur gestion locale (NodeManager) et de la coordination d'une application donnée (ApplicationMaster), au sein de containers.

Cette compréhension du stockage (HDFS) et de l'orchestration des ressources (YARN) est le prérequis direct des deux moteurs qui s'exécutent au-dessus de ce même cluster : Spark (Atelier 4.2) et Hive (Atelier 5.1).

---

## Pour aller plus loin

- Explorer la haute disponibilité du NameNode (Standby NameNode, JournalNodes) dans les versions récentes de Hadoop.
- Comparer le facteur de réplication par défaut (3) à un facteur réduit (2) ou augmenté (5) en termes de coût de stockage et de tolérance aux pannes.
- Lire la sortie de `hdfs dfsadmin -report` en détail et identifier la capacité totale, utilisée et restante du cluster.

### Bibliographie

- White, T. (2021). *Hadoop: The Definitive Guide*. O'Reilly.
- Shvachko, K., Kuang, H., Radia, S., & Chansler, R. (2010). *The Hadoop Distributed File System*. IEEE MSST.
- Vavilapalli, V. K., et al. (2013). *Apache Hadoop YARN: Yet Another Resource Negotiator*. ACM SoCC.
