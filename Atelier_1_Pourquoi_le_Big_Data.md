# Atelier 1 — Fondements du Big Data

**Module :** Introduction au Big Data et au Cloud Computing
**Formation :** Licence Informatique 2 — SupDeCo
**Enseignant :** M. TOP
**Durée :** 2 heures

---

## Objectifs de l'atelier

À l'issue de cet atelier, l'étudiant sera capable de :

- expliquer les facteurs ayant conduit à l'émergence du Big Data ;
- identifier les limites des SGBDR face aux données massives ;
- définir et caractériser les cinq dimensions du Big Data ;
- décrire le principe d'une architecture distribuée.

---

> **Définition**
>
> Le Big Data désigne un ensemble de méthodes, d'architectures et de technologies permettant de stocker, traiter et analyser des volumes de données dont les caractéristiques dépassent les capacités des systèmes d'information traditionnels.

Depuis le début des années 2000, la quantité de données produites par les individus, les entreprises et les objets connectés connaît une croissance exponentielle. Cette évolution a profondément modifié les méthodes de stockage, de traitement et d'analyse des données, conduisant à l'émergence du paradigme Big Data. Ce premier atelier présente les motivations techniques ayant conduit à cette évolution et introduit les principaux concepts qui seront développés tout au long du module.

---

## 1. Rappel théorique (20–30 min)

### 1.1 L'évolution des données numériques

Jusqu'aux années 1990, l'essentiel des données produites par une organisation provenait de ses propres systèmes internes : gestion commerciale, comptabilité, ressources humaines. Ces données étaient structurées, de volume modéré, et géraient sans difficulté par des bases de données relationnelles installées sur un unique serveur.

Trois transformations technologiques majeures ont bouleversé cette situation :

1. **La démocratisation d'Internet** : chaque page consultée, chaque clic, chaque recherche génère une trace numérique.
2. **L'essor des réseaux sociaux et du mobile** : les utilisateurs deviennent eux-mêmes producteurs de contenu (photos, vidéos, messages, géolocalisation).
3. **La multiplication des capteurs et objets connectés (IoT)** : véhicules, montres, machines industrielles produisent en continu des flux de données.

Il en résulte que les organisations ne traitent plus seulement des données qu'elles ont elles-mêmes saisies, mais des volumes considérables de données générées en continu, par des sources multiples, à des formats variés.

### 1.2 Les limites des bases de données relationnelles

Les systèmes de gestion de bases de données relationnelles (SGBDR — MySQL, PostgreSQL, Oracle, SQL Server) reposent sur un modèle solide : tables, schémas fixes, contraintes d'intégrité, transactions ACID (Atomicité, Cohérence, Isolation, Durabilité). Ce modèle reste excellent pour de nombreux usages, mais montre ses limites lorsque :

| Limite | Explication |
|---|---|
| **Scalabilité verticale** | Un SGBDR classique s'exécute sur une seule machine ; pour encaisser plus de charge, il faut ajouter de la RAM/CPU à cette machine (*scale-up*), ce qui a un plafond physique et financier. |
| **Schéma rigide** | Toute donnée doit correspondre à un schéma défini à l'avance (colonnes, types). Les données semi-structurées ou non structurées (JSON, texte libre, images) s'y adaptent mal. |
| **Coût des jointures à grande échelle** | Les jointures entre très grandes tables deviennent lentes et coûteuses en ressources. |
| **Disponibilité** | Une panne du serveur unique rend le système indisponible, sauf dispositifs de réplication complexes à mettre en œuvre. |

La question n'est donc pas « le relationnel est-il dépassé ? » (il ne l'est pas, il reste la norme pour la majorité des applications de gestion), mais « à partir de quel volume, de quelle vitesse ou de quelle variété de données un autre modèle devient-il nécessaire ? ».

> **Remarque**
>
> Les systèmes relationnels ne sont pas remplacés par les technologies Big Data. Dans la pratique, les deux approches coexistent et répondent à des besoins complémentaires.

### 1.3 Les 5V du Big Data

Le Big Data se caractérise généralement par cinq dimensions. Les trois premières (Volume, Vélocité et Variété) ont été proposées par Doug Laney en 2001 ; les dimensions Véracité et Valeur ont été introduites ultérieurement afin de mieux caractériser les problématiques modernes liées aux données massives.

- **Volume** : la quantité de données produites (téraoctets, pétaoctets, voire plus), qui dépasse la capacité de traitement d'un système classique.
- **Vélocité** : la vitesse à laquelle les données sont générées et doivent être traitées (flux en temps réel, capteurs, transactions financières).
- **Variété** : la diversité des formats — données structurées (tables), semi-structurées (JSON, XML), non structurées (texte, image, vidéo, son).
- **Véracité** : la fiabilité et la qualité des données, souvent bruitées, incomplètes ou incohérentes lorsqu'elles proviennent de sources multiples.
- **Valeur** : l'objectif final — une donnée n'a d'intérêt que si l'on peut en extraire une information exploitable pour la décision.

### 1.4 Introduction aux architectures distribuées

Afin de répondre à ces limitations, l'approche du Big Data consiste à changer de paradigme : au lieu de renforcer une seule machine (*scale-up*), on répartit les données et les traitements sur **plusieurs machines** qui travaillent ensemble (*scale-out*, ou **scalabilité horizontale**).

| | Scale-up (verticale) | Scale-out (horizontale) |
|---|---|---|
| **Principe** | Ajouter des ressources (RAM, CPU) à une machine unique | Ajouter des machines supplémentaires au groupe |
| **Plafond** | Limité par les capacités physiques d'un seul serveur | Extensible en théorie sans limite |
| **Coût** | Machines haut de gamme, coût croissant de façon non linéaire | Machines standards, coût généralement plus maîtrisé |
| **Tolérance aux pannes** | Point unique de défaillance | Perte d'un nœud tolérée si les données sont répliquées |

Principe général d'une architecture distribuée :

```text
        Client / Application
                │
        ┌───────┴────────┐
        │   Coordinateur  │
        └───────┬────────┘
                │
   ┌────────────┼────────────┐
   │            │            │
 Nœud 1       Nœud 2       Nœud 3
(données +  (données +   (données +
 calcul)     calcul)      calcul)
```

Chaque nœud stocke une partie des données et exécute une partie du traitement. Les avantages sont :

- une capacité de stockage et de calcul qui augmente simplement en ajoutant des machines ;
- une meilleure tolérance aux pannes (la perte d'un nœud n'interrompt pas le service si les données sont répliquées) ;
- un coût généralement plus maîtrisé (machines standards plutôt qu'un serveur surdimensionné).

C'est ce principe qui sous-tend l'ensemble des technologies étudiées dans ce module : NoSQL, Spark, Hadoop, et le Cloud Computing lui-même. Cette approche constitue le fondement des infrastructures modernes telles que Hadoop, Spark, Cassandra ou encore les plateformes de Cloud Computing.

### 1.5 Les niveaux d'analytique décisionnelle

Traiter un gros volume de données n'a de sens que si l'on en tire une information exploitable (la dimension **Valeur** des 5V). On distingue classiquement quatre types d'analytique, du plus simple au plus complexe :

| Type d'analytique | Question posée | Exemple |
|---|---|---|
| **Descriptive** | Que s'est-il passé ? | Chiffre d'affaires total du mois dernier par magasin |
| **Diagnostique** | Pourquoi est-ce arrivé ? | Pourquoi les ventes ont-elles chuté dans telle région ? |
| **Prédictive** | Que va-t-il se passer ? | Prévision des ventes du mois prochain |
| **Prescriptive** | Que devons-nous faire ? | Quelles actions mettre en place pour augmenter les ventes ? |

Ce module se concentre essentiellement sur l'analytique **descriptive** (agrégations, statistiques simples) réalisée à grande échelle — c'est la brique indispensable avant d'envisager les analyses diagnostique, prédictive ou prescriptive.

### 1.6 Le pipeline Big Data et les rôles associés

Une architecture Big Data s'organise généralement selon les étapes suivantes :

```text
Sources de données → Ingestion → Stockage / Traitement → Analyse et visualisation → Décision
```

- **Ingestion** : récupération des données depuis leur source (fichiers, bases, flux), par lot (*batch*, latence de l'ordre de la minute) ou en flux continu (*streaming*, latence de l'ordre de la milliseconde).
- **Stockage** : conservation des données brutes ou transformées (entrepôt de données structuré, ou *data lake* pour des données brutes et hétérogènes).
- **Traitement** : nettoyage, transformation, agrégation — c'est le rôle que joueront Spark et Hadoop dans ce module.
- **Analyse et visualisation** : restitution des résultats sous une forme exploitable (tableaux, graphiques, tableaux de bord).

Trois profils métiers interviennent typiquement le long de ce pipeline :

| Rôle | Contribution |
|---|---|
| **Data Engineer** | Construit et maintient l'infrastructure : collecte, stockage, intégration, prétraitement des données. |
| **Data Analyst** | Manipule les données déjà préparées, produit des analyses statistiques et des rapports. |
| **Data Scientist** | Développe des modèles prédictifs avancés (apprentissage automatique, IA). |
| **Machine Learning Engineer** | Industrialise et déploie en production les modèles conçus par le Data Scientist. |

Ce module place l'étudiant successivement du côté Data Engineer (construction de la plateforme : Docker, Spark, EMR) et Data Analyst (requêtes et agrégations sur les données).

### 1.7 Exploration d'un jeu de données volumineux sous Linux

Préalablement à l'introduction d'un outil Big Data, quelques commandes shell standards permettent déjà de caractériser un jeu de données trop volumineux pour un tableur — ce sont elles que l'on utilise dans l'atelier pratique ci-dessous.

| Commande | Rôle | Exemple |
|---|---|---|
| `wget <url>` | Télécharger un fichier depuis une URL | `wget https://.../purchases.txt.gz` |
| `gunzip <fichier>.gz` | Décompresser une archive gzip | `gunzip purchases.txt.gz` |
| `ls -l <fichier>` | Afficher la taille et les métadonnées d'un fichier | `ls -l purchases.txt` |
| `du -h <fichier>` | Afficher la taille d'un fichier en format lisible (Ko/Mo/Go) | `du -h purchases.txt` |
| `wc -l <fichier>` | Compter le nombre de lignes | `wc -l purchases.txt` |
| `head -n <fichier>` | Afficher les premières lignes | `head -5 purchases.txt` |
| `awk -F'\t' '{print NF}'` | Compter le nombre de champs par ligne (homogénéité du format) | `awk -F'\t' '{print NF}' purchases.txt \| sort -u` |
| `sort` / `uniq -d` | Détecter les lignes strictement dupliquées | `sort purchases.txt \| uniq -d \| wc -l` |
| `awk -F'\t' '{print $N}' \| sort -u` | Lister les valeurs distinctes d'une colonne `N` | `awk -F'\t' '{print $6}' purchases.txt \| sort -u` |

**Justification du recours aux outils Unix.** Elles ne chargent jamais l'intégralité du fichier en mémoire d'un coup : `wc`, `awk`, `sort` traitent le flux ligne par ligne (ou en passes optimisées pour `sort`), ce qui leur permet de rester utilisables sur des fichiers de plusieurs centaines de Mo, là où un tableur charge tout en RAM avant même d'afficher quoi que ce soit. C'est un premier avant-goût, à très petite échelle, du principe qui sera généralisé avec les architectures distribuées (§1.4) : traiter la donnée par blocs plutôt que de tout charger en un bloc unique.

---

## 2. Atelier pratique (45–60 min)

### Objectif
Manipuler un jeu de données volumineux et engager une réflexion collective sur les limites d'une approche SQL classique.

### Le jeu de données du module : `purchases.txt`

L'ensemble des ateliers de ce module (et en particulier les ateliers 4, 5 et 7, consacrés à Hadoop et Spark) s'appuient sur un unique jeu de données fil rouge : **`purchases.txt`**, un historique de tickets de caisse (issu du jeu de données pédagogique *Udacity — Intro to Hadoop and MapReduce*), disponible ici :

```text
https://github.com/CodeMangler/udacity-hadoop-course/raw/master/Datasets/purchases.txt.gz
```

Chaque ligne du fichier (valeurs séparées par des tabulations) représente un achat, avec les champs suivants :

| Champ | Type | Exemple |
|---|---|---|
| `pdate` | date | 2012-01-01 |
| `ptime` | heure (chaîne, 5 caractères) | 09:00 |
| `store` | chaîne | San Jose |
| `product` | chaîne | Men's T-Shirt |
| `cost` | flottant | 16.98 |
| `payment` | chaîne | MasterCard / Visa / Amex / Discover / Cash |

### Questions de réflexion

Avant de commencer les manipulations, répondre aux questions suivantes :

- Pourquoi un tableur devient-il inadapté à certains volumes de données ?
- Dans quelles situations un SGBDR reste-t-il préférable ?
- Quels types de données produisez-vous quotidiennement avec votre téléphone ?

### Déroulé

1. **Téléchargement et décompression**
   ```bash
   wget https://github.com/CodeMangler/udacity-hadoop-course/raw/master/Datasets/purchases.txt.gz
   gunzip purchases.txt.gz
   ```

2. **Découverte du jeu de données**

   Consignes d'observation :
   - Quelle est la taille du fichier (`ls -l purchases.txt`, `wc -l purchases.txt`) ?
   - Combien de lignes, combien de colonnes ?
   - Le format est-il homogène (mêmes colonnes partout) ou varie-t-il ?
   - Y a-t-il des valeurs manquantes, des doublons, des incohérences ?

3. **Tentative d'ouverture avec des outils classiques**
   Les étudiants tentent d'ouvrir le fichier avec un tableur (Excel/LibreOffice) et observent les limites (temps de chargement, plafond de lignes, ralentissements).

4. **Discussion en petits groupes**
   > Pourquoi une base SQL classique devient-elle insuffisante ?

   Points à faire émerger :
   - le volume dépasse ce qu'une seule machine peut charger en mémoire confortablement ;
   - le format n'est pas toujours tabulaire homogène ;
   - le besoin de traiter les données rapidement (vélocité) n'est pas compatible avec un chargement manuel ;
   - la nécessité d'automatiser et de distribuer le traitement.

5. **Restitution collective**
   Chaque groupe présente une limite identifiée et la dimension du Big Data (V) à laquelle elle correspond.

---

## 3. Synthèse

- Le Big Data n'est pas une mode technologique mais une réponse à une transformation réelle du volume, de la vitesse et de la diversité des données produites.
- Les bases relationnelles restent pertinentes pour l'immense majorité des applications de gestion ; elles atteignent leurs limites face à certains volumes, certains formats, ou certaines exigences de vélocité.
- Les 5V (Volume, Vélocité, Variété, Véracité, Valeur) donnent une grille de lecture pour qualifier un problème « Big Data ».
- La réponse technique repose sur la **distribution** : répartir données et calculs sur plusieurs machines plutôt que de renforcer une seule machine.
- Tout traitement Big Data s'inscrit dans un pipeline en quatre étapes (ingestion → stockage → traitement → analyse/visualisation), et vise le plus souvent une analytique descriptive avant d'envisager le diagnostic, la prédiction ou la prescription.

Ce constat sert de fil conducteur à l'ensemble du module : NoSQL (Atelier 2) puis traitement distribué avec Spark (Ateliers 4–5), le tout appuyé sur Docker (Atelier 3) et finalement déployé sur le Cloud (Ateliers 6–7). Le jeu de données `purchases.txt` découvert ici sera repris et traité tout au long du projet fil rouge.

---

## À retenir

À l'issue de cet atelier, les notions suivantes doivent être maîtrisées :

- Big Data
- 5V
- Scale-up
- Scale-out
- Architecture distribuée
- Pipeline Big Data
- Data Engineer
- Data Analyst

---

## Questions de révision

1. Pourquoi les SGBDR atteignent-ils leurs limites face aux données massives ?
2. Expliquez la différence entre scalabilité verticale et horizontale.
3. Les technologies Big Data remplacent-elles les bases relationnelles ? Justifiez.
4. Décrivez les cinq dimensions du Big Data.
5. Quel est le rôle d'une architecture distribuée ?
6. Pourquoi la notion de Valeur est-elle considérée comme la finalité du Big Data ?

---

## Pour aller plus loin

- Gartner, définition originelle des 3V (Doug Laney, 2001), étendue depuis aux 5V.
- Comparer les architectures *scale-up* vs *scale-out*.
- Explorer les outils d'ingestion Big Data usuels (Kafka, Flume, Sqoop, NiFi) et leurs équivalents managés sur AWS (Kinesis, DMS, DataSync).

### Bibliographie

- Dean, J., & Ghemawat, S. (2008). *MapReduce: Simplified Data Processing on Large Clusters*. Communications of the ACM.
- White, T. (2021). *Hadoop: The Definitive Guide*. O'Reilly.
- Karau, H., & Warren, R. (2023). *High Performance Spark*. O'Reilly.
- Laney, D. (2001). *3D Data Management: Controlling Data Volume, Velocity and Variety*. META Group.
- AWS. *Well-Architected Framework*.
