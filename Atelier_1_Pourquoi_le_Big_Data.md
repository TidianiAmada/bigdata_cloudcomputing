# Atelier 1 — Pourquoi le Big Data ?

**Module :** Introduction au Big Data et au Cloud Computing
**Formation :** Licence Informatique 2 — SupDeCo
**Enseignant :** M. TOP
**Durée :** 2 heures

---

## Objectifs de l'atelier

À l'issue de cet atelier, l'étudiant sera capable de :

- expliquer pourquoi le volume et la diversité des données ont explosé depuis les années 2000 ;
- identifier les limites concrètes des bases de données relationnelles face aux données massives ;
- définir et illustrer les 5V du Big Data ;
- comprendre le principe général d'une architecture distribuée.

---

## 1. Rappel théorique (20–30 min)

### 1.1 L'évolution des données numériques

Jusqu'aux années 1990, l'essentiel des données produites par une organisation provenait de ses propres systèmes internes : gestion commerciale, comptabilité, ressources humaines. Ces données étaient structurées, de volume modéré, et géraient sans difficulté par des bases de données relationnelles installées sur un unique serveur.

Trois évolutions majeures ont bouleversé cette situation :

1. **La démocratisation d'Internet** : chaque page consultée, chaque clic, chaque recherche génère une trace numérique.
2. **L'essor des réseaux sociaux et du mobile** : les utilisateurs deviennent eux-mêmes producteurs de contenu (photos, vidéos, messages, géolocalisation).
3. **La multiplication des capteurs et objets connectés (IoT)** : véhicules, montres, machines industrielles produisent en continu des flux de données.

Résultat : les organisations ne traitent plus seulement des données qu'elles ont elles-mêmes saisies, mais des volumes considérables de données générées en continu, par des sources multiples, à des formats variés.

### 1.2 Les limites des bases de données relationnelles

Les systèmes de gestion de bases de données relationnelles (SGBDR — MySQL, PostgreSQL, Oracle, SQL Server) reposent sur un modèle solide : tables, schémas fixes, contraintes d'intégrité, transactions ACID (Atomicité, Cohérence, Isolation, Durabilité). Ce modèle reste excellent pour de nombreux usages, mais montre ses limites lorsque :

| Limite | Explication |
|---|---|
| **Scalabilité verticale** | Un SGBDR classique s'exécute sur une seule machine ; pour encaisser plus de charge, il faut ajouter de la RAM/CPU à cette machine (*scale-up*), ce qui a un plafond physique et financier. |
| **Schéma rigide** | Toute donnée doit correspondre à un schéma défini à l'avance (colonnes, types). Les données semi-structurées ou non structurées (JSON, texte libre, images) s'y adaptent mal. |
| **Coût des jointures à grande échelle** | Les jointures entre très grandes tables deviennent lentes et coûteuses en ressources. |
| **Disponibilité** | Une panne du serveur unique rend le système indisponible, sauf dispositifs de réplication complexes à mettre en œuvre. |

La question n'est donc pas « le relationnel est-il dépassé ? » (il ne l'est pas, il reste la norme pour la majorité des applications de gestion), mais « à partir de quel volume, de quelle vitesse ou de quelle variété de données un autre modèle devient-il nécessaire ? ».

### 1.3 Les 5V du Big Data

Le Big Data se caractérise généralement par cinq dimensions :

- **Volume** : la quantité de données produites (téraoctets, pétaoctets, voire plus), qui dépasse la capacité de traitement d'un système classique.
- **Vélocité** : la vitesse à laquelle les données sont générées et doivent être traitées (flux en temps réel, capteurs, transactions financières).
- **Variété** : la diversité des formats — données structurées (tables), semi-structurées (JSON, XML), non structurées (texte, image, vidéo, son).
- **Véracité** : la fiabilité et la qualité des données, souvent bruitées, incomplètes ou incohérentes lorsqu'elles proviennent de sources multiples.
- **Valeur** : l'objectif final — une donnée n'a d'intérêt que si l'on peut en extraire une information exploitable pour la décision.

### 1.4 Introduction aux architectures distribuées

Face à ces limites, l'approche du Big Data consiste à changer de paradigme : au lieu de renforcer une seule machine (*scale-up*), on répartit les données et les traitements sur **plusieurs machines** qui travaillent ensemble (*scale-out*, ou **scalabilité horizontale**).

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

C'est ce principe qui sous-tend l'ensemble des technologies étudiées dans ce module : NoSQL, Spark, Hadoop, et le Cloud Computing lui-même.

### 1.5 Analyse de données : quatre niveaux de valeur ajoutée

Traiter un gros volume de données n'a de sens que si l'on en tire une information exploitable (la dimension **Valeur** des 5V). On distingue classiquement quatre types d'analytique, du plus simple au plus complexe :

| Type d'analytique | Question posée | Exemple |
|---|---|---|
| **Descriptive** | Que s'est-il passé ? | Chiffre d'affaires total du mois dernier par magasin |
| **Diagnostique** | Pourquoi est-ce arrivé ? | Pourquoi les ventes ont-elles chuté dans telle région ? |
| **Prédictive** | Que va-t-il se passer ? | Prévision des ventes du mois prochain |
| **Prescriptive** | Que devons-nous faire ? | Quelles actions mettre en place pour augmenter les ventes ? |

Ce module se concentre essentiellement sur l'analytique **descriptive** (agrégations, statistiques simples) réalisée à grande échelle — c'est la brique indispensable avant d'envisager les analyses diagnostique, prédictive ou prescriptive.

### 1.6 Le pipeline Big Data et les rôles associés

Toute plateforme Big Data s'organise autour d'un même enchaînement de quatre étapes :

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

Ce module place l'étudiant successivement du côté Data Engineer (construction de la plateforme : Docker, Spark, EMR) et Data Analyst (requêtes et agrégations sur les données).

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

### Déroulé

1. **Téléchargement et décompression**
   ```bash
   wget https://github.com/CodeMangler/udacity-hadoop-course/raw/master/Datasets/purchases.txt.gz
   gunzip purchases.txt.gz
   ```

2. **Découverte du jeu de données**

   Consignes d'observation :
   - Quelle est la taille du fichier (`ls -lh purchases.txt`, `wc -l purchases.txt`) ?
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

## Pour aller plus loin

- Gartner, définition originelle des 3V (Doug Laney, 2001), étendue depuis aux 5V.
- Comparer les architectures *scale-up* vs *scale-out*.
- Explorer les outils d'ingestion Big Data usuels (Kafka, Flume, Sqoop, NiFi) et leurs équivalents managés sur AWS (Kinesis, DMS, DataSync).
