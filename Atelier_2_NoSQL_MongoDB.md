# Atelier 2 — NoSQL avec MongoDB

**Module :** Introduction au Big Data et au Cloud Computing
**Formation :** Licence Informatique 2 — SupDeCo
**Enseignant :** M. TOP
**Durée :** 2 heures

---

## Objectifs de l'atelier

- comprendre le principe des bases de données NoSQL et en quoi elles répondent aux limites vues à l'Atelier 1 ;
- distinguer les quatre grandes familles de modèles NoSQL ;
- lancer et manipuler une base MongoDB dans un environnement conteneurisé (Docker) ;
- importer un jeu de données JSON réel et effectuer des requêtes de filtrage, de projection et d'agrégation.

---

## 1. Rappel théorique (20–30 min)

### 1.1 Pourquoi NoSQL ?

NoSQL signifie *Not Only SQL*. Ces bases de données ne remplacent pas le relationnel : elles proposent des modèles alternatifs, plus flexibles, conçus pour :

- s'adapter à des données dont la structure varie ou évolue (schéma flexible, voire absence de schéma) ;
- se répartir naturellement sur plusieurs machines (scalabilité horizontale) ;
- privilégier la disponibilité et la rapidité, quitte à assouplir certaines garanties de cohérence strictes (théorème CAP).

### 1.2 Le théorème CAP (notion clé)

Dans un système distribué, on ne peut garantir simultanément que deux des trois propriétés suivantes :

- **C**ohérence (*Consistency*) : toutes les lectures renvoient la donnée la plus récente.
- **D**isponibilité (*Availability*) : chaque requête reçoit une réponse, même en cas de panne partielle.
- **P**artition Tolerance (tolérance au morcellement réseau) : le système continue de fonctionner malgré une coupure entre nœuds.

Les bases NoSQL font des choix de compromis différents selon leur conception (souvent AP ou CP), alors qu'un SGBDR classique privilégie fortement CA sur une seule machine.

### 1.3 Les quatre grandes familles de modèles NoSQL

| Modèle | Unité de stockage | Exemple de SGBD | Cas d'usage typique |
|---|---|---|---|
| **Clé-valeur** | Une clé associée à une valeur opaque | Redis, DynamoDB | Cache, sessions utilisateurs, compteurs |
| **Orienté documents** | Documents semi-structurés (souvent JSON/BSON) | **MongoDB**, CouchDB | Catalogues produits, contenus, profils utilisateurs |
| **Orienté colonnes** | Familles de colonnes stockées ensemble | Cassandra, HBase | Séries temporelles, très gros volumes en écriture |
| **Orienté graphes** | Nœuds et relations | Neo4j | Réseaux sociaux, recommandations, détection de fraude |

### 1.4 Pourquoi MongoDB en atelier

MongoDB est une base orientée **documents** : chaque enregistrement est un document au format BSON (proche de JSON), regroupé dans des **collections** (équivalent souple d'une table). Elle est représentative des bases NoSQL les plus utilisées en entreprise et illustre bien :

- l'absence de schéma fixe (deux documents d'une même collection peuvent avoir des champs différents) ;
- la possibilité d'imbriquer des structures complexes (sous-documents, tableaux) sans jointure — un pays peut embarquer directement son tableau de devises ou de langues, là où le relationnel exigerait des tables séparées reliées par des clés étrangères ;
- une syntaxe de requête proche de JSON, facile à prendre en main.

Correspondance de vocabulaire SQL ↔ MongoDB :

| SQL | MongoDB |
|---|---|
| Base de données | Base de données (*database*) |
| Table | Collection |
| Ligne | Document |
| Colonne | Champ (*field*) |
| Clé primaire | `_id` |

### 1.5 Commandes et opérateurs MongoDB essentiels

Ces commandes couvrent l'essentiel de ce dont un étudiant a besoin pour suivre l'atelier — de la mise en route du serveur jusqu'à l'interrogation des données, dans `mongosh`.

**Mise en route (ligne de commande, hors `mongosh`) :**

| Commande | Rôle |
|---|---|
| `docker run -d --name mongodb -p 27017:27017 mongo:latest` | Démarrer un serveur MongoDB dans un conteneur |
| `docker exec -it mongodb mongosh` | Ouvrir un shell `mongosh` interactif dans le conteneur |
| `mongoimport --db <db> --collection <coll> --jsonArray --file <fichier>.json` | Importer un fichier JSON (tableau d'objets) dans une collection |

**Navigation (dans `mongosh`) :**

| Commande | Rôle |
|---|---|
| `show dbs` | Lister les bases de données existantes |
| `use <db>` | Basculer sur une base (la crée si elle n'existe pas encore) |
| `show collections` | Lister les collections de la base courante |

**Lecture et filtrage :**

| Commande | Rôle |
|---|---|
| `db.coll.countDocuments(<filtre>)` | Compter les documents (avec filtre optionnel) |
| `db.coll.distinct("champ", <filtre>)` | Lister les valeurs uniques d'un champ |
| `db.coll.findOne(<filtre>, <projection>)` | Récupérer un seul document |
| `db.coll.find(<filtre>, <projection>)` | Récupérer plusieurs documents |

Opérateurs de filtre courants : `$eq` (implicite), `$lt` / `$lte` / `$gt` / `$gte` (comparaison), `$in` (appartenance à une liste), et la notation pointée (`"currencies.name"`) pour filtrer sur un champ imbriqué dans un tableau.

**Agrégation (`db.coll.aggregate([ ... ])`)**, un pipeline d'étages enchaînés :

| Étage | Rôle | Équivalent SQL |
|---|---|---|
| `$match` | Filtrer les documents | `WHERE` |
| `$group` | Regrouper par valeur de `_id` et calculer des agrégats | `GROUP BY` |
| `$sum` / `$avg` / `$min` / `$max` | Fonctions d'agrégat utilisées dans `$group` | `SUM`/`AVG`/`MIN`/`MAX` |
| `$sort` | Trier les résultats (`1` croissant, `-1` décroissant) | `ORDER BY` |

**Écriture (pour aller plus loin) :**

| Commande | Rôle |
|---|---|
| `db.coll.insertOne(<document>)` | Insérer un document |
| `db.coll.createIndex({champ: 1})` | Créer un index pour accélérer les requêtes sur ce champ |

---

## 2. Démonstration guidée (20 min)

### 2.1 Lancer MongoDB avec Docker

```bash
docker run -d \
  --name mongodb \
  -p 27017:27017 \
  -v mongo_data:/data/db \
  mongo:latest
```

- `-d` : exécution en arrière-plan (*detached*).
- `-p 27017:27017` : expose le port par défaut de MongoDB.
- `-v mongo_data:/data/db` : volume nommé pour persister les données au-delà du cycle de vie du conteneur.

Vérification :

```bash
docker ps
docker exec -it mongodb mongosh
```

### 2.2 Le jeu de données de l'atelier : `countries.json`

Le jeu de données utilisé dans cet atelier est **`countries.json`**, un fichier contenant les fiches descriptives de 250 pays du monde (nom, capitale, région, population, devises, langues, codes, etc.). Chaque pays est un document JSON assez riche, avec des champs imbriqués (tableaux d'objets pour les devises et les langues) — un bon exemple de ce que le modèle documents de MongoDB sait représenter nativement, sans avoir besoin de plusieurs tables reliées par des jointures.

Extrait simplifié d'un document (Sénégal) :

```json
{
  "name": "Senegal",
  "capital": "Dakar",
  "region": "Africa",
  "subregion": "Western Africa",
  "population": 14799859,
  "currencies": [
    { "code": "XOF", "name": "West African CFA franc", "symbol": "Fr" }
  ],
  "languages": [
    { "iso639_1": "fr", "name": "French", "nativeName": "français" }
  ]
}
```

### 2.3 Import du jeu de données

Copie du fichier dans le conteneur puis import dans la base `geodb`, collection `countries` :

```bash
docker cp countries.json mongodb:/tmp/countries.json

docker exec -it mongodb mongoimport \
  --db geodb \
  --collection countries \
  --jsonArray \
  --file /tmp/countries.json
```

Vérification du nombre de documents importés (250 attendus) :

```javascript
use geodb
db.countries.countDocuments()
```

---

## 3. Atelier pratique (60–75 min)

### Objectif
Exprimer, en MongoDB, dix requêtes de complexité croissante (liste de valeurs distinctes, filtrage, projection, agrégation) sur le jeu de données `countries`.

Toutes les commandes ci-dessous s'exécutent dans `mongosh` (ou dans Robo3T/MongoDB Compass), connecté à la base `geodb`.

### 3.1 La liste des régions du monde

```javascript
db.countries.distinct("region")
```

### 3.2 La liste des sous-régions (`subregion`) du monde

```javascript
db.countries.distinct("subregion")
```

### 3.3 La région et la sous-région du Sénégal

```javascript
db.countries.findOne(
  { name: "Senegal" },
  { region: 1, subregion: 1, _id: 0 }
)
```

### 3.4 La liste des noms des pays de la région « Africa »

```javascript
db.countries.find(
  { region: "Africa" },
  { name: 1, _id: 0 }
)

// équivalent plus direct, sous forme de tableau de valeurs :
db.countries.distinct("name", { region: "Africa" })
```

### 3.5 Les pays d'Afrique dont la monnaie est le Franc CFA (Ouest ou Centre Africain)

Le champ `currencies` est un tableau d'objets (`{ code, name, symbol }`) : on peut filtrer directement sur un champ imbriqué de ce tableau, sans jointure.

```javascript
db.countries.find(
  {
    region: "Africa",
    "currencies.name": { $in: ["West African CFA franc", "Central African CFA franc"] }
  },
  { name: 1, _id: 0 }
)
```

### 3.6 Le nombre de pays de la région « Africa »

```javascript
db.countries.countDocuments({ region: "Africa" })
```

### 3.7 La capitale du Gabon et le nombre d'habitants

```javascript
db.countries.findOne(
  { name: "Gabon" },
  { capital: 1, population: 1, _id: 0 }
)
```

### 3.8 Pays d'Afrique de moins de 8 millions d'habitants (nom, capitale, population)

```javascript
db.countries.find(
  { region: "Africa", population: { $lt: 8000000 } },
  { name: 1, capital: 1, population: 1, _id: 0 }
)
```

### 3.9 Le nombre total d'habitants de la région Afrique

```javascript
db.countries.aggregate([
  { $match: { region: "Africa" } },
  { $group: { _id: null, totalPopulation: { $sum: "$population" } } }
])
```

### 3.10 Le nombre total d'habitants par région dans le monde

```javascript
db.countries.aggregate([
  { $group: { _id: "$region", totalPopulation: { $sum: "$population" } } },
  { $sort: { totalPopulation: -1 } }
])
```

### Consignes

1. Importer `countries.json` dans `geodb.countries` et vérifier le nombre de documents (250).
2. Exécuter les dix requêtes ci-dessus et noter les résultats.
3. Pour la requête 3.5, identifier dans les résultats quels pays utilisent chaque variante du Franc CFA (Ouest vs Centre Africain).
4. Modifier la requête 3.8 pour l'appliquer à une autre région (par exemple « Europe ») et comparer les résultats.

---

## 4. Synthèse

- NoSQL n'est pas un substitut universel au SQL mais une famille de réponses adaptées à des besoins spécifiques (flexibilité de schéma, scalabilité horizontale, disponibilité).
- MongoDB (modèle documents) permet de stocker des données semi-structurées, y compris des structures imbriquées (tableaux d'objets comme `currencies` ou `languages`), sans schéma rigide ni jointure.
- `distinct`, `find` (avec filtre et projection) et `aggregate` (`$match`, `$group`, `$sum`, `$sort`) couvrent l'essentiel des besoins d'interrogation courants, du simple filtrage jusqu'à l'agrégation par groupe.
- Le théorème CAP explique pourquoi les bases distribuées doivent arbitrer entre cohérence stricte et disponibilité.
- Le lancement de MongoDB via Docker préfigure l'approche utilisée pour l'ensemble des composants du module (Atelier 3).

---

## Pour aller plus loin

- Comparer MongoDB avec une base orientée colonnes (Cassandra) sur un même cas d'usage.
- Explorer les index MongoDB (`createIndex`) pour optimiser les requêtes sur `region` ou `population`.
- Réécrire la requête 3.10 avec `$avg` au lieu de `$sum` pour obtenir la population moyenne par région.
