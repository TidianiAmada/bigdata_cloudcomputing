# Atelier 2 — Les bases de données NoSQL : concepts et mise en œuvre avec MongoDB

**Module :** Introduction au Big Data et au Cloud Computing

**Formation :** Licence Informatique 2 — SupDeCo

**Enseignant :** M. TOP

**Durée :** 2 heures

---

Les limites des systèmes relationnels mises en évidence dans l'atelier précédent ont conduit à l'émergence de nouvelles familles de bases de données regroupées sous l'appellation NoSQL. Cet atelier présente les principaux modèles NoSQL avant d'introduire MongoDB, une base orientée documents largement utilisée dans les architectures Big Data modernes.

---

## Objectifs de l'atelier

- expliquer les principes fondamentaux des bases de données NoSQL et en quoi elles répondent aux limites vues à l'Atelier 1 ;
- distinguer les principales familles de modèles NoSQL et leurs domaines d'application ;
- mettre en œuvre une base MongoDB dans un environnement conteneurisé (Docker) ;
- interroger une base MongoDB à l'aide des opérations de filtrage, de projection et d'agrégation sur un jeu de données JSON réel.

---

## 1. Rappel théorique (20–30 min)

### 1.1 Fondements des bases de données NoSQL

> **Définition**
>
> Les bases de données NoSQL constituent une famille de systèmes de gestion de données conçus pour répondre aux besoins de scalabilité, de flexibilité de schéma et de disponibilité rencontrés dans les applications manipulant de grands volumes de données distribuées.

NoSQL signifie *Not Only SQL*. Ces bases de données ne remplacent pas le relationnel : elles proposent des modèles alternatifs, plus flexibles, conçus pour :

- s'adapter à des données dont la structure varie ou évolue (schéma flexible, voire absence de schéma) ;
- se répartir naturellement sur plusieurs machines (scalabilité horizontale) ;
- privilégier la disponibilité et la rapidité, quitte à assouplir certaines garanties de cohérence strictes (théorème CAP).

### 1.2 SQL et NoSQL : tableau comparatif

| Critère | SQL | NoSQL |
|---|---|---|
| Structure | Schéma fixe | Schéma flexible |
| Scalabilité | Verticale (machine plus puissante) | Horizontale (ajout de nœuds) |
| Cohérence | ACID | BASE (selon les systèmes) |
| Jointures | Oui, natives | Généralement limitées ou absentes |
| Cas d'usage | Applications transactionnelles | Applications distribuées et Big Data |

### 1.3 Le théorème CAP

Dans un système distribué, on ne peut garantir simultanément que deux des trois propriétés suivantes :

- **C**ohérence (*Consistency*) : toutes les lectures renvoient la donnée la plus récente.
- **D**isponibilité (*Availability*) : chaque requête reçoit une réponse, même en cas de panne partielle.
- **P**artition Tolerance (tolérance au morcellement réseau) : le système continue de fonctionner malgré une coupure entre nœuds.

Les bases NoSQL font des choix de compromis différents selon leur conception (souvent AP ou CP), alors qu'un SGBDR classique privilégie fortement CA sur une seule machine.

> **Remarque**
>
> Le théorème CAP ne s'applique qu'aux systèmes distribués confrontés à une partition réseau. En l'absence de partition, un système peut simultanément assurer cohérence et disponibilité.

### 1.4 Les quatre grandes familles de modèles NoSQL

| Modèle | Unité de stockage | Exemple de SGBD | Cas d'usage typique |
|---|---|---|---|
| **Clé-valeur** | Une clé associée à une valeur opaque | Redis, DynamoDB | Cache, sessions utilisateurs, compteurs |
| **Orienté documents** | Documents semi-structurés (souvent JSON/BSON) | **MongoDB**, CouchDB | Catalogues produits, contenus, profils utilisateurs |
| **Orienté colonnes** | Familles de colonnes stockées ensemble | Cassandra, HBase | Séries temporelles, très gros volumes en écriture |
| **Orienté graphes** | Nœuds et relations | Neo4j | Réseaux sociaux, recommandations, détection de fraude |

Aucun de ces modèles n'est supérieur aux autres de manière absolue. Chacun répond à une catégorie particulière de problèmes.

### 1.5 Choix de MongoDB comme support pédagogique

MongoDB est une base orientée **documents** : chaque enregistrement est un document au format BSON (proche de JSON), regroupé dans des **collections** (équivalent souple d'une table). Elle est représentative des bases NoSQL les plus utilisées en entreprise et illustre bien :

- l'absence de schéma fixe (deux documents d'une même collection peuvent avoir des champs différents) ;
- la possibilité d'imbriquer des structures complexes (sous-documents, tableaux) sans jointure — un pays peut embarquer directement son tableau de devises ou de langues, là où le relationnel exigerait des tables séparées reliées par des clés étrangères ;
- une syntaxe de requête proche de JSON, facile à prendre en main.

> **Remarque**
>
> L'absence de schéma ne signifie pas l'absence de structure. Dans les applications réelles, les documents MongoDB suivent généralement un modèle de données défini par les développeurs.

Correspondance de vocabulaire SQL ↔ MongoDB :

| SQL | MongoDB |
|---|---|
| Base de données | Base de données (*database*) |
| Table | Collection |
| Ligne | Document |
| Colonne | Champ (*field*) |
| Clé primaire | `_id` |

### 1.6 Le format BSON

Bien que les documents soient manipulés sous forme JSON, MongoDB les stocke en réalité au format **BSON** (*Binary JSON*), une représentation binaire optimisée permettant notamment la gestion de types supplémentaires (`Date`, `ObjectId`, données binaires, etc.) et un accès plus rapide aux champs.

### 1.7 Principes de modélisation dans MongoDB

Concevoir une base documentaire ne se limite pas à savoir l'interroger : il faut aussi choisir comment structurer les documents. Deux approches principales s'opposent :

- **L'imbrication (*embedding*)** : les données liées sont stockées directement dans le document parent (par exemple les devises et langues d'un pays, comme dans `countries.json`). Cette approche favorise la lecture en une seule requête mais duplique l'information et peut alourdir les documents.
- **La référence (*referencing*)** : les données liées sont stockées dans une collection séparée et reliées par un identifiant (à la manière d'une clé étrangère). Cette approche évite la duplication mais nécessite des requêtes supplémentaires (ou l'opérateur `$lookup`) pour reconstituer l'information.

Le choix entre ces deux approches dépend principalement :

- de la fréquence de lecture conjointe des données (l'imbrication favorise les lectures groupées) ;
- de la fréquence de mise à jour des données liées (la référence évite de propager un changement dans de nombreux documents) ;
- du volume des données imbriquées (un tableau qui peut croître indéfiniment est un mauvais candidat à l'imbrication).

Dans le jeu de données `countries.json` utilisé dans cet atelier, les devises et langues sont imbriquées car elles sont peu nombreuses, rarement modifiées et systématiquement lues avec le pays.

### 1.8 Référence rapide des commandes MongoDB

Les commandes suivantes constituent un socle minimal pour la manipulation de MongoDB, de la mise en route du serveur jusqu'à l'interrogation des données, dans `mongosh`.

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

### 2.2 Présentation du jeu de données utilisé : `countries.json`

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

- premier argument : critère de sélection (le filtre) ;
- second argument : projection des champs à retourner ;
- `_id: 0` masque l'identifiant généré automatiquement (`1` inclut un champ, `0` l'exclut).

Équivalent SQL de la requête `find` ci-dessus :

```sql
SELECT name
FROM countries
WHERE region = 'Africa';
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

- `$group` : chaque étage reçoit les documents de l'étage précédent ; ici on regroupe par `region` (`_id: "$region"`) et on calcule un total par groupe avec `$sum` ;
- `$sort` : trie les groupes obtenus par population totale décroissante (`-1`).

Équivalent SQL de cette agrégation :

```sql
SELECT region, SUM(population) AS totalPopulation
FROM countries
GROUP BY region
ORDER BY totalPopulation DESC;
```

### Consignes

1. Importer `countries.json` dans `geodb.countries` et vérifier le nombre de documents (250).
2. Exécuter les dix requêtes ci-dessus et noter les résultats.
3. Pour la requête 3.5, identifier dans les résultats quels pays utilisent chaque variante du Franc CFA (Ouest vs Centre Africain).
4. Modifier la requête 3.8 pour l'appliquer à une autre région (par exemple « Europe ») et comparer les résultats.
5. **Requête libre** : proposer, en groupe, une nouvelle requête répondant à une question métier de votre choix, par exemple :
   - Quels pays utilisent l'euro ?
   - Quels pays comptent plus de 100 millions d'habitants ?
   - Quelle région possède le plus grand nombre de pays ?
   - Quels pays possèdent plusieurs langues officielles ?

---

## Erreurs fréquentes

- oublier `_id: 0` dans la projection, qui fait alors apparaître l'identifiant généré automatiquement ;
- confondre `find()` (plusieurs documents) et `findOne()` (un seul document) ;
- oublier que `aggregate()` attend un tableau d'étages (`[ ... ]`), même s'il n'y en a qu'un seul ;
- utiliser `$sum` (ou un autre opérateur d'agrégat) en dehors d'un `$group`.

---

## 4. Synthèse

- NoSQL n'est pas un substitut universel au SQL mais une famille de réponses adaptées à des besoins spécifiques (flexibilité de schéma, scalabilité horizontale, disponibilité).
- MongoDB (modèle documents) permet de stocker des données semi-structurées, y compris des structures imbriquées (tableaux d'objets comme `currencies` ou `languages`), sans schéma rigide ni jointure.
- `distinct`, `find` (avec filtre et projection) et `aggregate` (`$match`, `$group`, `$sum`, `$sort`) couvrent l'essentiel des besoins d'interrogation courants, du simple filtrage jusqu'à l'agrégation par groupe.
- Le théorème CAP explique pourquoi les bases distribuées doivent arbitrer entre cohérence stricte et disponibilité.
- Le lancement de MongoDB via Docker préfigure l'approche utilisée pour l'ensemble des composants du module (Atelier 3).

### Synthèse visuelle

```
Relationnel
      │
      ▼
Limites du SQL
      │
      ▼
NoSQL
      │
      ▼
4 familles (clé-valeur, documents, colonnes, graphes)
      │
      ▼
MongoDB
      │
      ▼
Documents BSON (embedding / referencing)
      │
      ▼
find() · distinct() · aggregate()
```

---

## Pour aller plus loin

- Comparer MongoDB avec une base orientée colonnes (Cassandra) sur un même cas d'usage.
- Explorer les index MongoDB (`createIndex`) pour optimiser les requêtes sur `region` ou `population`.
- Réécrire la requête 3.10 avec `$avg` au lieu de `$sum` pour obtenir la population moyenne par région.
- Réfléchir à la modélisation d'un cas où la référence (*referencing*) serait préférable à l'imbrication (par exemple une collection `countries` référençant une collection `organisations_internationales`).

---

## Bibliographie

- Kleppmann, M. (2017). *Designing Data-Intensive Applications*. O'Reilly.
- Chodorow, K. (2019). *MongoDB: The Definitive Guide*. O'Reilly.
- Brewer, E. (2012). *CAP Twelve Years Later: How the Rules Have Changed*. *Computer*, 45(2), 23–29.
- MongoDB, Inc. *MongoDB Manual*. https://www.mongodb.com/docs/manual/
