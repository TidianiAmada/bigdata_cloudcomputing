# Solution — Atelier 2 : NoSQL avec MongoDB

Corrigé de l'atelier pratique (partie 3 de [Atelier_2_NoSQL_MongoDB.md](../Atelier_2_NoSQL_MongoDB.md)), exécuté réellement sur un conteneur MongoDB avec `countries.json`.

---

## 0. Environnement

```bash
docker run -d --name mongodb -p 27017:27017 mongo:latest
docker cp countries.json mongodb:/tmp/countries.json
docker exec -it mongodb mongoimport --db geodb --collection countries --jsonArray --file /tmp/countries.json
```

```text
250 document(s) imported successfully. 0 document(s) failed to import.
```

Vérification :

```javascript
db.countries.countDocuments()   // → 250
```

Les 250 documents attendus sont bien présents.

> Remarque environnement Windows/Git Bash : si `docker exec ... --file /tmp/countries.json` échoue avec une erreur du type `open C:/Users/.../countries.json: no such file or directory`, c'est que Git Bash réécrit automatiquement les chemins Unix en chemins Windows avant de les transmettre au conteneur. Il suffit de désactiver cette conversion pour la commande : `MSYS_NO_PATHCONV=1 docker exec ...`.

---

## 3.1 Liste des régions du monde

```javascript
db.countries.distinct("region")
```

```text
[ '', 'Africa', 'Americas', 'Asia', 'Europe', 'Oceania', 'Polar' ]
```

→ 6 régions réelles, plus une valeur vide `''` (quelques territoires du jeu de données n'ont pas de région renseignée) : premier exemple concret d'incohérence de données (dimension **Véracité**, cf. Atelier 1).

## 3.2 Liste des sous-régions

```javascript
db.countries.distinct("subregion")
```

```text
[
  '', 'Australia and New Zealand', 'Caribbean', 'Central America', 'Central Asia',
  'Eastern Africa', 'Eastern Asia', 'Eastern Europe', 'Melanesia', 'Micronesia',
  'Middle Africa', 'Northern Africa', 'Northern America', 'Northern Europe',
  'Polynesia', 'South America', 'South-Eastern Asia', 'Southern Africa',
  'Southern Asia', 'Southern Europe', 'Western Africa', 'Western Asia', 'Western Europe'
]
```

→ 22 sous-régions distinctes (hors valeur vide).

## 3.3 Région et sous-région du Sénégal

```javascript
db.countries.findOne({ name: "Senegal" }, { region: 1, subregion: 1, _id: 0 })
```

```json
{ "region": "Africa", "subregion": "Western Africa" }
```

→ Sénégal : région **Africa**, sous-région **Western Africa**.

## 3.4 Liste des pays de la région « Africa »

```javascript
db.countries.distinct("name", { region: "Africa" })
```

→ **60 pays/territoires**, par exemple : Algeria, Angola, Benin, Botswana, ..., Senegal, ..., Zimbabwe (liste complète de 60 entrées, incluant des territoires comme *British Indian Ocean Territory*, *Mayotte*, *Réunion*).

## 3.5 Pays d'Afrique dont la monnaie est le Franc CFA

```javascript
db.countries.find(
  { region: "Africa", "currencies.name": { $in: ["West African CFA franc", "Central African CFA franc"] } },
  { name: 1, _id: 0 }
)
```

**14 pays** au total, répartis en deux zones monétaires :

| Zone | Pays |
|---|---|
| **Franc CFA d'Afrique de l'Ouest** (West African CFA franc) — 8 pays | Bénin, Burkina Faso, Guinée-Bissau, Côte d'Ivoire, Mali, Niger, Togo, Sénégal |
| **Franc CFA d'Afrique Centrale** (Central African CFA franc) — 6 pays | Cameroun, République Centrafricaine, Congo, Tchad, Guinée Équatoriale, Gabon |

## 3.6 Nombre de pays de la région « Africa »

```javascript
db.countries.countDocuments({ region: "Africa" })
```

```text
60
```

→ cohérent avec le résultat de 3.4.

## 3.7 Capitale et population du Gabon

```javascript
db.countries.findOne({ name: "Gabon" }, { capital: 1, population: 1, _id: 0 })
```

```json
{ "capital": "Libreville", "population": 1802278 }
```

## 3.8 Pays d'Afrique de moins de 8 millions d'habitants

```javascript
db.countries.find(
  { region: "Africa", population: { $lt: 8000000 } },
  { name: 1, capital: 1, population: 1, _id: 0 }
)
```

→ **28 pays/territoires** sur les 60 de la région. Les plus peuplés qui passent tout de même sous le seuil : Sierra Leone (7 075 641 hab.) et Togo (7 143 000 hab.) ; les plus petits : French Southern Territories (140 hab.), Saint Helena, Ascension and Tristan da Cunha (4 255 hab.), British Indian Ocean Territory (3 000 hab. — une base militaire, pas un pays peuplé au sens usuel).

## 3.9 Nombre total d'habitants de la région Afrique

```javascript
db.countries.aggregate([
  { $match: { region: "Africa" } },
  { $group: { _id: null, totalPopulation: { $sum: "$population" } } }
])
```

```json
{ "_id": null, "totalPopulation": 1185705747 }
```

→ environ **1,186 milliard** d'habitants pour l'ensemble de la région Afrique dans ce jeu de données.

## 3.10 Nombre total d'habitants par région

```javascript
db.countries.aggregate([
  { $group: { _id: "$region", totalPopulation: { $sum: "$population" } } },
  { $sort: { totalPopulation: -1 } }
])
```

| Région | Population totale |
|---|---|
| Asia | 4 386 254 784 |
| Africa | 1 185 705 747 |
| Americas | 990 317 681 |
| Europe | 746 688 182 |
| Oceania | 40 169 837 |
| Polar | 1 000 |
| *(vide)* | 0 |

→ l'Asie domine très largement, conforme à la réalité démographique mondiale ; la ligne « vide » confirme à nouveau les quelques documents sans région renseignée.

---

## Consignes complémentaires

### 3. Répartition Ouest/Centre du Franc CFA (déjà détaillée en 3.5)

8 pays utilisent le **franc CFA d'Afrique de l'Ouest** (zone UEMOA) et 6 le **franc CFA d'Afrique Centrale** (zone CEMAC) — deux monnaies distinctes bien que portant un nom proche, un piège classique si l'on filtre uniquement sur `"currencies.code": "XOF"` sans distinguer les deux zones.

### 4. Requête 3.8 appliquée à l'Europe

```javascript
db.countries.find(
  { region: "Europe", population: { $lt: 8000000 } },
  { name: 1, capital: 1, population: 1, _id: 0 }
)
```

→ **34 pays/territoires** européens de moins de 8 millions d'habitants (contre 28 pour l'Afrique), par exemple l'Albanie, l'Andorre, la Bulgarie, le Danemark, l'Islande, l'Irlande, le Luxembourg, Monaco, la Norvège, la Slovénie, etc. — cela illustre que l'Europe compte proportionnellement beaucoup plus de petits États (micro-États comme Andorre, Monaco, Saint-Marin, le Liechtenstein) que l'Afrique, malgré un nombre de pays du même ordre de grandeur.

### Pour aller plus loin : `$avg` au lieu de `$sum`

```javascript
db.countries.aggregate([
  { $group: { _id: "$region", avgPopulation: { $avg: "$population" } } },
  { $sort: { avgPopulation: -1 } }
])
```

| Région | Population moyenne par pays |
|---|---|
| Asia | ≈ 87 725 096 |
| Africa | ≈ 19 761 762 |
| Americas | ≈ 17 373 994 |
| Europe | ≈ 14 088 456 |
| Oceania | ≈ 1 487 772 |
| Polar | 1 000 |

**Observation clé** : le classement change entre `$sum` (3.10) et `$avg`. Le `$sum` reflète le poids démographique brut d'une région (l'Asie et l'Afrique dominent grâce à des pays très peuplés comme la Chine, l'Inde, le Nigeria). Le `$avg` révèle une information différente — ici la taille moyenne des pays qui composent chaque région — et confirme que l'Europe, bien que 3ᵉ en population totale, contient beaucoup de très petits États qui tirent sa moyenne vers le bas par rapport à l'Afrique.

---

## Synthèse technique

- `distinct` : lister les valeurs uniques d'un champ, avec filtre optionnel (3.1, 3.2, 3.4).
- `findOne` / `find` + projection (`{ champ: 1, _id: 0 }`) : lire un ou plusieurs documents en ne renvoyant que certains champs (3.3, 3.7).
- Filtrage sur un champ imbriqué d'un tableau (`"currencies.name"`) sans jointure (3.5) — impossible aussi simplement en SQL relationnel classique sans une table `currencies` séparée.
- `countDocuments` : compter sans rapatrier les documents (3.6).
- `aggregate` avec `$match`, `$group`, `$sum`, `$avg`, `$sort` : le pipeline d'agrégation, équivalent MongoDB d'un `GROUP BY` SQL (3.9, 3.10, et l'extension `$avg`).

Ce même pipeline d'agrégation (`$match → $group → $sort`) est directement transposable au vocabulaire Spark (`filter → groupBy → agg → orderBy`) qui sera utilisé aux ateliers 4 et 5 sur `purchases.txt`.
