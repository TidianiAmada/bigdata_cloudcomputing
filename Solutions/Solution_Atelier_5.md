# Solution — Atelier 5 : Spark SQL et introduction à Hive

Corrigé de l'atelier pratique (parties 2 et 3 de [Atelier_5_SparkSQL_Hive.md](../Atelier_5_SparkSQL_Hive.md)).

**Portée de ce corrigé** : la Partie 2 (Spark SQL) est exécutée réellement, dans les mêmes conditions que l'[Atelier 4](Solution_Atelier_4.md) (conteneur `apache/spark-py`, `purchases.txt` complet). La Partie 1 (Hive) suppose explicitement un cluster **Amazon EMR** déjà provisionné (`Cette partie se déroule sur le cluster EMR`, énoncé §2) : c'est une ressource cloud payante, dont la création ne se décide pas silencieusement en marge d'un exercice — elle est traitée en détail à l'Atelier 7. Ce corrigé donne donc, pour la partie Hive, le déroulé exact à reproduire une fois le cluster EMR disponible, et les résultats de référence à attendre — qui sont **nécessairement identiques** à ceux de Spark SQL puisque HiveQL et Spark SQL interrogent ici la même donnée avec la même requête logique (c'est d'ailleurs l'objet de la consigne 4 de l'atelier).

---

## Partie 1 — Hive (sur cluster EMR)

### Déroulé à reproduire sur EMR

```bash
# Sur le noeud maitre EMR, purchases.txt deja depose sur HDFS (cf. Atelier 4) :
hdfs dfs -put purchases.txt /user/hadoop/purchases_data/

beeline -u jdbc:hive2://localhost:10000
```

```sql
!record tp_hive.txt

CREATE DATABASE achatdb;
USE achatdb;

CREATE TABLE purchases (
    pdate    DATE,
    ptime    CHAR(5),
    store    STRING,
    product  STRING,
    cost     FLOAT,
    payment  STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/hadoop/purchases_data';
```

### Requêtes 1 à 4 et résultats de référence

Les quatre requêtes HiveQL de l'énoncé (§2.3 à §2.6) sont logiquement équivalentes à celles exécutées en Spark SQL ci-dessous — mêmes clauses `GROUP BY`/`ORDER BY`/`CASE WHEN`, sur la même donnée source. Les résultats à attendre sont donc ceux obtenus (réellement, par exécution) dans la Partie 2 :

| Requête | Résultat attendu |
|---|---|
| 1. Total ventes par magasin | Top 3 : Philadelphia (10 190 080,26 $), Durham (10 153 890,21 $), Laredo (10 144 604,98 $) |
| 2. Ventes par tranche horaire | 9 tranches actives (09h à 17h), ~459-460k ventes/heure |
| 3. Montant par mode de paiement | Cash en tête (207 245 078,69 $), quasi-égalité avec les 4 autres modes |
| 4. Taux cash / électronique | 20,03 % cash / 79,97 % électronique |

Le détail chiffré complet (tableaux, tous les magasins, tous les modes de paiement) figure dans la Partie 2 ci-dessous et dans le [corrigé de l'Atelier 4](Solution_Atelier_4.md), qui utilise exactement la même donnée.

**Différence pratique à connaître** : sur Hive, chaque requête `SELECT` est compilée en un plan d'exécution soumis au moteur configuré (historiquement MapReduce, potentiellement Tez ou Spark selon la configuration du cluster EMR) — la requête 1 illustre bien cela en écrivant explicitement son résultat sur HDFS via `INSERT OVERWRITE DIRECTORY` plutôt que de l'afficher directement, une différence de style héritée de l'origine batch/HDFS de Hive, alors que Spark SQL (`.show()`) affiche par défaut dans la session interactive.

### Interfaces web (rappel)

Une fois le cluster EMR lancé (Atelier 7), Hue (`:8888`) permet de rejouer ces mêmes requêtes sans ligne de commande, et d'exporter directement les résultats en CSV pour les visualisations demandées (histogramme pour la requête 2, diagrammes circulaires pour les requêtes 3 et 4).

---

## Partie 2 — Spark SQL sur `purchases.txt` (exécution réelle)

```python
schema = "pdate DATE, ptime STRING, store STRING, product STRING, cost DOUBLE, payment STRING"
df = spark.read.csv("/data/purchases.txt", sep="\t", schema=schema)
df.createOrReplaceTempView("purchases")
```

### Question 1 — Total des ventes par magasin

```python
spark.sql("""
    SELECT store, SUM(cost) AS total_ventes
    FROM purchases
    GROUP BY store
    ORDER BY total_ventes DESC
""").show(10)
```

```text
+------------+--------------------+
|       store|        total_ventes|
+------------+--------------------+
|Philadelphia|1.0190080260000013E7|
|      Durham|1.0153890209999997E7|
|      Laredo|1.0144604979999991E7|
|      Newark|1.0144052800000003E7|
|  Cincinnati|1.0139505740000002E7|
|  Washington|1.0139363389999997E7|
|      Irving|1.0133944080000008E7|
|  Fort Wayne|1.0132594020000003E7|
| Baton Rouge|1.0131273230000002E7|
|  Sacramento|1.0123468180000002E7|
+------------+--------------------+
```

→ **Identique** au top 10 obtenu en RDD et en DataFrame à l'Atelier 4 (Philadelphia en tête à 10 190 080,26 $), aux notations scientifiques de `.show()` près — c'est la vérification demandée par la consigne 4 de l'atelier.

### Question 2 — Nombre de ventes par tranche horaire

```python
spark.sql("""
    SELECT SUBSTRING(ptime, 1, 2) AS heure, COUNT(*) AS nb_ventes
    FROM purchases
    GROUP BY SUBSTRING(ptime, 1, 2)
    ORDER BY heure
""").show(24)
```

```text
+-----+---------+
|heure|nb_ventes|
+-----+---------+
|   09|   459672|
|   10|   459886|
|   11|   459332|
|   12|   459906|
|   13|   459772|
|   14|   459873|
|   15|   459531|
|   16|   460466|
|   17|   460038|
+-----+---------+
```

### Question 3 — Montant total par mode de paiement

```python
spark.sql("""
    SELECT payment, SUM(cost) AS montant_total
    FROM purchases
    GROUP BY payment
    ORDER BY montant_total DESC
""").show()
```

```text
+----------+--------------------+
|   payment|       montant_total|
+----------+--------------------+
|      Cash|2.0724507868999982E8|
|MasterCard|2.0701152435000026E8|
|  Discover|2.0686962147999936E8|
|      Visa| 2.067033619700003E8|
|      Amex|2.0662836676999962E8|
+----------+--------------------+
```

### Question 4 — Taux de paiement cash / électronique

```python
spark.sql("""
    SELECT
        CASE WHEN payment = 'Cash' THEN 'cash' ELSE 'electronique' END AS categorie,
        COUNT(*) * 100.0 / (SELECT COUNT(*) FROM purchases) AS taux_pct
    FROM purchases
    GROUP BY CASE WHEN payment = 'Cash' THEN 'cash' ELSE 'electronique' END
""").show()
```

```text
+------------+-----------------+
|   categorie|         taux_pct|
+------------+-----------------+
|electronique|79.97402908703590|
|        cash|20.02597091296410|
+------------+-----------------+
```

### Bonus — Requête pivot (magasin × mode de paiement)

```python
spark.sql("""
    SELECT *
    FROM (
        SELECT store, payment FROM purchases
    )
    PIVOT (
        COUNT(*) FOR payment IN ('Cash', 'Amex', 'Visa', 'MasterCard', 'Discover')
    )
    ORDER BY store
""").show(10)
```

```text
+-----------+----+----+----+----------+--------+
|      store|Cash|Amex|Visa|MasterCard|Discover|
+-----------+----+----+----+----------+--------+
|Albuquerque|8165|7987|8020|      8118|    8055|
|    Anaheim|7946|8003|8073|      7993|    8071|
|  Anchorage|7978|7938|8082|      7963|    7845|
|  Arlington|8164|7909|7958|      8047|    8270|
|    Atlanta|8175|7928|7939|      7831|    8295|
|     Aurora|7936|7942|8000|      7953|    7977|
|     Austin|8113|7949|8127|      8088|    8055|
|Bakersfield|8140|8189|7949|      7946|    8102|
|  Baltimore|8172|7858|8125|      8194|    7847|
|Baton Rouge|8109|8016|8137|      8022|    8103|
+-----------+----+----+----+----------+--------+
```

→ Pour chaque magasin, les 5 modes de paiement se répartissent presque uniformément autour de 8 000 transactions chacun — cohérent avec la répartition quasi-égale déjà observée à l'échelle globale (Question 3) : ce jeu de données pédagogique ne modélise pas de préférence de paiement particulière par magasin ou par région.

---

## Consigne 4 — Comparaison avec l'Atelier 4

| | RDD (Atelier 4) | DataFrame (Atelier 4) | Spark SQL (ici) | Hive (référence, EMR) |
|---|---|---|---|---|
| Top 1 magasin | Philadelphia — 10 190 080,26 $ | Philadelphia — 10 190 080,26 $ | Philadelphia — 10 190 080,26 $ | *(identique, à exécuter sur EMR)* |
| Taux cash | 20,03 % | 20,03 % | 20,03 % | *(identique)* |

Les quatre approches produisent **rigoureusement le même résultat métier**. Ce qui change, c'est uniquement le niveau d'abstraction et l'ergonomie d'écriture :

- **RDD** : manipulation explicite de tuples et d'indices de position (`f[2]`, `f[4]`).
- **DataFrame** : appels de méthode sur des colonnes nommées (`groupBy("store").agg(_sum("cost"))`).
- **Spark SQL** : chaîne de caractères SQL passée à `spark.sql(...)`, exécutée par le même moteur Catalyst que le DataFrame — les deux sont donc rigoureusement équivalents en performance.
- **HiveQL** : syntaxe SQL quasi identique à Spark SQL, mais évaluée par le moteur configuré sur le cluster Hive/EMR plutôt que directement par Spark.

---

## Synthèse

- Spark SQL et l'API DataFrame ne sont pas deux outils différents mais deux syntaxes pour un seul et même moteur d'exécution (Catalyst) — le choix entre les deux est purement une question de préférence d'écriture.
- Le corrigé de la Partie 1 (Hive) illustre une réalité de terrain : certains ateliers de ce module supposent une infrastructure cloud déjà déployée (EMR) : plutôt que de simuler artificiellement ce résultat sans cluster réel, ce corrigé s'appuie sur l'équivalence logique stricte entre HiveQL et Spark SQL sur la même requête et la même donnée pour fournir des résultats de référence fiables.
- La requête PIVOT confirme, à un niveau de détail plus fin (croisement magasin × mode de paiement), l'homogénéité déjà observée dans les agrégations globales de l'Atelier 4 : ce jeu de données pédagogique répartit ses ventes de façon presque uniforme, sans déséquilibre notable entre magasins ou moyens de paiement.
