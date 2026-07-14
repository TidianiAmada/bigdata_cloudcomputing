# Solution — Atelier 1 : Pourquoi le Big Data ?

Corrigé de l'atelier pratique (partie 2 de [Atelier_1_Pourquoi_le_Big_Data.md](../Atelier_1_Pourquoi_le_Big_Data.md)), basé sur une exécution réelle sur `purchases.txt`.

---

## 1. Téléchargement et décompression

```bash
wget https://github.com/CodeMangler/udacity-hadoop-course/raw/master/Datasets/purchases.txt.gz
gunzip purchases.txt.gz
```

Résultat : `purchases.txt.gz` (~38 Mo) se décompresse en `purchases.txt` (~202 Mo) — un ratio de compression d'environ 5,5x, cohérent avec un fichier texte très répétitif (peu de villes, peu de produits, peu de moyens de paiement distincts).

---

## 2. Découverte du jeu de données

### Taille et volumétrie

```bash
ls -l purchases.txt
wc -l purchases.txt
```

| Mesure | Valeur observée |
|---|---|
| Taille du fichier | ≈ 202 Mo (211 312 924 octets) |
| Nombre de lignes | **4 138 476** |
| Nombre de colonnes | 6 (séparées par des tabulations) |

### Colonnes

```bash
head -5 purchases.txt
```

```text
2012-01-01	09:00	San Jose	Men's Clothing	214.05	Amex
2012-01-01	09:00	Fort Worth	Women's Clothing	153.57	Visa
2012-01-01	09:00	San Diego	Music	66.08	Cash
2012-01-01	09:00	Pittsburgh	Pet Supplies	493.51	Discover
2012-01-01	09:00	Omaha	Children's Clothing	235.63	MasterCard
```

Les 6 colonnes correspondent bien à `pdate, ptime, store, product, cost, payment` décrites dans l'énoncé.

### Homogénéité du format

```bash
awk -F'\t' '{print NF}' purchases.txt | sort -u
```

→ toutes les lignes renvoient **6** : le format est homogène sur l'ensemble du fichier (pas de ligne à 5 ou 7 champs). C'est un fichier plat structuré, mais sans schéma imposé par un SGBD — rien n'empêche une future ligne malformée d'être insérée sans validation.

### Valeurs manquantes, doublons, incohérences

```bash
# champs vides
awk -F'\t' '{for(i=1;i<=NF;i++) if($i=="") c++} END{print c+0}' purchases.txt
# → 0 champ vide

# lignes strictement dupliquées
sort purchases.txt | uniq -d | wc -l
# → 0 doublon exact

# valeurs distinctes des colonnes catégorielles
awk -F'\t' '{print $6}' purchases.txt | sort -u   # payment
awk -F'\t' '{print $3}' purchases.txt | sort -u | wc -l   # store
awk -F'\t' '{print $4}' purchases.txt | sort -u | wc -l   # product
```

| Contrôle | Résultat |
|---|---|
| Champs vides | 0 |
| Lignes dupliquées | 0 |
| Moyens de paiement distincts | 5 : `Amex, Cash, Discover, MasterCard, Visa` — cohérent avec l'énoncé |
| Magasins distincts (`store`) | 103 villes |
| Produits distincts (`product`) | 18 catégories |

Constat : ce jeu de données précis est en réalité assez « propre » (pas de valeurs manquantes ni de doublons) — c'est volontaire pédagogiquement. La difficulté ici n'est donc **pas** la qualité (Véracité) mais bien le **Volume** : 4,1 millions de lignes et 202 Mo, un fichier texte plat sans index, qu'aucun outil bureautique ne gère confortablement.

---

## 3. Tentative d'ouverture avec un tableur

En ouvrant `purchases.txt` dans Excel ou LibreOffice Calc :

- **Excel** : plafond fixe de **1 048 576 lignes** par feuille. Avec 4 138 476 lignes, le fichier est **tronqué silencieusement** aux ~1,05 million de premières lignes — soit un quart seulement des données, sans avertissement bloquant systématique.
- **LibreOffice Calc** : plafond similaire (~1 048 576 lignes également sur les versions récentes) ; en dessous de ce seuil, l'ouverture reste possible mais très lente (plusieurs dizaines de secondes à quelques minutes) et consomme énormément de RAM, le fichier étant chargé intégralement en mémoire avant tout affichage.
- Toute formule ou tri appliqué ensuite sur la feuille devient perceptiblement lent, alors qu'aucune analyse n'a encore été faite : le simple *chargement* épuise déjà les ressources de la machine.

**Conclusion observée** : un tableur est conçu pour une consultation interactive sur un volume de quelques dizaines de milliers de lignes maximum ; il n'est pas outillé pour du traitement de masse.

---

## 4. Discussion — Pourquoi une base SQL classique devient-elle insuffisante ?

| Limite observée sur `purchases.txt` | Dimension du Big Data (V) concernée |
|---|---|
| 4,1 millions de lignes / 202 Mo : dépasse ce qu'un tableur (et, à plus grande échelle, une seule machine) peut charger confortablement en mémoire | **Volume** |
| Le fichier n'est qu'un point de départ : dans un contexte réel, ces tickets de caisse arriveraient en continu (caisses connectées), pas en un seul batch figé de 2012 | **Vélocité** |
| Le format ici est tabulaire homogène, mais dans une vraie plateforme de vente, les mêmes données de vente coexistent avec des formats hétérogènes (photos de produits, avis clients en texte libre, logs de navigation JSON) qu'une table SQL rigide ne peut pas absorber sans remodeler le schéma | **Variété** |
| Même si ce fichier précis est propre (0 doublon, 0 champ vide), à cette échelle et avec des sources multiples, la probabilité d'incohérences (doublons, valeurs aberrantes, formats de date différents selon la caisse) augmente fortement | **Véracité** |
| Charger 4 millions de lignes dans une table pour ne finalement produire qu'un chiffre agrégé (CA total, panier moyen) montre que l'enjeu n'est pas de stocker mais d'en extraire une information utile à la décision | **Valeur** |

**Point clé à retenir** : un SGBDR classique *pourrait* techniquement stocker ces 4,1 millions de lignes (ce n'est pas énorme pour PostgreSQL, par exemple). La limite réelle apparaît quand ce volume est multiplié par plusieurs milliers de magasins, plusieurs années d'historique, et des requêtes d'agrégation lancées en continu — c'est la combinaison des 5V, pas un seul facteur, qui pousse vers une architecture distribuée.

---

## 5. Restitution collective — synthèse par groupe

Exemple de restitution attendue (une limite → un V) :

1. « Le fichier ne rentre pas dans Excel » → **Volume**
2. « Dans la vraie vie, les tickets de caisse arrivent en temps réel, pas dans un fichier figé » → **Vélocité**
3. « Un magasin pourrait vouloir croiser ces ventes avec des avis clients ou des photos de rayons » → **Variété**
4. « Avec des centaines de magasins et de caisses différentes, des incohérences de saisie sont inévitables » → **Véracité**
5. « Connaître le total ne sert à rien si on n'en tire pas une décision (réassort, promotion, fermeture de rayon) » → **Valeur**

---

## Récapitulatif technique (commandes utilisées)

```bash
wc -l purchases.txt
ls -l purchases.txt
du -h purchases.txt
head -5 purchases.txt
awk -F'\t' '{print NF}' purchases.txt | sort -u
awk -F'\t' '{for(i=1;i<=NF;i++) if($i=="") c++} END{print c+0}' purchases.txt
sort purchases.txt | uniq -d | wc -l
awk -F'\t' '{print $6}' purchases.txt | sort -u
awk -F'\t' '{print $3}' purchases.txt | sort -u | wc -l
awk -F'\t' '{print $4}' purchases.txt | sort -u | wc -l
```

Ce même fichier `purchases.txt` sera repris dans les ateliers 4, 5 et 7 pour être traité avec Hadoop et Spark — la présente exploration manuelle sert de point de comparaison avec le traitement distribué qui suivra.
