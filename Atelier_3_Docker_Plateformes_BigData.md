# Atelier 3 — Docker pour les plateformes Big Data

**Module :** Introduction au Big Data et au Cloud Computing

**Formation :** Licence Informatique 2 — SupDeCo

**Enseignant :** M. TOP

**Durée :** 2 heures

---

## Objectifs de l'atelier

- comprendre les concepts fondamentaux de Docker : image, conteneur, registre, volume, réseau ;
- comprendre l'intérêt de la conteneurisation pour les plateformes Big Data ;
- déployer une plateforme locale multi-services avec Docker Compose ;
- acquérir les commandes Docker indispensables pour la suite du module.

> **Remarque pédagogique :** à partir de cet atelier, Docker devient le socle technique de toutes les manipulations du cours. Ce n'est pas un chapitre isolé : MongoDB (Atelier 2), Spark (Ateliers 4-5) seront systématiquement exécutés dans ce même environnement conteneurisé, ce qui prépare la transition naturelle vers Amazon EMR (Atelier 7), où l'on retrouvera les mêmes briques logicielles dans le Cloud.

---

## 1. Rappel théorique (20–30 min)

### 1.1 Le problème que résout Docker

Avant la conteneurisation, déployer une application impliquait de reproduire manuellement son environnement (système d'exploitation, versions de logiciels, dépendances) sur chaque machine — source classique du problème *« ça marche sur ma machine »*. Docker répond à ce problème en empaquetant une application avec tout son environnement d'exécution dans une unité portable et reproductible : le **conteneur**.

### 1.2 Les concepts fondamentaux

| Concept | Définition |
|---|---|
| **Image** | Modèle en lecture seule contenant le code, les dépendances et la configuration nécessaires à l'exécution d'une application. Construite à partir d'un `Dockerfile` ou récupérée depuis un registre. |
| **Conteneur** | Instance en cours d'exécution d'une image. Isolé au niveau processus, léger (contrairement à une machine virtuelle complète). |
| **Registre** (*registry*) | Dépôt d'images, public (Docker Hub) ou privé, permettant de stocker et distribuer des images. |
| **Volume** | Mécanisme de persistance des données en dehors du cycle de vie du conteneur (un conteneur supprimé perd ses données sauf si elles sont dans un volume). |
| **Réseau** | Docker crée des réseaux virtuels permettant à des conteneurs de communiquer entre eux par leur nom, indépendamment de la machine hôte. |

### 1.3 Conteneur vs machine virtuelle

```text
Machine virtuelle                Conteneur Docker
┌─────────────────┐              ┌─────────────────┐
│   Application    │              │   Application    │
├─────────────────┤              ├─────────────────┤
│  OS invité complet│              │   Librairies     │
├─────────────────┤              ├─────────────────┤
│   Hyperviseur     │              │  Moteur Docker   │
├─────────────────┤              ├─────────────────┤
│   OS hôte         │              │   OS hôte         │
└─────────────────┘              └─────────────────┘
```

Le conteneur partage le noyau du système hôte : il démarre en quelques secondes (contre plusieurs dizaines de secondes à minutes pour une VM) et consomme beaucoup moins de ressources. C'est cette légèreté qui rend Docker particulièrement adapté aux plateformes Big Data, où l'on doit souvent déployer rapidement plusieurs services (base de données, moteur de traitement, interface) sur une même machine de travail.

### 1.4 Construire une image avec un Dockerfile

Un **Dockerfile** décrit, instruction par instruction, comment construire une image à partir d'une image de base. C'est le point de départ de toute image personnalisée — y compris celles qui, plus loin dans le module, exécutent Spark ou une API de traitement de données.

Exemple : une petite API **FastAPI** exposant un traitement de données avec `pandas` et `numpy`.

```dockerfile
FROM python:3.12
# Image de base : Python 3.12 déjà installé, sert de socle à toutes les couches suivantes

WORKDIR /app
# Définit /app comme répertoire de travail courant dans le conteneur
# (les instructions suivantes s'exécutent depuis ce répertoire)

COPY requirements.txt .
# Copie uniquement le fichier des dépendances avant le reste du code source
# (permet à Docker de réutiliser le cache si le code change mais pas les dépendances)

RUN pip install -r requirements.txt
# Installe les dépendances (ici : fastapi, uvicorn, pandas, numpy)
# à l'intérieur de l'image, une fois pour toutes

COPY . .
# Copie le reste du code source de l'application dans /app

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
# Commande exécutée au démarrage du conteneur : lance le serveur FastAPI
# sur le port 8000, accessible depuis l'extérieur du conteneur (0.0.0.0)
```

**Docker construit les images par empilement de couches (*layers*)** : chaque instruction du Dockerfile produit une nouvelle couche, mise en cache indépendamment des autres. Réorganiser le Dockerfile pour placer les instructions qui changent le moins souvent (image de base, dépendances) avant celles qui changent à chaque modification (code source) permet de réutiliser le cache et d'accélérer nettement les reconstructions.

```text
   Layer 5   Application (COPY . .)
   Layer 4   Dépendances installées (RUN pip install -r requirements.txt)
   Layer 3   requirements.txt copié (COPY requirements.txt .)
   Layer 2   Répertoire de travail (WORKDIR /app)
   Layer 1   Image de base (FROM python:3.12)
```

Si seul le code de `Layer 5` change entre deux constructions, Docker réutilise le cache des couches 1 à 4 et ne reconstruit que la dernière — d'où l'intérêt de copier `requirements.txt` séparément du reste du code.

### 1.5 Docker Compose

Une plateforme Big Data combine plusieurs services (MongoDB, Spark, éventuellement Hadoop). Les lancer un par un avec des commandes `docker run` séparées est fastidieux et source d'erreurs. **Docker Compose** permet de décrire, dans un fichier unique `docker-compose.yml`, l'ensemble des services, leurs images, leurs ports, leurs volumes et leur réseau partagé, puis de les démarrer d'une seule commande.

---

## 2. Commandes Docker indispensables

```bash
# Images
docker pull mongo:latest          # télécharger une image depuis le registre
docker images                     # lister les images locales
docker rmi <image>                # supprimer une image

# Conteneurs
docker run -d --name mongodb -p 27017:27017 mongo   # créer et démarrer un conteneur
docker ps                         # conteneurs en cours d'exécution
docker ps -a                      # tous les conteneurs (y compris arrêtés)
docker stop <conteneur>           # arrêter
docker start <conteneur>          # redémarrer
docker rm <conteneur>             # supprimer
docker logs <conteneur>           # consulter les journaux
docker exec -it <conteneur> bash  # ouvrir un terminal dans le conteneur

# Volumes et réseaux
docker volume ls
docker network ls

# Docker Compose
docker compose up -d              # démarrer tous les services définis
docker compose up -d <service>    # (re)démarrer un seul service, sans toucher aux autres
docker compose down               # arrêter et supprimer les services (garde les volumes nommés)
docker compose down -v            # arrêter et supprimer aussi les volumes nommés
docker compose logs -f            # suivre les journaux de tous les services
docker compose logs -f <service>  # suivre les journaux d'un seul service
docker compose ps                 # état des services
```

### 2.1 Commandes de diagnostic (à connaître pour déboguer une plateforme)

Au-delà des commandes de base, quelques commandes de diagnostic sont indispensables dès qu'un service ne démarre pas ou ne se connecte pas correctement :

| Commande | Rôle | Exemple d'usage |
|---|---|---|
| `docker logs <conteneur>` | Lire les journaux d'un conteneur (souvent la première chose à faire quand un conteneur redémarre en boucle ou plante) | `docker logs spark-worker` |
| `docker exec <conteneur> getent hosts <service>` | Vérifier que la résolution DNS interne à Docker Compose fonctionne entre deux services | `docker exec spark-worker getent hosts spark-master` |
| `docker exec <conteneur> <cmd> --eval "..."` | Exécuter une commande ponctuelle non interactive dans un conteneur (utile pour scripter des vérifications) | `docker exec mongodb mongosh --eval "db.runCommand({ping:1})"` |
| `docker volume ls --filter name=<motif>` | Retrouver un volume nommé précis parmi tous les volumes de la machine | `docker volume ls --filter name=mongo_data` |
| `docker rm -f <conteneur>` | Forcer la suppression d'un conteneur (même s'il tourne encore), pour tester la persistance d'un volume par exemple | `docker rm -f mongodb` |
| `curl http://localhost:8080/json/` | Interroger l'API REST exposée par le master Spark (mêmes informations que l'interface web, mais scriptable) | vérifier le nombre de workers actifs (`aliveworkers`) |

**Piège fréquent à connaître** : certaines images ne sont pas conçues pour tourner en root ou en mode standalone (c'est le cas d'`apache/spark-py`, utilisée plus loin dans cet atelier faute de disponibilité de `bitnami/spark`). Un conteneur qui redémarre en boucle sans message d'erreur explicite en façade doit systématiquement être diagnostiqué avec `docker logs`, qui affichera par exemple une `AccessDeniedException` si le processus n'a pas le droit d'écrire dans le répertoire de travail par défaut.

---

## 3. Atelier pratique (60 min)

### Objectif
Déployer deux plateformes multi-services avec Docker Compose : d'abord un exemple léger et universel (FastAPI + Redis) pour assimiler les mécanismes de base, puis la plateforme Big Data du module (MongoDB + Spark) qui servira de socle aux ateliers suivants.

### 3.1 Premier exemple : FastAPI + Redis

Avant d'orchestrer une plateforme Big Data complète, un exemple plus léger permet d'observer isolément ce qu'apporte Docker Compose : plusieurs services, un réseau partagé, une dépendance entre services, des ports exposés, des variables d'environnement. Architecture :

```text
Client
   │
   ▼
FastAPI  (service "api", port 8000)
   │
   ▼
Redis    (service "redis", port 6379, interne au réseau Docker)
```

**`main.py`** (l'API incrémente un compteur stocké dans Redis à chaque appel) :

```python
import os
import redis
from fastapi import FastAPI

app = FastAPI()
r = redis.Redis(host=os.environ.get("REDIS_HOST", "redis"), port=6379, decode_responses=True)

@app.get("/count")
def count():
    total = r.incr("visits")   # incrémente et retourne la nouvelle valeur
    return {"visits": total}
```

**`requirements.txt`** :

```text
fastapi
uvicorn
redis
```

Le service `api` réutilise exactement le Dockerfile présenté au §1.4 (même structure `FROM` → `WORKDIR` → `COPY requirements.txt` → `RUN pip install` → `COPY .` → `CMD`).

**`docker-compose.yml`** :

```yaml
version: "3.8"

services:
  api:
    build: .
    container_name: fastapi_app
    ports:
      - "8000:8000"
    environment:
      - REDIS_HOST=redis
    depends_on:
      - redis
    networks:
      - demo_net

  redis:
    image: redis:7-alpine
    container_name: redis_cache
    networks:
      - demo_net

networks:
  demo_net:
    driver: bridge
```

Déroulé :

1. Placer `main.py`, `requirements.txt`, le `Dockerfile` du §1.4 et `docker-compose.yml` dans le même dossier.
2. Construire et démarrer les deux services :
   ```bash
   docker compose up -d --build
   ```
3. Appeler l'API plusieurs fois et observer le compteur s'incrémenter :
   ```bash
   curl http://localhost:8000/count
   curl http://localhost:8000/count
   ```
4. Redémarrer uniquement le service `api` (`docker compose restart api`) et rappeler `/count` : le compteur continue sa progression, preuve que la donnée vit dans Redis et non dans le conteneur `api`.

*Exercice* : arrêter puis relancer `redis` seul (`docker compose restart redis`, sans option `-v`) et vérifier que le compteur est conservé — expliquer pourquoi, en lien avec la notion de volume vue au §1.2 (ici implicite : le conteneur `redis` n'est pas supprimé, seulement redémarré).

Cet exemple illustre en quelques minutes ce que Docker Compose apporte (réseau partagé `demo_net`, résolution par nom de service `redis`, `depends_on`, variables d'environnement, ports) avant de passer à une plateforme plus lourde.

### 3.2 La plateforme du module : MongoDB + Spark

**`docker-compose.yml`** :

```yaml
version: "3.8"

services:
  mongodb:
    image: mongo:latest
    container_name: mongodb
    ports:
      - "27017:27017"
    volumes:
      - mongo_data:/data/db
    networks:
      - bigdata_net

  spark-master:
    image: apache/spark-py:v3.4.0
    container_name: spark-master
    command: ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.master.Master"]
    ports:
      - "8080:8080"   # interface web du master
      - "7077:7077"   # port de communication Spark
    networks:
      - bigdata_net

  spark-worker:
    image: apache/spark-py:v3.4.0
    container_name: spark-worker
    command: ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.worker.Worker", "spark://spark-master:7077"]
    depends_on:
      - spark-master
    networks:
      - bigdata_net

volumes:
  mongo_data:

networks:
  bigdata_net:
    driver: bridge
```

> **Note sur le choix de l'image.** L'image `bitnami/spark`, longtemps utilisée dans les tutoriels (elle démarre master/worker via la seule variable `SPARK_MODE`), n'est plus systématiquement disponible en accès libre sur Docker Hub. On utilise ici l'image officielle `apache/spark-py`, qui ne connaît pas `SPARK_MODE` : le rôle (master ou worker) est fixé explicitement via `command:`, en appelant directement la classe Java correspondante (`spark-class org.apache.spark.deploy.master.Master` / `...deploy.worker.Worker`), ce qui a l'avantage de s'exécuter au premier plan et de garder le conteneur actif sans configuration supplémentaire.

### 3.3 Déroulé

1. Créer le fichier `docker-compose.yml` ci-dessus dans un nouveau dossier de travail (distinct de celui du §3.1).
2. Lancer la plateforme :
   ```bash
   docker compose up -d
   ```
3. Vérifier que les trois services sont actifs :
   ```bash
   docker compose ps
   ```
4. Ouvrir l'interface web du master Spark dans un navigateur : `http://localhost:8080`.
5. Vérifier que le worker apparaît bien rattaché au master.
6. Se connecter à MongoDB depuis le conteneur :
   ```bash
   docker exec -it mongodb mongosh
   ```
7. Arrêter proprement la plateforme :
   ```bash
   docker compose down
   ```

### Points de vigilance à observer

- Que se passe-t-il si l'on relance `docker compose up -d` sans avoir fait `down` au préalable ?
- Pourquoi les conteneurs peuvent-ils se joindre par leur **nom de service** (`spark-master`) plutôt que par une adresse IP ?
- Que devient la donnée MongoDB si l'on supprime le conteneur mais pas le volume ?

> **Version corrigée et prête à l'emploi.** Le corrigé ([Solution_Atelier_3.md](Solutions/Solution_Atelier_3.md)) détaille le diagnostic complet du `spark-worker` (`AccessDeniedException` sur `/opt/spark/work`, résolue par `--work-dir /tmp/spark-work`, et `--host spark-master` pour fiabiliser l'annonce d'adresse du master). La version finale, avec en plus un volume `./data:/data` pour partager `purchases.txt`, est fournie à la racine du dépôt sous `docker-compose-mongo-spark.yml` — c'est ce fichier que réutilisent directement les Ateliers 4.2 et 5.2 (déposer `purchases.txt` dans un dossier `data/` à côté de ce fichier avant de lancer `docker compose -f docker-compose-mongo-spark.yml up -d`).

---

## 4. Synthèse

- Docker permet d'empaqueter une application avec son environnement d'exécution dans un conteneur léger, portable et reproductible.
- Image, conteneur, registre, volume et réseau sont les cinq notions à maîtriser pour déployer n'importe quel service.
- Un `Dockerfile` construit une image par empilement de couches ; ordonner les instructions des plus stables (image de base, dépendances) aux plus changeantes (code applicatif) permet de tirer parti du cache de build.
- Docker Compose orchestre plusieurs conteneurs liés à partir d'un seul fichier de configuration — le principe est identique qu'il s'agisse d'une paire légère (FastAPI + Redis) ou d'une plateforme Big Data complète (MongoDB + Spark).
- Cette plateforme locale (MongoDB + Spark) sert de base à l'ensemble des ateliers suivants, avant d'être reproduite dans le Cloud avec Amazon EMR.

---

## Pour aller plus loin

- Écrire un `Dockerfile` personnalisé intégrant `pandas`/`numpy` pour une application de traitement de données, et mesurer l'effet du cache de build en modifiant tour à tour `requirements.txt` puis uniquement le code source.
- Étudier les différences entre réseau `bridge`, `host` et `overlay`.
- Remplacer Redis par MongoDB dans l'exemple léger du §3.1 (`FastAPI + MongoDB`) et comparer la configuration nécessaire.
