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

### 1.4 Docker Compose

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
docker compose down               # arrêter et supprimer les services
docker compose logs -f            # suivre les journaux de tous les services
docker compose ps                 # état des services
```

---

## 3. Atelier pratique (60 min)

### Objectif
Déployer, avec un seul fichier `docker-compose.yml`, une plateforme locale comprenant MongoDB et Spark.

### 3.1 Fichier `docker-compose.yml`

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
    image: bitnami/spark:latest
    container_name: spark-master
    environment:
      - SPARK_MODE=master
    ports:
      - "8080:8080"   # interface web du master
      - "7077:7077"   # port de communication Spark
    networks:
      - bigdata_net

  spark-worker:
    image: bitnami/spark:latest
    container_name: spark-worker
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
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

### 3.2 Déroulé

1. Créer le fichier `docker-compose.yml` ci-dessus dans un dossier de travail.
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

---

## 4. Synthèse

- Docker permet d'empaqueter une application avec son environnement d'exécution dans un conteneur léger, portable et reproductible.
- Image, conteneur, registre, volume et réseau sont les cinq notions à maîtriser pour déployer n'importe quel service.
- Docker Compose orchestre plusieurs conteneurs liés (base de données, moteur de calcul) à partir d'un seul fichier de configuration.
- Cette plateforme locale (MongoDB + Spark) sert de base à l'ensemble des ateliers suivants, avant d'être reproduite dans le Cloud avec Amazon EMR.

---

## Pour aller plus loin

- Écrire un `Dockerfile` personnalisé pour créer sa propre image.
- Étudier les différences entre réseau `bridge`, `host` et `overlay`.
