# Solution — Atelier 3 : Docker pour les plateformes Big Data

Corrigé de l'atelier pratique (partie 3 de [Atelier_3_Docker_Plateformes_BigData.md](../Atelier_3_Docker_Plateformes_BigData.md)), déployé et testé réellement avec Docker Compose.

---

## Note importante : image `bitnami/spark` indisponible

En exécutant l'atelier tel quel, `docker compose up -d` échoue dès l'étape de récupération de l'image :

```text
spark-master Error failed to resolve reference "docker.io/bitnami/spark:latest": docker.io/bitnami/spark:latest: not found
```

Bitnami a retiré la quasi-totalité de ses images `latest`/anciennes versions du Docker Hub gratuit courant 2025 (elles sont désormais réservées à l'offre payante *Bitnami Secure Images*). C'est un excellent exemple concret d'un problème très courant en Big Data/Cloud : une image ou un service tiers utilisé en production peut disparaître ou changer de politique sans préavis — d'où l'intérêt de connaître au moins une alternative.

**Solution retenue** : remplacer `bitnami/spark:latest` par l'image officielle **`apache/spark-py:latest`** (maintenue par le projet Apache Spark lui-même). Cette image ne propose pas la variable `SPARK_MODE` (spécifique à Bitnami) : le rôle *master*/*worker* s'obtient en donnant directement la commande Spark bas niveau à exécuter, et le worker doit pointer vers un répertoire de travail accessible en écriture par son utilisateur non-root (`--work-dir /tmp/spark-work`, sans quoi il plante avec une `AccessDeniedException` sur `/opt/spark/work`).

## `docker-compose.yml` final (adapté)

```yaml
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
    image: apache/spark-py:latest
    container_name: spark-master
    command: ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.master.Master", "--host", "spark-master"]
    ports:
      - "8080:8080"   # interface web du master
      - "7077:7077"   # port de communication Spark
    volumes:
      - ./data:/data
    networks:
      - bigdata_net

  spark-worker:
    image: apache/spark-py:latest
    container_name: spark-worker
    command: ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.worker.Worker", "spark://spark-master:7077", "--work-dir", "/tmp/spark-work"]
    depends_on:
      - spark-master
    volumes:
      - ./data:/data
    networks:
      - bigdata_net

volumes:
  mongo_data:

networks:
  bigdata_net:
    driver: bridge
```

(`purchases.txt` est placé dans `./data` pour être exploité directement à l'Atelier 4, via `/data/purchases.txt` dans le conteneur.)

---

## Déroulé exécuté

### 1. Lancement

```bash
docker compose up -d
```

```text
Network atelier3_bigdata_net  Created
Volume "atelier3_mongo_data"  Created
Container mongodb        Started
Container spark-master   Started
Container spark-worker   Started
```

### 2. Vérification des trois services

```bash
docker compose ps
```

```text
NAME           IMAGE                    STATUS          PORTS
mongodb        mongo:latest             Up              0.0.0.0:27017->27017/tcp
spark-master   apache/spark-py:latest   Up              0.0.0.0:7077->7077/tcp, 0.0.0.0:8080->8080/tcp
spark-worker   apache/spark-py:latest   Up
```

Les trois conteneurs sont bien à l'état `Up`.

### 3. Interface web du master (`http://localhost:8080`) et rattachement du worker

Plutôt que de décrire une capture d'écran, interrogation directe de l'API JSON exposée par le master (même donnée que celle affichée par l'UI) :

```bash
curl -s http://localhost:8080/json/
```

```json
{
  "url": "spark://spark-master:7077",
  "workers": [{
    "id": "worker-20260714000035-172.19.0.4-41713",
    "host": "172.19.0.4",
    "cores": 12,
    "memory": 6751,
    "state": "ALIVE"
  }],
  "aliveworkers": 1,
  "status": "ALIVE"
}
```

→ **1 worker actif (`ALIVE`)**, correctement rattaché au master, avec 12 cœurs et 6,75 Go de RAM détectés (ressources de la machine hôte).

### 4. Connexion à MongoDB

```bash
docker exec -it mongodb mongosh --eval "db.runCommand({ping:1})"
```

```json
{ "ok": 1 }
```

### 5. Arrêt propre

```bash
docker compose down
```

---

## Points de vigilance — réponses observées

### Que se passe-t-il si l'on relance `docker compose up -d` sans avoir fait `down` au préalable ?

Test réel effectué (stack déjà démarrée, on relance `up -d` directement) :

```bash
docker compose up -d
```

```text
Container mongodb       Running
Container spark-master  Running
Container spark-worker  Running
```

→ Docker Compose est **idempotent** : il compare l'état désiré (le fichier YAML) à l'état réel des conteneurs déjà en place. Comme rien n'a changé et que les conteneurs tournent déjà, il se contente d'afficher `Running` sans les recréer ni générer d'erreur de conflit. Si en revanche on avait modifié le `docker-compose.yml` (nouvelle image, nouvelle variable d'environnement...), Compose aurait recréé uniquement les services impactés — c'est cette même mécanique qui a été utilisée plus loin pour relancer isolément `spark-worker` après correction de son `--work-dir`, sans toucher `mongodb` ni `spark-master`.

### Pourquoi les conteneurs peuvent-ils se joindre par leur nom de service (`spark-master`) plutôt que par une adresse IP ?

Vérification DNS réelle depuis le conteneur `spark-worker` :

```bash
docker exec spark-worker getent hosts spark-master
```

```text
172.19.0.3      spark-master
```

→ Docker Compose crée un réseau bridge dédié (ici `atelier3_bigdata_net`) doté d'un **serveur DNS interne** : chaque conteneur du réseau est automatiquement enregistré sous son nom de service. `spark-master` se résout donc en l'IP interne du conteneur correspondant (`172.19.0.3`), sans que l'on ait besoin de connaître ou de coder en dur cette IP (qui peut d'ailleurs changer à chaque recréation du conteneur). C'est exactement ce mécanisme que `SPARK_MASTER_URL=spark://spark-master:7077` exploite pour que le worker retrouve le master.

### Que devient la donnée MongoDB si l'on supprime le conteneur mais pas le volume ?

Test réel effectué :

```bash
# 1. Insertion d'une donnée de test
docker exec mongodb mongosh --eval \
  "db.getSiblingDB('testdb').demo.insertOne({marqueur:'persistant-avant-suppression'})"

# 2. Suppression du conteneur (le volume nommé n'est pas supprimé par 'docker rm')
docker rm -f mongodb

# 3. Le volume existe toujours
docker volume ls --filter name=mongo_data
# → local     atelier3_mongo_data

# 4. Recréation du conteneur via Compose (même volume monté sur /data/db)
docker compose up -d mongodb

# 5. Relecture de la donnée
docker exec mongodb mongosh --eval "db.getSiblingDB('testdb').demo.find()"
```

```json
[{ "_id": "ObjectId('6a557c3a98589e58999c56c8')", "marqueur": "persistant-avant-suppression" }]
```

→ La donnée est **intacte** après suppression puis recréation du conteneur : elle n'a jamais vécu dans le conteneur lui-même mais dans le volume nommé `mongo_data`, dont le cycle de vie est indépendant de celui du conteneur. C'est précisément la distinction clé entre l'état d'un conteneur (éphémère par défaut) et un volume (persistant tant qu'il n'est pas explicitement supprimé avec `docker volume rm` ou `docker compose down -v`).

---

## Synthèse

- L'écosystème Big Data évolue vite : une image utilisée l'an dernier (`bitnami/spark`) peut devenir indisponible — savoir lire un message d'erreur Docker et basculer vers une image équivalente (`apache/spark-py`) est une compétence aussi importante que la syntaxe Compose elle-même.
- `docker compose up -d` est idempotent : sans changement dans le fichier, il ne fait que constater que tout tourne déjà.
- La résolution de noms entre conteneurs d'un même réseau Compose repose sur un DNS interne à Docker, ce qui évite de coder en dur des adresses IP volatiles.
- Un volume nommé découple la durée de vie des données de celle du conteneur qui les utilise — c'est ce qui rend un redéploiement (mise à jour d'image, correction de configuration) sans perte de données.
- Cette même plateforme (MongoDB + Spark standalone), avec `purchases.txt` monté dans `./data`, sert de socle direct à l'Atelier 4.
