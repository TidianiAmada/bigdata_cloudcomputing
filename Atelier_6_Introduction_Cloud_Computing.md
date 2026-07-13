# Atelier 6 — Introduction au Cloud Computing

**Module :** Introduction au Big Data et au Cloud Computing
**Formation :** Licence Informatique 2 — SupDeCo
**Enseignant :** M. TOP
**Durée :** 3 heures

---

## Objectifs de l'atelier

- définir le Cloud Computing et le principe de virtualisation ;
- distinguer les modèles IaaS, PaaS, SaaS ;
- distinguer Cloud public, privé et hybride ;
- comprendre les notions d'élasticité et de haute disponibilité ;
- identifier les services AWS fondamentaux (régions, zones de disponibilité, IAM, VPC, Security Groups, EC2, S3) ;
- se connecter à une machine EC2 et stocker des fichiers sur S3.

---

## 1. Rappel théorique (45–60 min)

### 1.1 Qu'est-ce que le Cloud Computing ?

Le Cloud Computing consiste à accéder à des ressources informatiques (calcul, stockage, réseau, bases de données) à la demande, via Internet, auprès d'un fournisseur, plutôt que d'acquérir et de gérer soi-même des serveurs physiques. Le modèle économique associé est la facturation à l'usage (*pay-as-you-go*) : on paie ce que l'on consomme, sans investissement initial en matériel.

### 1.2 La virtualisation

La virtualisation est la technologie qui rend le Cloud possible : elle permet de faire fonctionner plusieurs machines virtuelles (VM) indépendantes sur un même serveur physique, chacune disposant de son propre système d'exploitation, isolée des autres, grâce à un **hyperviseur**.

Un **VPS** (*Virtual Private Server*) est une machine virtuelle louée chez un hébergeur, se comportant comme un serveur dédié du point de vue de l'utilisateur, alors qu'elle partage en réalité une machine physique avec d'autres clients.

### 1.3 Les modèles de service : IaaS, PaaS, SaaS

```text
Responsabilité du client   Responsabilité du fournisseur
─────────────────────────────────────────────────────────
  On-Premise    │ IaaS         │ PaaS          │ SaaS
─────────────────────────────────────────────────────────
Applications    │ Applications │ Applications  │ [Géré par
Données         │ Données      │ Données       │  le
Runtime         │ Runtime      │ [Géré par     │  fournisseur]
Middleware      │ Middleware   │  le           │
OS              │ OS           │  fournisseur] │
Virtualisation  │ [Géré par le fournisseur]
Serveurs / Réseau / Stockage
```

| Modèle | Ce que fournit le prestataire | Ce que gère le client | Exemple |
|---|---|---|---|
| **IaaS** (Infrastructure as a Service) | Serveurs virtuels, stockage, réseau | OS, middleware, applications, données | Amazon EC2, S3 |
| **PaaS** (Platform as a Service) | Infrastructure + environnement d'exécution | Uniquement le code applicatif et les données | AWS Elastic Beanstalk, Heroku |
| **SaaS** (Software as a Service) | Application complète, prête à l'emploi | Uniquement l'utilisation et la configuration | Gmail, Salesforce |

Amazon EMR, étudié à l'Atelier 7, se situe entre IaaS et PaaS : AWS gère le provisionnement du cluster et l'installation des outils Big Data, tandis que l'utilisateur gère ses traitements et ses données.

### 1.4 Cloud public, privé et hybride

- **Cloud public** : infrastructure partagée entre plusieurs clients, gérée par un fournisseur tiers (AWS, Azure, Google Cloud). Coût généralement plus faible, mise en œuvre rapide.
- **Cloud privé** : infrastructure dédiée à une seule organisation, hébergée en interne ou par un prestataire. Contrôle et sécurité renforcés, coût plus élevé.
- **Cloud hybride** : combinaison des deux, permettant par exemple de garder des données sensibles en interne tout en exploitant l'élasticité du Cloud public pour les pics de charge.

### 1.5 Élasticité et haute disponibilité

- **Élasticité** : capacité à ajuster automatiquement (à la hausse comme à la baisse) les ressources allouées en fonction de la charge réelle, sans intervention manuelle. C'est ce qui permet de payer uniquement les ressources utilisées.
- **Haute disponibilité** : capacité d'un système à rester opérationnel malgré la panne d'un composant, généralement obtenue par la réplication des ressources sur plusieurs zones physiquement séparées.

### 1.6 AWS : notions fondamentales

**Régions et zones de disponibilité**

Une **région** AWS est une zone géographique (ex. : `eu-west-3` à Paris) composée de plusieurs **zones de disponibilité** (*Availability Zones*, AZ) — des centres de données physiquement distincts, reliés par un réseau à faible latence. Répartir des ressources sur plusieurs AZ permet de survivre à la panne d'un centre de données.

```text
Région eu-west-3 (Paris)
┌───────────────────────────────────────────┐
│   AZ-a          AZ-b          AZ-c          │
│ ┌────────┐   ┌────────┐   ┌────────┐        │
│ │ Datacenter│  │ Datacenter│  │ Datacenter│  │
│ └────────┘   └────────┘   └────────┘        │
└───────────────────────────────────────────┘
```

**IAM (Identity and Access Management)**

Service de gestion des identités et des permissions sur AWS. Permet de définir *qui* (utilisateur, groupe, service) peut faire *quoi* (action) sur *quelle ressource*, via des politiques (*policies*). Principe fondamental à retenir : le **principe du moindre privilège** — n'accorder que les permissions strictement nécessaires.

**VPC (Virtual Private Cloud)**

Réseau virtuel privé et isolé au sein d'AWS, dans lequel sont déployées les ressources (machines EC2, clusters EMR…). Un VPC est découpé en sous-réseaux (*subnets*), publics ou privés, et permet de contrôler précisément le routage et l'exposition des ressources à Internet.

**Security Groups**

Pare-feu virtuel appliqué au niveau d'une ressource (par exemple une instance EC2), définissant les règles de trafic entrant et sortant autorisées (protocole, port, plage d'adresses IP source).

**EC2 (Elastic Compute Cloud)**

Service IaaS fournissant des machines virtuelles (*instances*) à la demande, dans une grande variété de tailles et de familles (optimisées calcul, mémoire, etc.), facturées à l'usage.

**S3 (Simple Storage Service)**

Service de stockage d'objets, hautement disponible et durable, organisé en **buckets** (conteneurs) contenant des objets (fichiers) identifiés par une clé. C'est le service de stockage le plus utilisé pour les données Big Data sur AWS — Amazon EMR (Atelier 7) lit et écrit typiquement ses données depuis/vers S3.

---

## 2. Atelier pratique (90–120 min)

### 2.1 Connexion à une machine EC2

1. Dans la console AWS, lancer une instance EC2 (type `t2.micro`, éligible à l'offre gratuite), en choisissant une AMI (image système) Amazon Linux ou Ubuntu.
2. Créer ou sélectionner une **paire de clés** (`.pem`) pour l'authentification SSH.
3. Configurer un **Security Group** autorisant le trafic SSH (port 22) uniquement depuis l'adresse IP de l'étudiant.
4. Se connecter en SSH depuis un terminal :

```bash
chmod 400 ma-cle.pem
ssh -i ma-cle.pem ec2-user@<adresse-ip-publique>
```

5. Vérifier la connexion et exécuter quelques commandes de base (`whoami`, `df -h`, `uname -a`).

### 2.2 Stockage de fichiers sur S3

1. Créer un bucket S3 (nom unique au niveau mondial) :

```bash
aws s3 mb s3://atelier-supdeco-<votre-nom>
```

2. Envoyer un fichier local vers le bucket. On y dépose dès maintenant le jeu de données fil rouge `purchases.txt` (utilisé depuis l'Atelier 4 avec Spark et Hive) : il sera repris tel quel à l'Atelier 7 pour être traité par un cluster Amazon EMR.

```bash
aws s3 cp purchases.txt s3://atelier-supdeco-<votre-nom>/entrees/purchases.txt
```

3. Lister le contenu du bucket :

```bash
aws s3 ls s3://atelier-supdeco-<votre-nom>/
```

4. Télécharger le fichier depuis l'instance EC2 :

```bash
aws s3 cp s3://atelier-supdeco-<votre-nom>/entrees/purchases.txt .
```

### Consignes

1. Lancer une instance EC2 et s'y connecter en SSH.
2. Créer un bucket S3 et y déposer au moins un fichier.
3. Récupérer ce fichier depuis l'instance EC2 via la CLI AWS.
4. Identifier, dans la console AWS, la région et la zone de disponibilité de l'instance créée.
5. Discussion : pourquoi restreindre le Security Group à une seule adresse IP source plutôt qu'à `0.0.0.0/0` ?

---

## 3. Synthèse

- Le Cloud Computing repose sur la virtualisation et propose trois grands modèles de service (IaaS, PaaS, SaaS), avec un partage variable des responsabilités entre client et fournisseur.
- Le choix entre Cloud public, privé et hybride dépend des contraintes de coût, de contrôle et de sécurité.
- L'élasticité et la haute disponibilité sont les deux promesses centrales du Cloud, rendues possibles par la répartition des ressources sur plusieurs zones de disponibilité.
- EC2 (calcul) et S3 (stockage) sont les deux services AWS fondamentaux ; IAM et les Security Groups en assurent la sécurité. Cette base est indispensable avant d'aborder Amazon EMR (Atelier 7), qui orchestre EC2 et S3 pour former un cluster Big Data managé.

---

## Pour aller plus loin

- Explorer le AWS Free Tier et ses limites d'usage.
- Comparer les classes de stockage S3 (Standard, Infrequent Access, Glacier) et leur impact sur le coût.
