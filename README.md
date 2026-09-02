# 🌍 Real-Time Wikimedia Lakehouse

![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)
![Redpanda](https://img.shields.io/badge/Redpanda-000000?style=for-the-badge&logo=kafka&logoColor=white)
![MinIO](https://img.shields.io/badge/MinIO-C7202C?style=for-the-badge&logo=minio&logoColor=white)

Ce projet implémente une architecture complète de données en temps réel (Streaming Lakehouse) basée sur l'architecture **Medallion** (Bronze, Silver, Gold). 
Il ingère, traite et visualise le flux d'éditions en direct de [Wikimedia (Recent Changes)](https://stream.wikimedia.org/v2/stream/recentchange).

---

## 🏗️ Architecture du Projet

Le flux de données suit une architecture structurée en 3 couches stockées sur **MinIO** (compatible S3) sous format **Parquet** :

1. **📥 Ingestion** : Un script Python écoute les événements SSE de Wikimedia et les envoie dans un topic **Redpanda** (compatible Kafka).
2. **🥉 Bronze** : **Spark Structured Streaming** lit les événements depuis Kafka et stocke les données brutes telles quelles (format JSON natif encapsulé).
3. **🥈 Silver** : Les données sont nettoyées, aplaties, filtrées (seulement les modifications d'articles) et enrichies de nouvelles colonnes (comme `server_name` pour la langue).
4. **🥇 Gold** : Les données sont agrégées pour répondre aux besoins métiers :
   - Top 10 des articles les plus modifiés
   - Métriques détaillées par espace de noms (Namespace)
   - Statistiques par langue Wikipedia
   - Séries temporelles (Évolution du trafic par minute)
5. **📊 Visualisation** : Un **Dashboard Streamlit** lit la couche Gold depuis MinIO et rafraîchit automatiquement (toutes les 15s) les graphiques interactifs (Plotly).

---

## 🚀 Démarrage Rapide

### Prérequis
- [Docker](https://www.docker.com/) et Docker Compose installés sur votre machine.

### Installation

1. **Cloner le dépôt**
   ```bash
   git clone https://github.com/sameramri-alt/stream.git
   cd stream
   ```

2. **Configuration de l'environnement**
   Copiez le fichier d'exemple et renommez-le en `.env` :
   *(Les paramètres par défaut de MinIO et Redpanda s'y trouvent)*
   ```bash
   cp .env.example .env
   ```

3. **Lancer tous les services**
   ```bash
   docker compose up -d --build
   ```

> **Note :** Le premier démarrage peut prendre quelques minutes le temps de télécharger les images Docker (Spark, MinIO, Python, etc.).

---

## 🌐 Services et Accès

Une fois que tous les conteneurs sont lancés, vous avez accès aux interfaces suivantes :

| Service | Accès local | Description |
|---|---|---|
| **Streamlit Dashboard** | [http://localhost:8501](http://localhost:8501) | Tableau de bord en temps réel pour visualiser les données Gold. |
| **Redpanda Console** | [http://localhost:8080](http://localhost:8080) | Interface pour visualiser le flux de messages Kafka brut. |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | Interface web du stockage S3 (Identifiants dans le `.env`). |
| **Spark Master UI** | [http://localhost:8081](http://localhost:8081) | Tableau de bord pour surveiller le cluster Spark. |
| **Spark Worker UI** | [http://localhost:8083](http://localhost:8083) | Suivi des exécuteurs Spark. |

---

## 🛠️ Contenu du Dépôt

- `docker-compose.yml` : Fichier de déploiement multi-conteneurs.
- `producer/` : Script Python pour l'ingestion SSE vers Kafka.
- `spark/jobs/` : Script PySpark contenant toute la logique Medallion (Bronze/Silver/Gold).
- `dashboard/` : Code de l'application web Streamlit et ses graphiques.
- `.env.example` : Exemple de variables d'environnement nécessaires pour configurer le projet.

---

## 🛑 Arrêt et Nettoyage

Pour arrêter proprement les services :
```bash
docker compose down
```

Pour arrêter les services **ET** supprimer tout l'historique des bases de données (MinIO et Redpanda) afin de repartir de zéro :
```bash
docker compose down -v
```

---
*Réalisé dans le cadre de la mise en place d'une infrastructure Big Data Streaming.*
