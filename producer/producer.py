"""
=================================================================
producer.py — Ingestion des événements Wikimedia vers Redpanda
=================================================================

Description complète du script :

Ce programme est le point d'entrée du flux de données temps réel.
Il démarre sur un flux public SSE de Wikipédia, lit chaque événement
quand un article est modifié, puis le publie dans Redpanda (Kafka).

Le but est de créer une source de données continue, robuste et
prête à être consommée par Spark pour construire un lakehouse.

Le flux logique est le suivant :

Wikipedia SSE
    │
    ├─> requests.get(..., stream=True)
    │
    ├─> SSEClient lit les événements en continu
    │
    ├─> json.loads(...) transforme le message JSON en objet Python
    │
    ├─> ajout de ingestion_timestamp
    │
    └─> KafkaProducer.send(...) -> Redpanda topic: wikimedia-raw

Pourquoi c'est important ?
- Wikipédia fournit des données temps réel sans devoir créer un service interne.
- Redpanda sert de tampon et de broker entre la source et le traitement.
- Le producer isole la source externe du moteur de calcul Spark.
=================================================================
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from sseclient import SSEClient

# ---------------------------------------------------------------
# 1. Configuration des logs
# ---------------------------------------------------------------
# Le logging est indispensable pour surveiller :
# - si le flux SSE est actif,
# - si Redpanda est prêt,
# - si des messages sont refusés,
# - si la reconnexion fonctionne.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------
# 2. Configuration provenant de l'environnement (.env)
# ---------------------------------------------------------------
# Les variables d'environnement évitent de coder en dur les
# valeurs sensibles ou dépendantes de l'environnement de déploiement.
# Exemple : le broker n'est pas le même en local, en Docker et en prod.
SSE_URL = os.getenv(
    "WIKIMEDIA_SSE_URL",
    "https://stream.wikimedia.org/v2/stream/recentchange",
)
KAFKA_BROKERS = os.getenv("REDPANDA_BROKERS", "localhost:9092").split(",")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "wikimedia-raw")


# ---------------------------------------------------------------
# 3. Fonction : créer le KafkaProducer avec mécanisme de retry
# ---------------------------------------------------------------
def creer_producer(nb_tentatives=15, delai_secondes=5):
    """
    Cette fonction initialise le client Kafka / Redpanda.

    Contexte :
    Redpanda prend un peu de temps à démarrer dans Docker. Si le producer
    démarre trop vite, il peut tomber sur une erreur de connexion.

    Ce code réessaie plusieurs fois avant de lever une exception fatale.
    """
    logger.info(f"🔌 Tentative de connexion à Redpanda sur {KAFKA_BROKERS}...")

    for tentative in range(1, nb_tentatives + 1):
        try:
            # KafkaProducer est le client officiel Kafka pour Python
            # Il contrôle la connexion, la serialisation JSON et l'envoi.
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
                max_block_ms=10_000,
            )
            logger.info("✅ Connecté à Redpanda avec succès !")
            return producer

        except NoBrokersAvailable:
            logger.warning(
                f"⏳ Tentative {tentative}/{nb_tentatives} — "
                f"Redpanda pas encore prêt. Nouvel essai dans {delai_secondes}s..."
            )
            time.sleep(delai_secondes)

    raise RuntimeError(
        f"❌ Impossible de se connecter à Redpanda après {nb_tentatives} tentatives."
    )


# ---------------------------------------------------------------
# 4. Fonction : enrichir et envoyer un événement
# ---------------------------------------------------------------
def envoyer_evenement(producer, evenement):
    """
    Cette fonction ajoute un horodatage d'ingestion et envoie ensuite
    le message dans le topic Kafka/Redpanda.

    Pourquoi ajouter ingestion_timestamp ?
    Parce que le message original contient la date de modification de
    Wikipédia, mais pas forcément l'heure exacte où votre pipeline l'a
    reçu. Cet ajout permet de mesurer la latence et de tracer le flux.
    """
    evenement["ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()

    # On utilise le titre comme clé Kafka pour regrouper les messages
    # liés au même article. Cela évite de mélanger les événements d'une
    # page avec ceux d'une autre lors du traitement Spark.
    cle = evenement.get("title", "").encode("utf-8")
    producer.send(KAFKA_TOPIC, key=cle, value=evenement)


# ---------------------------------------------------------------
# 5. Fonction principale : boucle de consommation du flux SSE
# ---------------------------------------------------------------
def main():
    """
    Cette fonction est le cœur du script.

    Elle fait tourner le programme en boucle infinie jusqu'à ce qu’il
    soit arrêté manuellement. Si le flux Wikipédia est interrompu, elle
    tente de se reconnecter automatiquement.
    """
    logger.info("=" * 60)
    logger.info("🚀 DÉMARRAGE DU PRODUCER WIKIMEDIA")
    logger.info(f"   Source  : {SSE_URL}")
    logger.info(f"   Brokers : {KAFKA_BROKERS}")
    logger.info(f"   Topic   : {KAFKA_TOPIC}")
    logger.info("=" * 60)

    # 1) Connexion initiale à Redpanda
    producer = creer_producer()

    # 2) Compteurs de suivi
    nb_envoyes = 0
    nb_erreurs = 0

    # -----------------------------------------------------------
    # Boucle externe : gestion de la connexion au flux SSE
    # -----------------------------------------------------------
    while True:
        try:
            # La Wikimedia API exige un User-Agent explicite. Sans ce header,
            # la demande peut être rejetée ou limitée.
            headers = {
                "User-Agent": "WikimediaDataLakehouse/1.0 (Student Project; Contact: stream@example.com)"
            }

            # requests.get(..., stream=True) laisse la connexion ouverte pour
            # lire les événements au fur et à mesure, sans attendre la fin.
            reponse = requests.get(SSE_URL, headers=headers, stream=True, timeout=60)
            reponse.raise_for_status()

            # SSEClient transforme le flux HTTP en objets event
            client = SSEClient(reponse)

            logger.info("🟢 Réception des événements Wikipedia en cours...")

            # -----------------------------------------------------
            # Boucle interne : lecture des événements au fil du temps
            # -----------------------------------------------------
            for evenement in client.events():
                # Les heartbeats SSE peuvent être vides ; on les ignore.
                if not evenement.data or evenement.data.strip() == "":
                    continue

                try:
                    # Transforme la chaîne JSON reçue par Wikipédia en diction Python.
                    donnees = json.loads(evenement.data)

                    # Envoie l'événement enrichi dans Redpanda.
                    envoyer_evenement(producer, donnees)
                    nb_envoyes += 1

                    # Log périodique pour surveiller le débit sans spammer la console.
                    if nb_envoyes % 100 == 0:
                        producer.flush()
                        logger.info(
                            f"📨 {nb_envoyes} événements envoyés | "
                            f"Erreurs: {nb_erreurs} | "
                            f"Dernier article: {donnees.get('title', '?')}"
                        )

                except json.JSONDecodeError:
                    # Certaines lignes SSE ne sont pas du JSON valide.
                    nb_erreurs += 1
                    logger.debug("⚠️ Événement SSE non-JSON ignoré.")

                except Exception as e:
                    # Gestion générique des erreurs de publication.
                    nb_erreurs += 1
                    logger.warning(f"⚠️ Erreur lors de l'envoi : {e}")

        except KeyboardInterrupt:
            # Arrêt propre du programme CTRL+C.
            logger.info("🛑 Arrêt demandé par l'utilisateur.")
            producer.flush()
            producer.close()
            break

        except Exception as e:
            # Si le flux est coupé, on reconnecte automatiquement après 10s.
            logger.error(f"💥 Connexion SSE perdue : {e}")
            logger.info("🔄 Reconnexion dans 10 secondes...")
            time.sleep(10)


# ---------------------------------------------------------------
# 6. Point d'entrée du script
# ---------------------------------------------------------------
if __name__ == "__main__":
    main()
