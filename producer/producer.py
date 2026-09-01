"""
============================================================
producer.py — Ingestion Wikipedia vers Redpanda
============================================================
Ce script fait UNE seule chose en boucle infinie :
  1. Se connecte au flux SSE public de Wikipedia
  2. Reçoit chaque modification d'article en temps réel
  3. Ajoute l'heure d'arrivée (ingestion_timestamp)
  4. Envoie le message dans Redpanda (topic: wikimedia-raw)

Flux de données :
  Wikipedia SSE ──► producer.py ──► Redpanda
============================================================
"""

import json
import os
import logging
import time
from datetime import datetime, timezone

import requests
from sseclient import SSEClient
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ============================================================
# CONFIGURATION DU LOGGING
# Format : heure [PRODUCER] niveau - message
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURATION — Lue depuis les variables d'environnement (.env)
# ============================================================
SSE_URL = os.getenv(
    "WIKIMEDIA_SSE_URL",
    "https://stream.wikimedia.org/v2/stream/recentchange"
)
KAFKA_BROKERS = os.getenv("REDPANDA_BROKERS", "localhost:9092").split(",")
KAFKA_TOPIC   = os.getenv("KAFKA_TOPIC", "wikimedia-raw")


# ============================================================
# FONCTION : Créer le KafkaProducer avec retry
# ============================================================
def creer_producer(nb_tentatives=15, delai_secondes=5):
    """
    Essaie de se connecter à Redpanda.
    Redpanda peut mettre quelques secondes à démarrer,
    donc on réessaie plusieurs fois avant d'abandonner.
    """
    logger.info(f"🔌 Tentative de connexion à Redpanda sur {KAFKA_BROKERS}...")

    for tentative in range(1, nb_tentatives + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS,
                # Sérialise les dict Python en JSON puis en bytes
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                # acks="all" = attendre confirmation de Redpanda avant de continuer
                acks="all",
                # 3 tentatives automatiques si l'envoi échoue
                retries=3,
                # Timeout de connexion : 10 secondes
                max_block_ms=10_000,
            )
            logger.info(f"✅ Connecté à Redpanda avec succès !")
            return producer

        except NoBrokersAvailable:
            logger.warning(
                f"⏳ Tentative {tentative}/{nb_tentatives} — "
                f"Redpanda pas encore prêt. Nouvel essai dans {delai_secondes}s..."
            )
            time.sleep(delai_secondes)

    # Si on arrive ici, toutes les tentatives ont échoué
    raise RuntimeError(
        f"❌ Impossible de se connecter à Redpanda après {nb_tentatives} tentatives."
    )


# ============================================================
# FONCTION : Envoyer un événement dans Redpanda
# ============================================================
def envoyer_evenement(producer, evenement):
    """
    Enrichit l'événement avec l'heure d'arrivée
    et l'envoie dans le topic Redpanda.
    """
    # Ajout de l'horodatage d'ingestion (quand ON a reçu le message)
    evenement["ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()

    # Envoi asynchrone dans Redpanda
    # La clé = le nom de l'article (pour regrouper les messages du même article)
    cle = evenement.get("title", "").encode("utf-8")
    producer.send(KAFKA_TOPIC, key=cle, value=evenement)


# ============================================================
# BOUCLE PRINCIPALE
# ============================================================
def main():
    logger.info("=" * 60)
    logger.info("🚀 DÉMARRAGE DU PRODUCER WIKIMEDIA")
    logger.info(f"   Source  : {SSE_URL}")
    logger.info(f"   Brokers : {KAFKA_BROKERS}")
    logger.info(f"   Topic   : {KAFKA_TOPIC}")
    logger.info("=" * 60)

    # Connexion à Redpanda
    producer = creer_producer()

    # Compteurs pour les logs périodiques
    nb_envoyes = 0
    nb_erreurs = 0

    # Boucle externe : reconnexion automatique si le flux SSE coupe
    while True:
        try:
            # Wikimedia exige impérativement un User-Agent personnalisé
            headers = {
                "User-Agent": "WikimediaDataLakehouse/1.0 (Student Project; Contact: stream@example.com)"
            }
            # Ouvre une connexion HTTP persistante vers Wikipedia
            reponse = requests.get(SSE_URL, headers=headers, stream=True, timeout=60)
            reponse.raise_for_status()
            client = SSEClient(reponse)

            logger.info("🟢 Réception des événements Wikipedia en cours...")

            # Boucle interne : traite chaque événement reçu
            for evenement in client.events():

                # Ignorer les événements vides ou les "heartbeats"
                if not evenement.data or evenement.data.strip() == "":
                    continue

                try:
                    # Parser le JSON reçu de Wikipedia
                    donnees = json.loads(evenement.data)

                    # Envoyer dans Redpanda
                    envoyer_evenement(producer, donnees)
                    nb_envoyes += 1

                    # Afficher un résumé toutes les 100 messages
                    if nb_envoyes % 100 == 0:
                        producer.flush()  # S'assurer que tout est bien envoyé
                        logger.info(
                            f"📨 {nb_envoyes} événements envoyés | "
                            f"Erreurs: {nb_erreurs} | "
                            f"Dernier article: {donnees.get('title', '?')}"
                        )

                except json.JSONDecodeError:
                    nb_erreurs += 1
                    logger.debug("⚠️ Événement SSE non-JSON ignoré.")

                except Exception as e:
                    nb_erreurs += 1
                    logger.warning(f"⚠️ Erreur lors de l'envoi : {e}")

        except KeyboardInterrupt:
            logger.info("🛑 Arrêt demandé par l'utilisateur.")
            producer.flush()
            producer.close()
            break

        except Exception as e:
            logger.error(f"💥 Connexion SSE perdue : {e}")
            logger.info("🔄 Reconnexion dans 10 secondes...")
            time.sleep(10)


# Point d'entrée du script
if __name__ == "__main__":
    main()
