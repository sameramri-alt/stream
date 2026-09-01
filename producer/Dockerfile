# ============================================================
# Dockerfile — Producer Python
# Construit l'image Docker pour le script producer.py
# ============================================================

# Image de base : Python 3.11 version légère (slim)
FROM python:3.11-slim

# Dossier de travail à l'intérieur du conteneur
WORKDIR /app

# Copier et installer les dépendances en premier
# (optimisation : Docker met en cache cette couche si requirements.txt ne change pas)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier le script principal
COPY producer.py .

# Commande exécutée au démarrage du conteneur
# -u = mode "unbuffered" : les logs apparaissent immédiatement
CMD ["python", "-u", "producer.py"]
