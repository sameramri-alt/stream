# ============================================================
# Dockerfile — Dashboard Streamlit
# ============================================================

FROM python:3.11-slim

WORKDIR /app

# Installer les dépendances système nécessaires pour pyarrow
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

# Port exposé par Streamlit
EXPOSE 8501

# Démarrage de Streamlit
# --server.headless=true : mode serveur (pas d'ouverture du navigateur)
# --server.address=0.0.0.0 : accessible depuis l'extérieur du conteneur
CMD ["streamlit", "run", "app.py", \
     "--server.port=8501", \
     "--server.address=0.0.0.0", \
     "--server.headless=true"]
