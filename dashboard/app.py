"""
============================================================
app.py — Dashboard Streamlit (version provisoire)
============================================================
Ce fichier sera complété au Jour 3 du projet.
Pour l'instant, il affiche juste un message de démarrage
pour confirmer que le conteneur fonctionne correctement.
============================================================
"""

import streamlit as st
import os

# Configuration de la page
st.set_page_config(
    page_title="Wikimedia Real-Time Dashboard",
    page_icon="🌍",
    layout="wide"
)

# Titre principal
st.title("🌍 Real-Time Wikimedia Lakehouse")
st.markdown("### Dashboard en cours de construction...")

# Informations de connexion
st.info(
    "✅ Le conteneur Streamlit fonctionne correctement.\n\n"
    "Le dashboard complet sera développé au **Jour 3** du projet, "
    "une fois que les couches Bronze, Silver et Gold seront opérationnelles."
)

# Afficher les variables d'environnement (pour vérification)
st.markdown("---")
st.markdown("#### 🔧 Configuration détectée")

col1, col2 = st.columns(2)
with col1:
    st.metric("MinIO Endpoint", os.getenv("MINIO_ENDPOINT", "Non défini"))
    st.metric("Gold Top Path", os.getenv("GOLD_TOP_PATH", "Non défini"))
with col2:
    st.metric("MinIO User", os.getenv("MINIO_ROOT_USER", "Non défini"))
    st.metric("Gold Metrics Path", os.getenv("GOLD_METRICS_PATH", "Non défini"))
