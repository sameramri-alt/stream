"""
=================================================================
app.py — Dashboard Streamlit pour le lakehouse Wikimedia
=================================================================

Ce fichier est la couche de visualisation du système.
Son rôle n'est pas de traiter les données, mais de les lire dans MinIO
et de les afficher de manière claire pour l'utilisateur.

Le dashboard sert à répondre à des questions comme :
- quels sont les articles les plus modifiés ?
- quelles langues Wikipedia sont les plus actives ?
- comment évolue le trafic d'éditions dans le temps ?
=================================================================
"""

import os
import time
from datetime import datetime
from urllib.parse import urlparse

import duckdb
import pandas as pd
import plotly.express as px
import pyarrow.dataset as ds
import pyarrow.fs as pafs
import streamlit as st

# ---------------------------------------------------------------
# 1. Configuration de la page Streamlit
# ---------------------------------------------------------------
st.set_page_config(
    page_title="Wikimedia Real-Time Dashboard",
    page_icon="🌍",
    layout="wide",
)


# ---------------------------------------------------------------
# 2. Fonction de lecture des données Gold depuis MinIO
# ---------------------------------------------------------------
# ttl=15 signifie que le cache expire toutes les 15 secondes.
# À chaque expiration, Streamlit relit les nouveaux fichiers Parquet.
@st.cache_data(ttl=15)
def load_gold_dataset(path: str) -> pd.DataFrame:
    """
    Lit un dataset Parquet stocké dans MinIO et le retourne
    sous forme de DataFrame pandas.
    """
    parsed = urlparse(path)
    bucket = parsed.netloc
    key_prefix = parsed.path.lstrip("/")

    if not bucket:
        return pd.DataFrame()

    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    endpoint_host = endpoint.replace("http://", "").replace("https://", "")
    access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")

    fs = pafs.S3FileSystem(
        endpoint_override=endpoint_host,
        access_key=access_key,
        secret_key=secret_key,
        region="us-east-1",
        scheme="http",
    )

    dataset_path = f"{bucket}/{key_prefix}"
    try:
        table = ds.dataset(dataset_path, filesystem=fs, format="parquet").to_table()
        return table.to_pandas()
    except Exception:
        return pd.DataFrame()


# ---------------------------------------------------------------
# 3. Chemins des données Gold
# ---------------------------------------------------------------
GOLD_TOP_PATH = os.getenv("GOLD_TOP_PATH", "s3a://lakehouse/gold/top_articles/")
GOLD_METRICS_PATH = os.getenv("GOLD_METRICS_PATH", "s3a://lakehouse/gold/metrics_by_wiki/")
GOLD_LANG_PATH = os.getenv("GOLD_LANG_PATH", "s3a://lakehouse/gold/edits_by_language/")
GOLD_TIMESERIES_PATH = os.getenv("GOLD_TIMESERIES_PATH", "s3a://lakehouse/gold/edits_timeseries/")

# ---------------------------------------------------------------
# 4. En-tête du dashboard
# ---------------------------------------------------------------
st.title("🌍 Real-Time Wikimedia Lakehouse")

# Afficher l'heure de la dernière mise à jour
st.caption(f"🕐 Dernière mise à jour : {datetime.now().strftime('%H:%M:%S')} — Rafraîchissement automatique toutes les 15 secondes")

# Bouton de rafraîchissement manuel
col_refresh, _ = st.columns([1, 5])
with col_refresh:
    if st.button("🔄 Rafraîchir maintenant"):
        st.cache_data.clear()
        st.rerun()

st.markdown("---")

# ---------------------------------------------------------------
# 5. Chargement de toutes les données Gold
# ---------------------------------------------------------------
with st.spinner("Chargement des données Gold depuis MinIO..."):
    top_articles = load_gold_dataset(GOLD_TOP_PATH)
    metrics = load_gold_dataset(GOLD_METRICS_PATH)
    lang_data = load_gold_dataset(GOLD_LANG_PATH)
    timeseries_data = load_gold_dataset(GOLD_TIMESERIES_PATH)

# Vérification : si aucune donnée n'est disponible
if top_articles.empty and metrics.empty and lang_data.empty and timeseries_data.empty:
    st.warning("⚠️ Aucune donnée Gold détectée. Vérifiez que le job Spark tourne.")
    st.stop()

# ---------------------------------------------------------------
# 6. SECTION 1 : Métriques globales en haut de page
# ---------------------------------------------------------------
st.markdown("## 📊 Vue d'ensemble")

col1, col2, col3, col4 = st.columns(4)

# Nombre total d'éditions (somme de toutes les lignes Gold top_articles)
total_edits = int(top_articles["total_edits"].sum()) if not top_articles.empty else 0
col1.metric("📝 Total éditions", f"{total_edits:,}")

# Nombre d'articles uniques
unique_articles = len(top_articles["article_title"].unique()) if not top_articles.empty else 0
col2.metric("📄 Articles uniques", f"{unique_articles:,}")

# Nombre de langues actives
active_langs = len(lang_data["wiki_language"].unique()) if not lang_data.empty else 0
col3.metric("🌐 Langues actives", f"{active_langs:,}")

# Nombre de points temporels
ts_points = len(timeseries_data) if not timeseries_data.empty else 0
col4.metric("⏱️ Points temporels", f"{ts_points:,}")

st.markdown("---")

# ---------------------------------------------------------------
# 7. SECTION 2 : Top 10 des articles les plus modifiés
# ---------------------------------------------------------------
st.markdown("## 📈 Top 10 des articles les plus modifiés")

if not top_articles.empty:
    con = duckdb.connect(database=":memory:")
    top_10 = con.execute(
        """
        SELECT article_title, SUM(total_edits) as total_edits
        FROM top_articles
        GROUP BY article_title
        ORDER BY total_edits DESC
        LIMIT 10
        """
    ).df()

    if not top_10.empty:
        fig_top = px.bar(
            top_10,
            x="total_edits",
            y="article_title",
            orientation="h",
            title="",
            color="total_edits",
            color_continuous_scale="Viridis",
            labels={"article_title": "Article", "total_edits": "Nombre d'éditions"},
        )
        fig_top.update_layout(
            yaxis=dict(autorange="reversed"),
            height=400,
            showlegend=False,
        )
        st.plotly_chart(fig_top, use_container_width=True)
else:
    st.info("Pas encore de données pour les articles.")

st.markdown("---")

# ---------------------------------------------------------------
# 8. SECTION 3 (NOUVEAU) : Éditions par langue Wikipedia
# ---------------------------------------------------------------
st.markdown("## 🌐 Éditions par langue Wikipedia")

if not lang_data.empty:
    con = duckdb.connect(database=":memory:")
    lang_agg = con.execute(
        """
        SELECT wiki_language, SUM(edit_count) as total_edits
        FROM lang_data
        GROUP BY wiki_language
        ORDER BY total_edits DESC
        LIMIT 20
        """
    ).df()

    if not lang_agg.empty:
        col_pie, col_bar = st.columns(2)

        with col_pie:
            # Camembert des 10 premières langues
            fig_pie = px.pie(
                lang_agg.head(10),
                values="total_edits",
                names="wiki_language",
                title="Répartition des 10 langues les plus actives",
                color_discrete_sequence=px.colors.qualitative.Set3,
            )
            fig_pie.update_traces(textposition="inside", textinfo="percent+label")
            fig_pie.update_layout(height=450)
            st.plotly_chart(fig_pie, use_container_width=True)

        with col_bar:
            # Barres horizontales des 20 premières langues
            fig_lang = px.bar(
                lang_agg,
                x="total_edits",
                y="wiki_language",
                orientation="h",
                title="Top 20 des langues par nombre d'éditions",
                color="total_edits",
                color_continuous_scale="Tealgrn",
                labels={"wiki_language": "Serveur Wikipedia", "total_edits": "Éditions"},
            )
            fig_lang.update_layout(
                yaxis=dict(autorange="reversed"),
                height=450,
                showlegend=False,
            )
            st.plotly_chart(fig_lang, use_container_width=True)
else:
    st.info("Pas encore de données par langue. Attendez que le job Spark traite quelques lots.")

st.markdown("---")

# ---------------------------------------------------------------
# 9. SECTION 4 (NOUVEAU) : Évolution du trafic dans le temps
# ---------------------------------------------------------------
st.markdown("## 📉 Évolution du trafic Wikipedia (éditions par minute)")

if not timeseries_data.empty:
    con = duckdb.connect(database=":memory:")
    ts_agg = con.execute(
        """
        SELECT minute, SUM(edits_per_minute) as edits_per_minute
        FROM timeseries_data
        GROUP BY minute
        ORDER BY minute ASC
        """
    ).df()

    if not ts_agg.empty:
        fig_ts = px.line(
            ts_agg,
            x="minute",
            y="edits_per_minute",
            title="",
            labels={"minute": "Heure", "edits_per_minute": "Éditions / minute"},
            markers=True,
        )
        fig_ts.update_traces(
            line=dict(color="#00CC96", width=2),
            marker=dict(size=5),
        )
        fig_ts.update_layout(
            height=400,
            xaxis_title="Temps",
            yaxis_title="Nombre d'éditions par minute",
        )
        st.plotly_chart(fig_ts, use_container_width=True)
else:
    st.info("Pas encore de données temporelles. Attendez quelques minutes.")

st.markdown("---")

# ---------------------------------------------------------------
# 10. Tableaux de données brutes (pour exploration)
# ---------------------------------------------------------------
with st.expander("📋 Voir les données brutes (cliquer pour ouvrir)"):
    if not top_articles.empty:
        st.markdown("### Top articles")
        st.dataframe(top_articles.head(30), use_container_width=True)

    if not metrics.empty:
        st.markdown("### Métriques par namespace")
        st.dataframe(metrics.head(30), use_container_width=True)

    if not lang_data.empty:
        st.markdown("### Éditions par langue")
        st.dataframe(lang_data.head(30), use_container_width=True)

# ---------------------------------------------------------------
# 11. AUTO-REFRESH : Recharge automatique toutes les 15 secondes
# ---------------------------------------------------------------
# Cette boucle attend 15 secondes puis force un rechargement complet
# de la page, ce qui déclenche la relecture des données depuis MinIO.
time.sleep(15)
st.rerun()
