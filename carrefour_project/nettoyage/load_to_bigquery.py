"""
load_to_bigquery.py
--------------------
Charge les 5 CSV Carrefour (beaute_sante, cuisine, jardin, livres, smartphones)
dans BigQuery, puis construit le modele en etoile complet :
  - staging.produits_raw          (donnees brutes concatenees)
  - dwh_carrefour.Dim_MARQUE
  - dwh_carrefour.Dim_CATEGORIE
  - dwh_carrefour.Dim_VENDEUR
  - dwh_carrefour.Dim_PRODUIT
  - dwh_carrefour.Dim_TEMPS
  - dwh_carrefour.Fact_OFFRES

Prerequis :
  pip install --user google-cloud-bigquery pandas pandas-gbq db-dtypes
  Etre authentifie (Cloud Shell l'est automatiquement via gcloud)

Usage :
  python3 load_to_bigquery.py
"""

import re
import pandas as pd
from google.cloud import bigquery

# ============================================================
# CONFIGURATION - a adapter
# ============================================================
PROJECT_ID = "houssemkhemiri-sandbox-khemiri"         
DATASET_ID = "dwh_carrefour"           # nom du dataset cree a l'etape 4
STAGING_TABLE = f"{PROJECT_ID}.{DATASET_ID}.staging_produits_raw"

# Chemin des CSV (uploades dans $HOME sur Cloud Shell)
CSV_FILES = [
    "carrefour_beaute_sante_clean.csv",
    "carrefour_cuisine_clean.csv",
    "carrefour_jardin_clean.csv",
    "carrefour_livres_clean.csv",
    "carrefour_smartphones_clean.csv",
]

EAN_REGEX = re.compile(r"^\d{8}$|^\d{12,14}$")  # EAN8, UPC-12, EAN13, GTIN14


# ============================================================
# ETAPE 1 - Lecture et nettoyage local des CSV
# ============================================================
def load_and_prepare_csvs(files):
    frames = []
    for f in files:
        df = pd.read_csv(f, dtype={"ean": str})
        # certains fichiers (cuisine avant correction) pouvaient ne pas avoir seller_type
        if "seller_type" not in df.columns:
            df["seller_type"] = "carrefour"
        frames.append(df)

    full = pd.concat(frames, ignore_index=True)

    # Securite supplementaire sur les colonnes texte (au cas ou)
    full["brand"] = full["brand"].fillna("Non spécifié").replace("", "Non spécifié")
    full["seller_type"] = full["seller_type"].fillna("carrefour")

    # Flag EAN valide (GTIN) vs UUID marketplace / valeur non standard
    full["is_ean_valide_gtin"] = full["ean"].astype(str).apply(
        lambda x: bool(EAN_REGEX.match(x.strip()))
    )

    # Typage propre
    full["price"] = pd.to_numeric(full["price"], errors="coerce")
    full["promo_price"] = pd.to_numeric(full["promo_price"], errors="coerce")
    full["available"] = full["available"].astype(bool)
    full["scraped_at"] = pd.to_datetime(full["scraped_at"], errors="coerce", utc=True)

    return full


# ============================================================
# ETAPE 2 - Chargement de la table de staging dans BigQuery
# ============================================================
def load_staging_table(client, df):
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # ecrase a chaque execution
        autodetect=True,
    )
    job = client.load_table_from_dataframe(df, STAGING_TABLE, job_config=job_config)
    job.result()  # attend la fin du job
    print(f"Staging charge : {job.output_rows} lignes -> {STAGING_TABLE}")


# ============================================================
# ETAPE 3 - Construction du modele en etoile (SQL DDL/DML)
# ============================================================
DDL_STATEMENTS = [
    # --- Dim_MARQUE ---
    f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.Dim_MARQUE` AS
    SELECT
      ROW_NUMBER() OVER (ORDER BY marque) AS marque_id,
      marque AS nom_marque
    FROM (
      SELECT DISTINCT brand AS marque
      FROM `{STAGING_TABLE}`
    )
    """,

    # --- Dim_CATEGORIE ---
    f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.Dim_CATEGORIE` AS
    SELECT
      ROW_NUMBER() OVER (ORDER BY categorie) AS categorie_id,
      categorie AS nom_categorie
    FROM (
      SELECT DISTINCT category AS categorie
      FROM `{STAGING_TABLE}`
    )
    """,

    # --- Dim_VENDEUR ---
    f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.Dim_VENDEUR` AS
    SELECT
      ROW_NUMBER() OVER (ORDER BY type_vendeur) AS vendeur_id,
      type_vendeur
    FROM (
      SELECT DISTINCT seller_type AS type_vendeur
      FROM `{STAGING_TABLE}`
    )
    """,

    # --- Dim_PRODUIT (un ean = une ligne, on garde la version la plus recente) ---
    f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.Dim_PRODUIT` AS
    WITH derniere_version AS (
      SELECT
        ean, title, brand, category, is_ean_valide_gtin,
        ROW_NUMBER() OVER (PARTITION BY ean ORDER BY scraped_at DESC) AS rn
      FROM `{STAGING_TABLE}`
    )
    SELECT
      d.ean,
      d.title AS titre,
      m.marque_id,
      c.categorie_id,
      d.is_ean_valide_gtin
    FROM derniere_version d
    JOIN `{PROJECT_ID}.{DATASET_ID}.Dim_MARQUE` m ON m.nom_marque = d.brand
    JOIN `{PROJECT_ID}.{DATASET_ID}.Dim_CATEGORIE` c ON c.nom_categorie = d.category
    WHERE d.rn = 1
    """,

    # --- Dim_TEMPS (une ligne par jour de scraping, aplatie) ---
    f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.Dim_TEMPS` AS
    SELECT
      ROW_NUMBER() OVER (ORDER BY jour) AS jour_id,
      jour AS date_complete,
      EXTRACT(DAY FROM jour) AS jour_du_mois,
      FORMAT_DATE('%A', jour) AS nom_jour,
      EXTRACT(DAYOFWEEK FROM jour) AS jour_semaine,
      EXTRACT(WEEK FROM jour) AS numero_semaine,
      EXTRACT(DAYOFWEEK FROM jour) IN (1, 7) AS is_weekend,
      EXTRACT(MONTH FROM jour) AS numero_mois,
      FORMAT_DATE('%B', jour) AS nom_mois,
      EXTRACT(QUARTER FROM jour) AS trimestre,
      EXTRACT(YEAR FROM jour) AS annee
    FROM (
      SELECT DISTINCT DATE(scraped_at) AS jour
      FROM `{STAGING_TABLE}`
    )
    """,

    # --- Fact_OFFRES ---
    f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.Fact_OFFRES` AS
    SELECT
      GENERATE_UUID() AS offre_id,
      s.ean,
      v.vendeur_id,
      t.jour_id,
      s.price AS prix,
      s.promo_price AS prix_promo,
      CASE WHEN s.promo_price IS NOT NULL
           THEN ROUND(s.price - s.promo_price, 2) END AS remise_montant,
      CASE WHEN s.promo_price IS NOT NULL AND s.price > 0
           THEN ROUND((s.price - s.promo_price) / s.price * 100, 2) END AS remise_pourcentage,
      s.available AS disponibilite,
      s.scraped_at
    FROM `{STAGING_TABLE}` s
    JOIN `{PROJECT_ID}.{DATASET_ID}.Dim_VENDEUR` v ON v.type_vendeur = s.seller_type
    JOIN `{PROJECT_ID}.{DATASET_ID}.Dim_TEMPS` t ON t.date_complete = DATE(s.scraped_at)
    """,
]


def build_star_schema(client):
    for i, stmt in enumerate(DDL_STATEMENTS, start=1):
        print(f"Execution etape SQL {i}/{len(DDL_STATEMENTS)}...")
        client.query(stmt).result()
    print("Modele en etoile construit avec succes.")


# ============================================================
# MAIN
# ============================================================
def main():
    print("Lecture des CSV...")
    df = load_and_prepare_csvs(CSV_FILES)
    print(f"{len(df)} lignes chargees depuis {len(CSV_FILES)} fichiers.")

    client = bigquery.Client(project=PROJECT_ID)

    print("Chargement en staging BigQuery...")
    load_staging_table(client, df)

    print("Construction du modele en etoile...")
    build_star_schema(client)

    print("\nTerminé. Tables disponibles dans BigQuery :")
    for t in ["Dim_MARQUE", "Dim_CATEGORIE", "Dim_VENDEUR", "Dim_PRODUIT", "Dim_TEMPS", "Fact_OFFRES"]:
        print(f"  - {DATASET_ID}.{t}")


if __name__ == "__main__":
    main()