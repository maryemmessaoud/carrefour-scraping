# Carrefour France — Pipeline Data Engineering de bout en bout

Conception et mise en œuvre d'un pipeline complet de Data Engineering appliqué au site e-commerce **Carrefour France** (carrefour.fr) : Web Scraping → Nettoyage → Data Warehouse → Data Visualisation.

Projet réalisé dans le cadre d'un stage d'été chez **DevoTeam Tunisie** (1ère année Cycle Ingénieur, Science des Données — FST Tunis).

## 🎯 Objectif

Collecter, structurer et analyser les données produits de cinq catégories du catalogue Carrefour (prix, promotions, disponibilité, marque) afin de construire un outil de *competitive intelligence / price monitoring* : entrepôt de données décisionnel + tableaux de bord business et data science.

**Catégories couvertes :** Smartphones et Objets connectés · Jardin · Cuisine · Livres et Culture · Beauté et Santé

**Volume collecté :** plus de 6 000 offres produits

## 🏗️ Architecture du pipeline

```
Carrefour.fr  →  Scraping  →  Nettoyage  →  BigQuery (3 couches +      →  Looker Studio
                (Scrapy +      (Python /       modèle en flocon          (Business +
                Playwright)    pandas)         de neige)                 Data Science)
```

1. **Extraction** — Web scraping avec Scrapy + Playwright (rendu JavaScript, contournement anti-bot)
2. **Nettoyage** — Structuration et fiabilisation des données avec Python/pandas
3. **Modélisation & chargement** — Schéma en flocon de neige, architecture BigQuery en 3 couches (raw → staging → DWH)
4. **Visualisation** — Dashboards interactifs Looker Studio + couche analytique BigQuery ML

## 🛠️ Stack technique

| Étape | Technologies |
|---|---|
| Web Scraping | Scrapy, scrapy-playwright, Playwright (Chromium headless) |
| Nettoyage | Python, pandas |
| Data Warehouse | Google BigQuery (SQL, modélisation dimensionnelle de Kimball) |
| Machine Learning | BigQuery ML (K-means, détection d'anomalies IQR) |
| Visualisation | Looker Studio |
| Authentification GCP | ADC (gcloud) / Compte de service (IAM) |


## 🕷️ 1. Web Scraping

Le site étant une Single Page Application protégée par des mécanismes anti-bot, un simple client HTTP (`requests`) ne suffit pas : le contenu produit est injecté dynamiquement via `window.__INITIAL_STATE__` après exécution du JavaScript.

- **Scrapy** pour l'orchestration (pagination, concurrence, retries, logging)
- **Playwright** (Chromium headless) pour le rendu JavaScript complet
- Décodage du format de sérialisation `devalue` (reconstruction récursive de l'état applicatif)
- Règle de priorité vendeur (Carrefour > marketplace) et pagination résiliente (un échec ponctuel n'interrompt pas le crawl)

```bash
python -m venv venv
venv\Scripts\activate        # Windows
pip install scrapy scrapy-playwright
playwright install

scrapy crawl carrefour_cuisine -o carrefour_cuisine.csv
```

## 🧹 2. Nettoyage des données

Script unique `clean_carrefour_csv.py`, réutilisable pour les cinq catégories, appliquant : typage strict, suppression des doublons/valeurs invalides, décodage des entités HTML, normalisation des marques, correction des fausses promotions, détection (sans suppression) des EAN non-GTIN, et garde-fous sur l'unicité de la clé primaire.

```bash
pip install pandas
python clean_carrefour_csv.py carrefour_cuisine.csv carrefour_cuisine_clean.csv --expected-category "Cuisine"
```

## 🗄️ 3. Data Warehouse — BigQuery

Architecture en trois couches :

| Dataset | Rôle |
|---|---|
| `DWH_Carrefour_ScrapingData` | Couche brute (staging), chargement autodetect |
| `DWH_Carrefour_Staging` | Couche structurée par entité |
| `DWH_Carrefour` | Modèle final en flocon de neige + couche analytique |

**Modèle dimensionnel** (méthode de Kimball) : `Fact_OFFRES` reliée à `Dim_PRODUIT` (elle-même reliée à `Dim_MARQUE` et `Dim_CATEGORIE`), `Dim_VENDEUR` et `Dim_TEMPS` (table plate).

```bash
gcloud auth application-default login
python3 load_to_bigquery.py
```

> Chargement actuel en `WRITE_TRUNCATE` (full refresh / SCD1). Une évolution vers une **SCD2** (historisation des dimensions) est documentée comme piste d'amélioration pour permettre le suivi de prix dans le temps.

## 📊 4. Data Visualisation — Looker Studio

**Volet Business** : KPI globaux, classement des marques, comparaison Carrefour vs Marketplace, distribution des prix.

**Volet Data Science** (BigQuery ML) :
- Segmentation prix par **K-means** (3 segments : Entrée/Milieu/Premium, basée sur le z-score du prix par catégorie)
- Détection d'anomalies de prix par méthode **IQR**
- Matrice de positionnement marque × catégorie

**Indicateurs clés obtenus :** 6 228 produits · 176,40 € prix moyen · 21,56 % taux de promo moyen · segmentation 75,5 % entrée de gamme / 13,6 % milieu de gamme / 10,8 % premium · 9,55 % du catalogue signalé en anomalie de prix (concentré sur la catégorie Jardin)

## 🚀 Perspectives d'évolution

- Orchestration et automatisation (Airflow / Cloud Scheduler)
- Chargement incrémental et historisation SCD2
- Robustesse accrue du scraping (rotation user-agents/proxys)
- Élargissement à d'autres catégories/enseignes concurrentes
- Modèles ML additionnels (prédiction de prix, NLP sur les titres produits)
- Tests de qualité de données automatisés (Great Expectations)

## 👤 Auteur

**Maryem Messaoud** — 1ère année Cycle Ingénieur, Science des Données, Faculté des Sciences de Tunis
Stage encadré par M. Houssem Khemiri — DevoTeam Tunisie (juillet–août 2026)
