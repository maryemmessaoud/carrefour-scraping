"""
Script de nettoyage unique pour les fichiers CSV Carrefour
(carrefour_cuisine.csv, carrefour_smartphones.csv, ou toute autre catégorie
suivant le même schéma) avant chargement dans un pipeline ETL cloud.

Usage :
    python clean_carrefour_csv.py carrefour_cuisine.csv carrefour_cuisine_clean.csv \
        --expected-category "Cuisine"

    python clean_carrefour_csv.py carrefour_smartphones.csv carrefour_smartphones_clean.csv \
        --expected-category "Smartphones et Objets connectés"

Corrections appliquées :
    - Suppression des lignes entièrement vides
    - Typage strict : ean (texte), price/promo_price (float), available (bool)
    - Suppression des lignes sans EAN, sans titre, ou avec prix invalide (<=0 / non numérique)
    - Suppression des doublons stricts sur EAN
    - Nettoyage des titres : espaces multiples, entités HTML (&deg; -> °), symboles parasites en bord de chaîne
    - Normalisation des marques : correction des typos connues, valeurs "vides"/
      "AUCUNE_MARQUE"/"N/A" -> NULL explicite
    - Category forcée à la valeur attendue si fournie (--expected-category), écarts comptabilisés
    - Correction des fausses promos (promo_price >= price -> NULL)
    - Détection (sans suppression) des EAN qui ne respectent pas le format GTIN standard
      (8, 12, 13 ou 14 chiffres) -> utile pour les UUID marketplace mal mappés
    - Colonnes de lineage ajoutées : scraped_at, source_file
    - Garde-fou final : vérifie que 'ean' est utilisable comme clé primaire
      (non-null + unique) avant d'écrire le fichier de sortie
    - Rapport de nettoyage récapitulatif affiché en fin d'exécution
"""

import argparse
import html
import re
import sys
from datetime import datetime, timezone

import pandas as pd

# ---------------------------------------------------------------------------
# Tables de référence (à enrichir au fil des runs de scraping)
# ---------------------------------------------------------------------------

BRAND_NORMALIZATION = {
    "CARREFOUR HOM": "CARREFOUR HOME",
    "HELL S KITCHEN": "HELL'S KITCHEN",
}

# Valeurs qui signifient "pas de marque" et doivent devenir NULL
BRAND_NULL_VALUES = {"", "AUCUNE_MARQUE", "N/A", "NA", "NONE", "-"}

# Un GTIN valide fait 8, 12, 13 ou 14 chiffres (EAN-8, UPC-12, EAN-13, GTIN-14)
GTIN_RE = re.compile(r"^\d{8}$|^\d{12}$|^\d{13}$|^\d{14}$")


# ---------------------------------------------------------------------------
# Fonctions de nettoyage unitaires
# ---------------------------------------------------------------------------

def clean_title(series: pd.Series) -> pd.Series:
    """Décode les entités HTML, normalise les espaces, retire les symboles
    parasites en début/fin de chaîne (ex: '**steelie...  *')."""
    out = series.astype("string")
    out = out.map(lambda x: html.unescape(x) if pd.notna(x) else x)
    out = out.str.replace(r"\s+", " ", regex=True).str.strip()
    out = out.str.strip("*_- ")
    return out


def clean_ean(series: pd.Series) -> pd.Series:
    """Force l'EAN en texte pour préserver les zéros non significatifs."""
    return series.astype("string").str.strip()


def clean_available(series: pd.Series) -> pd.Series:
    """Convertit 'available' (souvent 'True'/'False' en texte) en bool nullable."""
    mapping = {
        "true": True, "false": False,
        "1": True, "0": False,
        "yes": True, "no": False,
    }
    return (
        series.astype("string")
        .str.strip()
        .str.lower()
        .map(mapping)
        .astype("boolean")
    )


def clean_brand(series: pd.Series):
    """Normalise espaces/typos et remplace les valeurs 'vides' par NULL explicite.
    Retourne (série nettoyée, nombre de valeurs corrigées par la table de mapping)."""
    out = series.astype("string").str.replace(r"\s+", " ", regex=True).str.strip()
    n_mapped = out.isin(BRAND_NORMALIZATION.keys()).sum()
    out = out.replace(BRAND_NORMALIZATION)

    upper = out.str.upper()
    is_null_like = upper.isin({v.upper() for v in BRAND_NULL_VALUES}) | out.isna()
    out = out.mask(is_null_like, pd.NA)

    return out, int(n_mapped)


def flag_invalid_gtin(ean_series: pd.Series) -> pd.Series:
    """Retourne un masque booléen : True si l'EAN NE respecte PAS le format GTIN
    standard (ex: UUID marketplace glissé dans le champ EAN)."""
    return ~ean_series.fillna("").str.match(GTIN_RE)


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

def clean_dataframe(df: pd.DataFrame, expected_category, source_file: str):
    report = {}
    report["n_read"] = len(df)

    # 1. Lignes entièrement vides
    n_before = len(df)
    df = df.dropna(how="all")
    report["n_empty_rows_removed"] = n_before - len(df)

    # 2. Typage EAN + suppression EAN manquants
    df["ean"] = clean_ean(df["ean"])
    n_before = len(df)
    df = df[df["ean"].notna() & (df["ean"].str.len() > 0)]
    report["n_missing_ean_removed"] = n_before - len(df)

    # 3. Titres : nettoyage + suppression titres vides
    df["title"] = clean_title(df["title"])
    n_before = len(df)
    df = df[df["title"].notna() & (df["title"].str.len() > 0)]
    report["n_empty_title_removed"] = n_before - len(df)

    # 4. Doublons stricts sur EAN
    n_before = len(df)
    df = df.drop_duplicates(subset=["ean"], keep="first")
    report["n_duplicates_removed"] = n_before - len(df)

    # 5. Prix : cast + suppression prix invalides (NaN ou <= 0)
    df["price"] = pd.to_numeric(df["price"], errors="coerce").round(2)
    n_before = len(df)
    df = df[df["price"].notna() & (df["price"] > 0)]
    report["n_invalid_price_removed"] = n_before - len(df)

    # 6. Promo : cast + correction des fausses promos (promo_price >= price)
    df["promo_price"] = pd.to_numeric(df["promo_price"], errors="coerce").round(2)
    invalid_promo_mask = df["promo_price"].notna() & (df["promo_price"] >= df["price"])
    report["n_promo_corrected"] = int(invalid_promo_mask.sum())
    df.loc[invalid_promo_mask, "promo_price"] = pd.NA

    # 7. available -> bool
    df["available"] = clean_available(df["available"])

    # 8. Brand : normalisation + valeurs vides -> NULL
    df["brand"], n_brand_mapped = clean_brand(df["brand"])
    report["n_brand_replaced"] = n_brand_mapped

    # 9. Category forcée à la valeur attendue
    df["category"] = df["category"].astype("string").str.strip()
    if expected_category:
        n_mismatch = int((df["category"].str.lower() != expected_category.lower()).sum())
        df["category"] = expected_category
        report["n_category_replaced"] = n_mismatch
    else:
        report["n_category_replaced"] = 0

    # 10. Détection (sans suppression) des EAN au format non-GTIN (ex: UUID marketplace)
    invalid_gtin_mask = flag_invalid_gtin(df["ean"])
    report["n_ean_non_gtin_format"] = int(invalid_gtin_mask.sum())

    # 11. Colonnes de lineage
    df["scraped_at"] = datetime.now(timezone.utc).isoformat()
    df["source_file"] = source_file

    # 12. Garde-fou final : 'ean' doit être utilisable comme clé primaire.
    #     Toutes les étapes précédentes sont censées garantir ça (suppression des
    #     EAN manquants à l'étape 2, dédoublonnage à l'étape 4) ; ces deux
    #     assertions sont un filet de sécurité qui arrête le script plutôt que
    #     d'écrire un fichier avec une PK invalide si jamais la logique change
    #     un jour ou qu'un cas non prévu se glisse dans les données.
    assert df["ean"].notna().all(), "PK invalide : EAN manquant détecté"
    assert df["ean"].is_unique, "PK invalide : EAN dupliqué détecté"

    report["n_kept"] = len(df)
    return df.reset_index(drop=True), report


def print_report(report: dict, output_path: str) -> None:
    """Affiche un rapport de nettoyage récapitulatif, format terminal."""
    sep = "=" * 55
    print(sep)
    print("NETTOYAGE DU FICHIER CSV".center(55))
    print(sep)
    print()
    print(f"Produits chargés : {report['n_read']}")
    print()
    print(sep)
    print("RAPPORT DE NETTOYAGE".center(55))
    print(sep)
    print(f"Produits lus                    : {report['n_read']}")
    print(f"Lignes vides supprimées         : {report['n_empty_rows_removed']}")
    print(f"EAN manquants supprimés         : {report['n_missing_ean_removed']}")
    print(f"Titres vides supprimés          : {report['n_empty_title_removed']}")
    print(f"Doublons supprimés              : {report['n_duplicates_removed']}")
    print(f"Prix invalides supprimés        : {report['n_invalid_price_removed']}")
    print(f"Prix promotion corrigés         : {report['n_promo_corrected']}")
    print(f"Marques remplacées              : {report['n_brand_replaced']}")
    print(f"Catégories remplacées           : {report['n_category_replaced']}")
    print(f"EAN format non-GTIN (signalés)  : {report['n_ean_non_gtin_format']}")
    print(f"Produits conservés              : {report['n_kept']}")
    print()
    print("Types finaux :")
    for col, dtype in report["dtypes"].items():
        print(f"{col:<20} {dtype}")
    print()
    print(f"Fichier enregistré : {output_path}")
    print(sep)
    print("NETTOYAGE TERMINÉ AVEC SUCCÈS".center(55))
    print(sep)
    print()
    print("Vérification clé primaire (ean) : OK -> non-null + unique")


def main():
    parser = argparse.ArgumentParser(description="Nettoyage CSV Carrefour pour ETL cloud.")
    parser.add_argument("input_csv", help="Chemin du CSV brut à nettoyer")
    parser.add_argument("output_csv", help="Chemin du CSV nettoyé en sortie")
    parser.add_argument(
        "--expected-category",
        default=None,
        help="Catégorie attendue (ex: 'Cuisine' ou 'Smartphones et Objets connectés'). "
             "Si fournie, la colonne 'category' est forcée à cette valeur.",
    )
    args = parser.parse_args()

    df = pd.read_csv(args.input_csv, dtype={"ean": str})

    required_cols = {"ean", "title", "brand", "category", "price", "promo_price", "available"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Colonnes manquantes dans le CSV d'entrée : {missing}")

    try:
        df_clean, report = clean_dataframe(df, args.expected_category, source_file=args.input_csv)
    except AssertionError as e:
        print(f"[ERREUR] {e}", file=sys.stderr)
        print("[ERREUR] Fichier de sortie NON écrit -- corrigez les données sources.", file=sys.stderr)
        sys.exit(1)

    df_clean.to_csv(args.output_csv, index=False)

    report["dtypes"] = df_clean.dtypes.astype(str).to_dict()
    print_report(report, args.output_csv)


if __name__ == "__main__":
    main()