"""
Streamlit application for performance attribution reporting.

This app guides the user through three steps:

1. **Mapping upload** – load a CSV with at least the columns `code_report`,
   `ID`, `NOM` and `DEVISE`, plus optional `TYPE_PART`, `PORTEFEUILLE` and
   `BENCHMARK`. The mapping is used to match Bloomberg BONE exports and
   optionally fetch IFM data.

2. **BONE imports** – upload all Bloomberg exports (MTD/QTD/YTD) in one go.
   The user selects the period to analyse (MTD/QTD/YTD). Only files whose
   names contain `_<period>_` are considered. The app does not display
   the files themselves, but reports the number of files processed and
   warns of any parsing issues.

3. **Analyses** – display two tables: "Analyse interne" comparing IFM and
   BONE values for the portfolio and benchmark, and "Attribution PF vs
   BM" comparing portfolio and benchmark performance. Both tables can
   optionally incorporate IFM data either from a SQL server or from an
   uploaded Excel file. The user chooses the source and provides
   necessary connection details or column mappings.

Additional features:

• **Theme** – a dark purple theme inspired by Natixis, including the
  company logo displayed at the top.

• **MultiIndex columns** – tables group Portefeuille, Benchmark and
  Écarts columns under hierarchical headers.

• **Excel export** – both tables can be downloaded as Excel files with
  decimal formatting preserved.

This file relies on the helper functions defined in `utils.py` for
common tasks such as reading the mapping, parsing BONE files and
constructing tables. To modify IFM column names or period mappings,
adjust `utils.py` accordingly.
"""

from __future__ import annotations

import datetime
from typing import List, Optional, Tuple

import pandas as pd
import streamlit as st

try:
    from . import utils  # type: ignore
except ImportError:
    import utils  # type: ignore


# ----------------------------------------------------------------------------
# UI styling
# ----------------------------------------------------------------------------

def inject_css() -> None:
    """
    Inject custom CSS to emulate a clean, Apple‑inspired aesthetic.

    The previous version of this app used a dark purple theme inspired by
    Natixis. To give the interface a lighter look reminiscent of
    Apple's design language, we define a new set of CSS variables for
    backgrounds, text and accent colours. Buttons retain rounded
    corners and use an iOS‑like blue accent. The overall palette
    maximises contrast between text and its container, as Apple
    recommends in its human interface guidelines.
    """
    st.markdown(
        """
        <style>
        :root {
          /* Light background and dark text inspired by Apple's UI */
          --bg: #f5f5f7;
          --card-bg: #ffffff;
          --text: #1d1d1f;
          /* Primary accent colour similar to iOS blue */
          --accent: #007aff;
          --accent-hover: #005bb5;
        }
        /* Apply global background and text colours */
        .stApp {
          background-color: var(--bg);
          color: var(--text);
        }
        /* Style for Streamlit buttons (upload, download, etc.) */
        .stButton>button,
        .stDownloadButton>button {
          background-color: var(--accent);
          color: #ffffff;
          border: none;
          border-radius: 8px;
          padding: 6px 12px;
          font-size: 1rem;
        }
        .stButton>button:hover,
        .stDownloadButton>button:hover {
          background-color: var(--accent-hover);
        }
        /* Adjust spacing at the top of the page */
        .block-container {
          padding-top: 1.2rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


# ----------------------------------------------------------------------------
# IFM Excel helper
# ----------------------------------------------------------------------------

def get_ifm_values_from_excel(
    df: pd.DataFrame,
    id_col: str,
    type_col: str,
    period_col: str,
    devise_col: str,
    pf_col: str,
    bm_col: str,
    date_col: Optional[str],
    id_ptf: str,
    type_part: str,
    devise: str,
    period_ui: str,
    asof: datetime.date,
) -> Tuple[Optional[float], Optional[float]]:
    """
    Retrieve IFM values from an uploaded Excel DataFrame. Filters the
    DataFrame on matching ID, Type Part, mapped period (using
    utils.PERIOD_MAP), Devise and, optionally, a date column. Returns
    (PF_IFM, BM_IFM) as floats or (None, None) if no match is found.
    """
    try:
        periode_calc = utils.PERIOD_MAP.get(period_ui, "1MOIS")
        # Normalise comparison values
        id_ptf_s = str(id_ptf).strip()
        type_part_s = str(type_part).strip()
        devise_s = str(devise).strip()

        # Build boolean mask with trimmed strings
        left_id = df[id_col].astype(str).str.strip()
        left_type = df[type_col].astype(str).str.strip()
        left_period = df[period_col].astype(str).str.strip()
        left_devise = df[devise_col].astype(str).str.strip()
        mask = (
            (left_id == id_ptf_s)
            & (left_type == type_part_s)
            & (left_period == periode_calc)
            & (left_devise == devise_s)
        )
        if date_col:
            # Only apply date filter if column exists; coerce invalid to NaT
            dt_series = pd.to_datetime(df[date_col], errors="coerce").dt.date
            mask = mask & (dt_series == asof)

        subset = df[mask]
        if subset.empty:
            return None, None
        row = subset.iloc[0]
        pf_val = row[pf_col] if pf_col in subset.columns else None
        bm_val = row[bm_col] if bm_col in subset.columns else None

        # Use tolerant numeric parsing (handles %, commas, parentheses)
        pf_float = utils._to_float(pf_val)
        bm_float = utils._to_float(bm_val)
        return pf_float, bm_float
    except Exception:
        return None, None


# ----------------------------------------------------------------------------
# Main application
# ----------------------------------------------------------------------------

def main() -> None:
    st.set_page_config(page_title="Reporting Attribution", layout="wide")
    inject_css()

    # Display logo (Natixis colours)
    logo_url = (
        "https://upload.wikimedia.org/wikipedia/fr/thumb/1/14/Natixis_logo.svg/640px-Natixis_logo.svg.png"
    )
    st.image(logo_url, width=200)
    st.title("Reporting d'attribution de performance")

    # ----------------------------------------------------------------------
    # Step 1: Mapping upload
    st.header("Étape 1 – Chargement du mapping")
    mapping_file = st.file_uploader("Fichier de mapping (.csv)", type=["csv"])
    if mapping_file:
        mapping_df = utils.load_mapping(mapping_file.getvalue())
    else:
        mapping_df = pd.DataFrame()

    # Harmonise ID as string right away (prevents int vs object merges later)
    if not mapping_df.empty and "ID" in mapping_df.columns:
        mapping_df["ID"] = mapping_df["ID"].astype(str)

    if mapping_df.empty:
        st.info(
            "Veuillez fournir un fichier de mapping comportant au moins les colonnes "
            "code_report, ID, NOM et DEVISE."
        )
        return
    st.success(f"Mapping chargé : {len(mapping_df)} lignes.")
    # We do not display the entire mapping table to reduce clutter

    # ----------------------------------------------------------------------
    # Step 2: BONE import and parameters
    st.header("Étape 2 – Importation des exports Bloomberg BONE")
    col1, col2 = st.columns([1, 1])
    with col1:
        period = st.radio("Période", ["MTD", "QTD", "YTD"], horizontal=True)
    with col2:
        asof_date = st.date_input(
            "Date de performance (colonne date pour IFM)",
            value=datetime.date.today(),
        )

    bone_files = st.file_uploader(
        "Chargez l'ensemble de vos fichiers BONE (.xlsx) pour toutes les périodes",
        type=["xlsx"],
        accept_multiple_files=True,
    )

    results: List[Tuple[str, Optional[float], Optional[float], List[str]]] = []
    warning_messages: List[str] = []
    if bone_files:
        # Process only files that correspond to the selected period
        for f in bone_files:
            code_report, pf_bone, bm_bone, warns = utils.parse_bone_file(f, period)
            # parse_bone_file returns (None, None, None, []) for files of other periods
            if code_report is None:
                continue
            results.append((code_report, pf_bone, bm_bone, warns))
            warning_messages.extend([f"{f.name}: {w}" for w in warns])
        total = len(results)
        st.success(f"{total} fichier(s) BONE traité(s) pour la période {period}.")
        if warning_messages:
            with st.expander("Avertissements lors de la lecture des fichiers BONE"):
                for msg in warning_messages:
                    st.warning(msg)

    if not results:
        st.info("Aucun fichier BONE chargé pour la période sélectionnée. Veuillez importer des fichiers.")
        return

    # ---- Helpers: auto‑map IFM column names (case/format tolerant) ----
    def _norm_colname(s: str) -> str:
        import re
        return re.sub(r"[^a-z0-9]", "", str(s).lower())

    def _best_match(cols, candidates):
        """Return the first column from cols matching any candidate alias.
        Matching ignores case, spaces, underscores, hyphens, etc.
        """
        norm_map = {c: _norm_colname(c) for c in cols}
        wanted = [_norm_colname(x) for x in candidates]
        # exact-like match first
        for col, normed in norm_map.items():
            if normed in wanted:
                return col
        # partial contains as fallback
        for w in wanted:
            for col, normed in norm_map.items():
                if w and w in normed:
                    return col
        return None

    # ----------------------------------------------------------------------
    # Step 3: IFM integration selection
    st.header("Étape 3 – Intégration des données IFM")
    info_source = st.radio(
        "Source des données IFM",
        options=["Aucune", "Excel", "Serveur SQL"],
        index=0,
        help="Choisissez la source pour les données de performance IFM.",
    )

    ifm_rows: List[Tuple[str, Optional[float], Optional[float]]] = []
    # Variables to keep track of column names for Excel
    if info_source == "Excel":
        st.subheader("Charger les données IFM depuis un fichier Excel")
        ifm_excel = st.file_uploader(
            "Fichier IFM (.xlsx ou .csv)",
            type=["xlsx", "csv"],
            accept_multiple_files=False,
        )
        if ifm_excel is not None:
            # Read DataFrame
            try:
                if ifm_excel.name.lower().endswith(".csv"):
                    df_ifm = pd.read_csv(ifm_excel)
                else:
                    df_ifm = pd.read_excel(ifm_excel)
            except Exception as e:
                st.error(f"Erreur lors de la lecture du fichier IFM : {e}")
                df_ifm = pd.DataFrame()
            if not df_ifm.empty:
                st.success(f"Données IFM chargées : {df_ifm.shape[0]} lignes et {df_ifm.shape[1]} colonnes.")
                # Schéma IFM fixe et unique
                required = [
                    "ID_PTF_CDCAM",
                    "TY_PORTEUR",
                    "PERIOD_CALCUL",
                    "ID_DEVISE_PERF",
                    "DT_PERF",
                    "CS_PERF_BRUTE",
                    "CS_PERF_BENCH",
                ]
                columns = set(df_ifm.columns.astype(str))
                missing = [c for c in required if c not in columns]
                if missing:
                    st.error(
                        "Le fichier IFM ne contient pas les colonnes requises : "
                        + ", ".join(missing)
                    )
                else:
                    st.success("Schéma IFM détecté (colonnes fixes et uniques).")
                    # Colonnes fixes
                    id_col = "ID_PTF_CDCAM"
                    type_col = "TY_PORTEUR"
                    period_col = "PERIOD_CALCUL"
                    devise_col = "ID_DEVISE_PERF"
                    pf_col = "CS_PERF_BRUTE"
                    bm_col = "CS_PERF_BENCH"
                    date_col = "DT_PERF"  # filtrée sur la date saisie dans l'app

                    # Construire les tuples (ID, PF_IFM, BM_IFM) pour chaque ligne du mapping
                    for _, m in mapping_df.iterrows():
                        pf_ifm, bm_ifm = get_ifm_values_from_excel(
                            df_ifm,
                            id_col=id_col,
                            type_col=type_col,
                            period_col=period_col,
                            devise_col=devise_col,
                            pf_col=pf_col,
                            bm_col=bm_col,
                            date_col=date_col,
                            id_ptf=str(m["ID"]),
                            type_part=str(m.get("TYPE_PART", "")),
                            devise=str(m["DEVISE"]),
                            period_ui=period,
                            asof=asof_date,
                        )
                        ifm_rows.append((str(m["ID"]), pf_ifm, bm_ifm))

    elif info_source == "Serveur SQL":
        # Paramètres de connexion prédéfinis pour Infomad
        SQL_DRIVER = "{Adaptive Server Enterprise}"
        SQL_SERVER = "xxx"
        SQL_PORT = 4600
        SQL_DATABASE = "APIDATA"
        SQL_TABLE = "INFOMAD.dbo.IFM_PERF_PTF"

        st.subheader("Connexion à la base IFM (paramètres prédéfinis)")
        st.caption(
            "Le serveur, le port, la base et la table sont fixés dans le code. Veuillez saisir uniquement l’utilisateur et le mot de passe pour établir la connexion."
        )
        user = st.text_input("Utilisateur SQL")
        password = st.text_input("Mot de passe SQL", type="password")
        if user and password:
            # Construit la chaîne ODBC automatiquement
            conn_str = (
                f"DRIVER={SQL_DRIVER};"
                f"SERVER={SQL_SERVER};"
                f"PORT={SQL_PORT};"
                f"DATABASE={SQL_DATABASE};"
                f"UID={user};"
                f"PWD={password};"
            )
            # Build full criteria from mapping and selected period (ID, TYPE, DEVISE, PERIOD)
            criteria = utils.build_ifm_criteria(mapping_df, period)
            if not criteria:
                st.warning("Aucun critère IFM construit depuis le mapping (ID/TYPE/DEVISE manquants).")
            else:
                # Fetch IFM with full alignment on (ID, TYPE, DEVISE, PERIOD) and DT_PERF
                df_ifm_sql = utils.fetch_ifm_data_bulk_criteria(
                    conn_str=conn_str,
                    asof=asof_date,
                    criteria=criteria,
                    table=SQL_TABLE,
                )
                if not df_ifm_sql.empty:
                    # Pack into (ID, PF_IFM, BM_IFM) tuples for merge
                    ifm_rows = utils.pack_ifm_rows_from_df(df_ifm_sql)
                    st.success(f"Données IFM (SQL) chargées : {len(ifm_rows)} identifiant(s) trouvé(s).")
                else:
                    st.warning("Aucune donnée IFM récupérée via la connexion SQL pour les critères fournis.")

    # ----------------------------------------------------------------------
    # Construct tables
    internal_df = utils.build_internal_table(mapping_df, results)
    attribution_df = utils.build_attribution_table(mapping_df, results)

    # --- Defensive normalization before merges ---
    # Ensure DataFrame keys are strings
    if "ID" in internal_df.columns:
        internal_df["ID"] = internal_df["ID"].astype(str)
    if "ID" in attribution_df.columns:
        attribution_df["ID"] = attribution_df["ID"].astype(str)
    # Ensure IFM tuples carry string IDs
    if isinstance(ifm_rows, list) and ifm_rows:
        ifm_rows = [(str(t[0]), t[1], t[2]) for t in ifm_rows]

    # st.write("internal_df['ID'] dtype:", internal_df["ID"].dtype)
    # st.write("First ifm_rows tuple:", ifm_rows[0] if ifm_rows else None)

    # Merge IFM values if provided
    if ifm_rows:
        internal_df = utils.merge_ifm_on_internal(internal_df, ifm_rows)
        attribution_df = utils.merge_ifm_on_attribution(attribution_df, ifm_rows)

    # Build flat, fixed-order display DataFrames
    internal_display = utils.to_internal_display(internal_df)
    attribution_display = utils.to_attribution_display(attribution_df)

    # ----------------------------------------------------------------------
    # Display analyses and export options
    st.header("Analyses")
    tab1, tab2 = st.tabs(["Analyse de performance interne", "Attribution PF vs BM"])

    def center_style(df: pd.DataFrame) -> pd.io.formats.style.Styler:
        return df.style.set_properties(**{"text-align": "center"}).set_table_styles(
            [{"selector": "th", "props": [("text-align", "center")]}]
        )

    with tab1:
        st.subheader("Analyse de performance interne")
        st.dataframe(center_style(internal_display), use_container_width=True, hide_index=True)
        bytes_internal, fname_internal = utils.generate_excel_download(
            internal_display, f"analyse_interne_{period}.xlsx"
        )
        st.download_button(
            "Exporter en Excel",
            data=bytes_internal,
            file_name=fname_internal,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    with tab2:
        st.subheader("Attribution PF vs BM")
        st.dataframe(center_style(attribution_display), use_container_width=True, hide_index=True)
        bytes_attrib, fname_attrib = utils.generate_excel_download(
            attribution_display, f"attribution_pf_vs_bm_{period}.xlsx"
        )
        st.download_button(
            "Exporter en Excel",
            data=bytes_attrib,
            file_name=fname_attrib,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )


if __name__ == "__main__":
    main()