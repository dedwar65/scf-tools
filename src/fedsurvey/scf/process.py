from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

current_dir = Path(__file__).resolve().parent

# Load the data from a .dta file into a DataFrame
scf_merged = pd.read_stata(current_dir / "data/_raw/scf_merged.dta")

# Define the mapping dictionaries
mappings = {
    "hhsex": {0: "inap.", 1: "male", 2: "female"},
    "edcl": {
        1: "no high school diploma/GED",
        2: "high school diploma or GED",
        3: "some college or Assoc. degree",
        4: "Bachelors degree or higher",
    },
    "married": {
        1: "married/living with partner",
        2: "neither married nor living with partner",
    },
    "lf": {0: "working in some way", 1: "not working at all"},
    "racecl": {1: "white non-Hispanic", 2: "nonwhite or Hispanic"},
    "racecl4": {
        1: "white non-Hispanic",
        2: "black/African-American non-Hispanic",
        3: "Hispanic or Latino",
        4: "Other or Multiple race",
    },
    "racecl5": {
        1: "white non-Hispanic",
        2: "black/African-American non-Hispanic",
        3: "Hispanic or Latino",
        4: "Asian",
        5: "Other or Multiple race",
    },
    "race": {
        1: "white non-Hispanic",
        2: "black/African-American",
        3: "Hispanic",
        4: "Asian",
        5: "other",
    },
}

# Create labels for age groups
labels = [f"({i+1}-{i+5}]" for i in range(20, 95, 5)]

# Create a dictionary of all transformations
transformations = {
    # Label mappings
    **{f"{col}_lbl": scf_merged[col].map(mapping) for col, mapping in mappings.items()},
    # Age groups
    "age_lbl": pd.cut(scf_merged["age"], bins=range(20, 100, 5), right=True, labels=labels),
    # Financial calculations
    "equityfin": scf_merged["equity"].div(scf_merged["fin"]).replace([np.inf, -np.inf], np.nan),
    "finincome": scf_merged["fin"].div(scf_merged["income"]).replace([np.inf, -np.inf], np.nan),
    "finmill": scf_merged["fin"] / 1_000_000,
    "fillthou": scf_merged["fin"] / 1_000,
    "incomemill": scf_merged["income"] / 1_000_000,
    "incomethou": scf_merged["income"] / 1_000,
    # Financial deciles
    "findeciles": scf_merged.groupby("year")["fin"].transform(
        lambda x: pd.qcut(x, q=10, duplicates="drop", labels=False)
    ) / 10
}

# Create labeled DataFrame in one go
scf_merged_labeled = pd.DataFrame(transformations)

# Create two separate filtered DataFrames
scf_merged_full = pd.concat([scf_merged, scf_merged_labeled], axis=1)
scf_merged_full = scf_merged_full.filter(
    regex="age|race|hhsex|edcl|married|lf|fin|inc|equity|networth|asset|year|wgt|savres",
)

# Create a minimal version with only the specified columns
scf_merged_minimal = scf_merged.filter(regex="age|edcl|fin|inc|networth|asset|year|wgt")

# Save both processed DataFrames to .dta files
scf_merged_full.to_stata(current_dir / "data/scf_processed.dta", write_index=False)
scf_merged_minimal.to_stata(current_dir / "data/scf_processed_dc.dta", write_index=False)
