from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

current_dir = Path(__file__).resolve().parent

# Load the data from a .dta file into a DataFrame
df = pd.read_stata(current_dir / "data/_raw/scf_merged.dta")

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

df_new = pd.DataFrame()

# Apply all mappings and transformations directly on the original DataFrame
for column, mapping in mappings.items():
    df_new[f"{column}_lbl"] = df[column].map(mapping)

# Define labels for age groups
labels = [f"({i+1}-{i+5}]" for i in range(20, 95, 5)]

# Create a new column 'age_lbl' with age groups
df_new["age_lbl"] = pd.cut(df["age"], bins=range(20, 100, 5), right=True, labels=labels)

# Calculate 'equitfin', 'fininc', 'finmill', 'fillthou', 'incomemill', 'incomethou' in one line
df_new = df_new.assign(
    equityfin=df["equity"]
    .div(df["fin"], fill_value=np.nan)
    .replace([np.inf, -np.inf], np.nan),
    finincome=df["fin"]
    .div(df["income"], fill_value=np.nan)
    .replace([np.inf, -np.inf], np.nan),
    finmill=df["fin"].div(1_000_000, fill_value=np.nan),
    fillthou=df["fin"].div(1_000, fill_value=np.nan),
    incomemill=df["income"].div(1_000_000, fill_value=np.nan),
    incomethou=df["income"].div(1_000, fill_value=np.nan),
)

df_new["findeciles"] = (
    df.groupby("year")["fin"].transform(
        lambda x: pd.qcut(x, q=10, duplicates="drop", labels=False),
    )
    / 10
)


df_new.replace([np.inf, -np.inf], np.nan, inplace=True)

df = pd.concat([df, df_new], axis=1)

df = df.filter(
    regex="age|race|hhsex|edcl|married|lf|fin|inc|equity|networth|asset|year|wgt|savres",
)

# Save the processed DataFrame back to a .dta file
df.to_stata(current_dir / "data/scf_processed.dta", write_index=False)
