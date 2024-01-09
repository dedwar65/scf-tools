from __future__ import annotations

from pathlib import Path

import pandas as pd

from_end = {"sas": "", "stata": ".dta", "csv": ".csv"}
to_end = {"sas": ".sas7bdat", "stata": ".dta", "csv": ".csv", "pickle": ".pkl"}

# Map file formats to their corresponding pandas read functions
read_funcs = {"stata": pd.read_stata, "csv": pd.read_csv, "sas": pd.read_sas}

# Map file formats to their corresponding pandas write functions
write_funcs = {
    "stata": pd.DataFrame.to_stata,
    "csv": pd.DataFrame.to_csv,
    "pickle": pd.DataFrame.to_pickle,
}


def merge_files(from_format="stata", to_format="stata"):
    # Get the directory of the current file
    current_dir = Path(__file__).resolve().parent
    raw_dir = current_dir / "data/_raw"

    # Use Path.glob to find all files that start with 4 digits and end with the specified format
    data_files = sorted(raw_dir.glob(f"*[0-9][0-9][0-9][0-9]{from_end[from_format]}"))

    if not data_files:
        print(f"No files found in {raw_dir.name} with format {from_format}")
        return

    print(f"Found {len(data_files)} files in {raw_dir.name} with format {from_format}")

    read_func = read_funcs.get(from_format)
    if not read_func:
        print(f"Unsupported input format: {from_format}")
        return

    # Use list comprehension to create df_list and concatenate all dataframes in the list
    dfs = []
    for data_file in data_files:
        try:
            df = read_func(str(data_file)).assign(year=data_file.stem[-4:])
        except ValueError:
            df = read_func(str(data_file), convert_categoricals=False).assign(
                year=data_file.stem[-4:]
            )
        except Exception as e:
            print(f"Error reading file {data_file}: {e}")
            continue
        dfs.append(df)

    df = pd.concat(dfs, ignore_index=True)

    print(f"Successfully read {len(dfs)} files")

    # Save the merged dataframe in the specified format
    output_file = current_dir / f"data/_raw/scf_merged{to_end[to_format]}"
    write_func = write_funcs.get(to_format)
    if write_func:
        try:
            if to_format == "csv":
                write_func(df, str(output_file), index=False)
            else:
                write_func(df, str(output_file))
        except Exception as e:
            print(f"Error writing file {output_file.name}: {e}")
            return
    else:
        print(f"Unsupported output format: {to_format}")
        return

    print(f"Merged data saved to {output_file.name}")


if __name__ == "__main__":
    merge_files()
