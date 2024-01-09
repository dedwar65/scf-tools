from __future__ import annotations

import zipfile
from pathlib import Path

import requests

url = "https://www.federalreserve.gov/econres/files/scfp"
years = [str(i) for i in range(1989, 2023, 3)]
endings = {"sas": ".zip", "stata": "s.zip", "csv": "excel.zip"}


def download_data(type="stata"):
    ending = endings.get(type)

    if not ending:
        raise ValueError("Invalid type. Expected 'csv', 'sas' or 'stata'")

    # Get the directory of the current file
    current_dir = Path(__file__).resolve().parent
    data_dir = current_dir / "data"
    raw_dir = current_dir / "data/_raw"
    source_dir = current_dir / "data/_source"
    data_dir.mkdir(exist_ok=True)
    raw_dir.mkdir(exist_ok=True)
    source_dir.mkdir(exist_ok=True)

    with requests.Session() as session:
        for year in years:
            file_url = f"{url}{year}{ending}"

            # Define the local file path relative to the current file
            local_file = source_dir / f"scfp{year}{ending}"

            # Skip download if file already exists
            if local_file.exists():
                print(f"File {local_file.name} already exists. Skipping download.")
                continue

            print(f"Starting download of {local_file.name}...")

            try:
                response = session.get(file_url)

                with open(local_file, "wb") as f:
                    f.write(response.content)

                print(f"File {local_file.name} has been downloaded successfully.")

            except requests.exceptions.RequestException as e:
                print(f"An error occurred while downloading {local_file.name}: {e}")
                continue

            # Extract the zip file
            try:
                with zipfile.ZipFile(local_file, "r") as zip_ref:
                    zip_ref.extractall(raw_dir)

                print(
                    f"File {local_file.name} has been extracted to the raw directory."
                )

            except zipfile.BadZipFile as e:
                print(f"An error occurred while extracting {local_file.name}: {e}")


if __name__ == "__main__":
    download_data()
