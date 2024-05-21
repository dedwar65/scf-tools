from __future__ import annotations

import logging
import zipfile
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO)

SCF_DATA_URL = "https://www.federalreserve.gov/econres/files/"
FIRST_YEAR = 1989
LAST_YEAR = 2022
INTERVAL = 3
YEARS_IN_SCF = set(range(FIRST_YEAR, LAST_YEAR + 1, INTERVAL))
FILE_TYPES = {"sas": ".zip", "stata": "s.zip", "csv": "excel.zip"}

PKG_DIR = Path(__file__).resolve().parent
DATA_DIR = PKG_DIR / "data"
UNZIP_DIR = "_raw"
ARCHIVE_DIR = "_zip"
(DATA_DIR).mkdir(exist_ok=True)
(DATA_DIR / UNZIP_DIR).mkdir(exist_ok=True)
(DATA_DIR / ARCHIVE_DIR).mkdir(exist_ok=True)


def check_year_file_type(year: int, file_type: str) -> bool:
    if file_type not in FILE_TYPES:
        msg = (
            f"Invalid file type {file_type}. ",
            "Expected 'sas', 'stata' or 'csv'.",
        )
        raise ValueError(msg)

    if year not in YEARS_IN_SCF:
        msg = (
            f"Invalid year {year}. Expected a year in the range "
            f"{FIRST_YEAR}-{LAST_YEAR} in intervals of {INTERVAL}."
        )
        raise ValueError(msg)


def save_year_zip(
    year: int,
    file_type: str = "stata",
    save_dir: str | None = None,
    session=None,
):
    file_name = f"scfp{year}{FILE_TYPES[file_type]}"
    file_url = f"{SCF_DATA_URL}{file_name}"
    zip_path = save_dir / ARCHIVE_DIR / file_name

    # Check if the file already exists
    if zip_path.exists():
        logging.info(f"File {file_name} already exists. Skipping download.")

    else:
        try:
            response = session.get(file_url)
            response.raise_for_status()
            logging.info(f"File {file_name} has been downloaded successfully.")
        except requests.exceptions.RequestException as err:
            raise SystemExit(f"Failed to download scfp{file_name}: {err}")

        with zip_path.open("wb") as f:
            f.write(response.content)
            logging.info(f"File {file_name} has been saved to the zip directory.")

    return file_name


def unzip_file(file_name, file_dir=None, save_dir=None):
    local_file = file_dir / ARCHIVE_DIR / file_name
    unzip_dir = save_dir / UNZIP_DIR
    zip_file = unzip_dir / file_name

    if zip_file.exists():
        logging.info(f"File {file_name} already exists. Skipping extraction.")
        return

    try:
        with zipfile.ZipFile(local_file, "r") as zip_ref:
            zip_ref.extractall(unzip_dir)

        logging.info(f"File {file_name} has been extracted to the raw directory.")

    except zipfile.BadZipFile as e:
        logging.info(f"An error occurred while extracting {file_name}: {e}")


def make_dirs(save_dir):
    save_dir.mkdir(exist_ok=True)
    (save_dir / UNZIP_DIR).mkdir(exist_ok=True)
    (save_dir / ARCHIVE_DIR).mkdir(exist_ok=True)


def download_year(
    year: int,
    file_type: str = "stata",
    save_dir: str | None = None,
    session=None,
):
    save_dir = DATA_DIR if save_dir is None else Path(save_dir).resolve()
    make_dirs(save_dir)

    check_year_file_type(year, file_type)
    file_name = save_year_zip(year, file_type, session=session, save_dir=save_dir)
    unzip_file(file_name, file_dir=save_dir, save_dir=save_dir)


def download_all_years(file_type: str = "stata", save_dir: str | None = None):
    save_dir = DATA_DIR if save_dir is None else Path(save_dir).resolve()
    make_dirs(save_dir)

    with requests.Session() as session:
        for year in YEARS_IN_SCF:
            download_year(year, file_type, session=session, save_dir=save_dir)


if __name__ == "__main__":
    download_all_years()
