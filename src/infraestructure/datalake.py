import pandas as pd
from pathlib import Path
import shutil
import datetime
import os

from data_logger import logging

logger = logging.getLogger(__name__)

VALID_DATA_LEVELS = {"bronze", "silver", "gold", "temp"}
VALID_EXTENSIONS = {"xlsx", "csv"}


def get_path_pattern(assunto: str, nivel: str) -> str:
    if nivel not in VALID_DATA_LEVELS:
        raise ValueError(
            f"Nível inválido: {nivel}. Deve ser um dos seguintes: {VALID_DATA_LEVELS}"
        )

    now = datetime.datetime.now()

    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")

    timestamp = now.strftime("%H_%M_%S_%f")

    full_path = (
        f"data/{nivel}/{assunto}/year={year}/month={month}/day={day}/{timestamp}_"
    )

    return full_path


def save_file(file, assunto: str, nivel: str) -> str:
    extensao = file.filename.split(".")[-1]

    if extensao not in VALID_EXTENSIONS:
        raise ValueError(
            f"Extensão inválida: {extensao}. Deve ser uma das seguintes: {VALID_EXTENSIONS}"
        )

    path_pattern = get_path_pattern(assunto, nivel)
    full_path = f"{path_pattern}{file.filename}"

    os.makedirs(os.path.dirname(full_path), exist_ok=True)

    with open(full_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    return full_path


def save_file_from_df(
    df: pd.DataFrame,
    assunto: str,
    nivel: str,
    extensao: str,
    file_name: str | None = None,
) -> str:
    if extensao not in VALID_EXTENSIONS:
        raise ValueError(
            f"Extensão inválida: {extensao}. Deve ser uma das seguintes: {VALID_EXTENSIONS}"
        )

    if not file_name:
        file_name = assunto

    path_pattern = get_path_pattern(assunto, nivel)

    full_path = f"{path_pattern}{file_name}.{extensao}"

    os.makedirs(os.path.dirname(full_path), exist_ok=True)

    if extensao == "xlsx":
        df.to_excel(full_path, index=False)
    elif extensao == "csv":
        df.to_csv(full_path, index=False)

    return full_path


def extract_data_from_path(file_path):
    """
    Função para extrair dados de um arquivo Excel (.xlsx) a partir do caminho do arquivo.
    Retorna um DataFrame ou lança uma exceção em caso de erro.
     - Verifica se o arquivo existe e não está vazio.
     - Verifica se o formato do arquivo é suportado (.xlsx).
     - Lê o arquivo Excel e retorna um DataFrame.
    """
    """Função para extrair dados de um arquivo Excel (.xlsx)"""

    logger.info(f"Extracting data from file: {file_path}")

    if not os.path.exists(file_path):
        raise ValueError(f"File does not exist: {file_path}")

    if os.path.getsize(file_path) == 0:
        raise ValueError(f"File is empty: {file_path}")

    if not file_path.endswith(".xlsx"):
        raise ValueError("Unsupported file format. Only .xlsx are supported.")

    logger.info("Reading Excel file")
    df = pd.read_excel(file_path)

    if df.empty:
        raise ValueError(f"Dataframe is empty: {file_path}")

    logger.info("Data extracted successfully")
    return df


def get_latest_template_file_path():
    """
    Função para obter o caminho do arquivo de template mais recente na pasta gold/templates.
    Retorna o caminho do arquivo ou None se nenhum arquivo for encontrado.
    """
    folder = Path("data/templates")

    files = list(folder.glob("*"))

    if not files:
        return None

    latest_file = max(files, key=lambda f: f.stat().st_mtime)

    return latest_file


def extract_files_from_upload_date(
    upload_date: pd.Timestamp, folder_path="data/bronze"
) -> list[pd.DataFrame] | None:
    """
    Função para extrair dados de arquivos Excel (.xlsx) na pasta bronze com base na data de upload.
    Retorna uma lista de DataFrames ou None se nenhum arquivo for encontrado.
    """
    folder = Path(folder_path)

    files = list(folder.glob("*"))
    if not files:
        return None

    if upload_date is None:
        upload_date = pd.Timestamp(datetime.datetime.min)
    filtered_files = [f for f in files if f.stat().st_mtime >= upload_date.timestamp()]
    if not filtered_files:
        return None

    dfs = []
    for file in filtered_files:
        try:
            file_path = str(file)
            df = extract_data_from_path(file_path)
            dfs.append(df)
        except Exception as e:
            print(f"Error processing file {file}: {e}")

    return dfs
