import pandas as pd
from pathlib import Path
import os

def extract_data_from_path(file_path):
    """Função para extrair dados de um arquivo Excel (.xlsx)"""
    if not os.path.exists(file_path):
        raise ValueError(f"File does not exist: {file_path}")

    if os.path.getsize(file_path) == 0:
        raise ValueError(f"File is empty: {file_path}")

    if not file_path.endswith('.xlsx'):
        raise ValueError("Unsupported file format. Only .xlsx are supported.")
    
    df = pd.read_excel(file_path)

    if df.empty:
        raise ValueError(f"Dataframe is empty: {file_path}")
    
    return df

def get_latest_template_file_path():
    folder = Path("data/gold/templates")

    files = list(folder.glob("*"))

    if not files:
        return None

    latest_file = max(files, key=lambda f: f.stat().st_mtime)

    return latest_file