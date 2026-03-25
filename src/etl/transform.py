import pandas as pd
import unicodedata

from src.config import schema

def transform_data(df):
    df_normalizado = df.copy()
    normalize_columns(df_normalizado)
    normalize_data(df_normalizado)
    fix_data_types(df_normalizado, schema)
    clean_missing_values(df_normalizado)
    return df_normalizado

def normalize_columns(df):
    """Função para normalizar os nomes das colunas de um DataFrame"""

    df.columns = (
        df.columns
            .str.replace(r'[ªº]', '', regex=True)     # remove caracteres de ordinal
            .str.replace(r'[^\w\s]', '', regex=True)  # remove caracteres especiais
            .str.replace(r'[\n\r\t]', ' ', regex=True) # remove quebras de linha
            
            .str.normalize('NFKD')                    # 
            .str.encode('ascii', errors='ignore')     # remove acentos
            .str.decode('utf-8')                      #

            .str.replace(r'\s+', ' ', regex=True)     # remove múltiplos espaços
            .str.strip()                              # remove espaço bordas
            
            .str.lower()                              # minúsculo
            .str.replace(' ', '_')                    # espaço vira underscore
    )

    return df

def normalize_data(df):
    """Normaliza dados (colunas + conteúdo)"""

    for col in df.select_dtypes(include="object").columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.strip()
            .replace(["", "nan", "None", "null", "NULL"], pd.NA)
        )

        # remover acentos
        df[col] = df[col].apply(
            lambda x: unicodedata.normalize("NFKD", x)
            .encode("ascii", "ignore")
            .decode("utf-8") if pd.notna(x) else x
        )

    df = df.drop_duplicates()

    return df


def fix_data_types(df, schema: dict = None):
    """Corrige tipos automaticamente ou via schema"""

    for col, dtype in schema.items():

        if col not in df.columns:
            continue

        if dtype == "int":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

        elif dtype == "float":
            df[col] = pd.to_numeric(df[col], errors="coerce")

        elif dtype == "datetime":
            df[col] = pd.to_datetime(df[col], errors="coerce")

        elif dtype == "string":
            df[col] = df[col].astype("string")

        elif dtype == "boolean":
            df[col] = df[col].map(
                {"true": True, "false": False, "1": True, "0": False}
            ).astype("boolean")

    return df


def clean_missing_values(df):
    """Tratamento de nulos"""

    df = df.dropna(how="all")
    df = df.replace(r"^\s*$", pd.NA, regex=True)

    return df
    