import pandas as pd
import unicodedata

def transform_data(df, schema):
    df_normalizado = df.copy()
    normalize_columns(df_normalizado)
    fix_data_types(df_normalizado, schema)
    clean_missing_values(df_normalizado)
    df_normalizado = df_normalizado.dropna(how="all")
    normalize_data(df_normalizado)
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
        .replace(["", "nan", "None", "null", "NULL", "nat", "NaT"], pd.NA)
        )

        # remover acentos
        df[col] = df[col].apply(
            lambda x: unicodedata.normalize("NFKD", x)
            .encode("ascii", "ignore")
            .decode("utf-8") if pd.notna(x) else x
        )

    df = df.drop_duplicates()

    return df


def fix_data_types(df, schema: dict):
    """Corrige tipos via schema"""

    for col, dtype in schema.items():

        if col not in df.columns:
            continue

        if dtype == "Int64":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

        elif dtype == "float":
            df[col] = pd.to_numeric(df[col], errors="coerce")

        elif dtype == "datetime64[ns]":
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

def union_dataframes(dfs, filter_columns=None):
    """Função para unir uma lista de DataFrames, alinhando colunas e tipos"""

    if not dfs:
        return pd.DataFrame()

    # Alinha colunas
    all_columns = set().union(*[set(df.columns) for df in dfs])
    all_columns = [col for col in all_columns if not filter_columns or col in filter_columns]
    aligned_dfs = []
    for df in dfs:
        missing_cols = set(all_columns) - set(df.columns)
        for col in missing_cols:
            df[col] = pd.NA
        aligned_dfs.append(df[sorted(all_columns)])

    # Concatena DataFrames alinhados
    combined_df = pd.concat(aligned_dfs, ignore_index=True)

    # Distinct
    combined_df = combined_df.drop_duplicates()

    return combined_df

def verify_data_quality(df, schema):
    """Função para verificar qualidade dos dados"""

    not_null_columns = schema.get("not_null", [])
    unique_columns = schema.get("unique", [])
    expected_schema = schema.get("types", {})

    if not df.empty:
        if not_null_columns:
            verify_not_null_columns(df, not_null_columns)
        if unique_columns:
            verify_unique_columns(df, unique_columns)
        if expected_schema:
            verify_data_types(df, expected_schema)
    return df

def verify_not_null_columns(df, not_null_columns):
    """Verifica se colunas obrigatórias não possuem valores nulos"""

    missing_values = df[not_null_columns].isnull().sum()
    if missing_values.any():
        raise ValueError(f"Colunas obrigatórias com valores nulos: {missing_values[missing_values > 0]}")

def verify_unique_columns(df, unique_columns):
    """Verifica se a combinação de colunas únicas possui duplicatas"""

    if df.duplicated(subset=unique_columns).any():
        duplicated_rows = df[df.duplicated(subset=unique_columns, keep=False)]
        raise ValueError(
            f"Existem registros duplicados para as colunas {unique_columns}:\n{duplicated_rows}"
        )
        
def verify_data_types(df, expected_schema):
    """Verifica se os tipos de dados das colunas estão corretos"""

    for col, expected_type in expected_schema.items():
        if col in df.columns:
            actual_type = df[col].dtype
            if actual_type != expected_type:
                raise ValueError(f"Coluna '{col}' tem tipo '{actual_type}' mas esperava '{expected_type}'")
            
def fix_null_values_mysql(df):
    """Função para converter valores nulos em None, para compatibilidade com MySQL"""
    import numpy as np
    df = df.replace({np.nan: None})

    df = df.where(pd.notnull(df), None)

    for col in df.select_dtypes(include=['datetime64[ns]']).columns:
        df[col] = df[col].apply(lambda x: x.to_pydatetime() if pd.notna(x) else None)
    
    for col in df.select_dtypes(include=['timedelta64[ns]']).columns:
        df[col] = df[col].apply(
            lambda x: str(x).split(" ")[-1] if x is not None else None
        )

        
    return df