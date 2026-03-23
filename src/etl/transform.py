
def transform_data(df):
    df_normalizado = df.copy()
    normalize_columns(df_normalizado)
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
    """Função para normalizar os dados de um DataFrame"""


def fix_data_types(df):
    """Função para corrigir os tipos de dados de um DataFrame"""
    