from src.infraestructure.datalake import (
    get_latest_template_file_path,
    extract_data_from_path,
    save_file,
    save_file_from_df,
)
from src.api.utils.utils import dataframe_to_llm_input
from src.config import schema_config, GEMINI_API_KEY
from google import genai
from google.genai import types
from src.api.utils.models import MappingResponse

import pandas as pd

from data_logger import logging

logger = logging.getLogger(__name__)


def get_latest_template_file_path_service():
    path = get_latest_template_file_path()
    return path


def validate_file(file):

    # Salva o arquivo na pasta temp para ser processado
    full_path = save_file(file=file, nivel="temp", assunto="costs")

    # Transformar o arquivo em um dataframe se for válido
    df = extract_data_from_path(full_path)

    return df


def validate_schema_with_llm(df):
    columns = dataframe_to_llm_input(df)
    schema = schema_config

    prompt = f"""
    Você é um especialista em estruturação e validação de schemas de arquivos Excel.

    Sua tarefa é analisar as colunas recebidas e fazer um DE/PARA para o schema esperado.

    REGRAS IMPORTANTES:
    - Responda SOMENTE JSON válido
    - NÃO use markdown
    - NÃO escreva explicações
    - NÃO escreva texto antes ou depois do JSON
    - Se não encontrar correspondência para uma coluna, use: "target_field": null
    - Se uma coluna obrigatória não for encontrada, adicione em "missing_required"
    - Considere nomes das colunas, exemplos de valores e tipos dos dados
    - O mapeamento deve escolher o campo MAIS compatível semanticamente

    SCHEMA ESPERADO:
    {schema}

    COLUNAS RECEBIDAS:
    {columns}

    FORMATO DE RESPOSTA OBRIGATÓRIO:

    {{
    "mappings": [
        {{
        "source_column": "Dt Compra",
        "target_field": "data_compra"
        }},
        {{
        "source_column": "Fornecedor Nome",
        "target_field": "fornecedor"
        }},
        {{
        "source_column": "Observacao Produto",
        "target_field": "observacoes"
        }},
        {{
        "source_column": "Coluna Desconhecida",
        "target_field": null
        }}
    ],

    "missing_required": [
        "quantidade_comprada",
        "valor_total"
    ]
    }}

    REGRAS PARA missing_required:
    - Adicione apenas campos que estão em schema_config["not_null"]
    - Um campo obrigatório é considerado encontrado apenas se existir algum mapping compatível
    - NÃO invente colunas
    """

    client = genai.Client(api_key=GEMINI_API_KEY)

    response = client.models.generate_content(
        model="gemini-3.1-flash-lite-preview",
        contents=prompt,
        config=types.GenerateContentConfig(
            temperature=0,
            response_mime_type="application/json",
            response_schema=MappingResponse,
        ),
    )

    resultado: MappingResponse = response.parsed
    if resultado is None:
        raise ValueError(
            "Não foi possível processar o arquivo. O modelo não retornou um mapeamento válido."
        )

    if resultado.missing_required:
        raise ValueError(
            "Campos obrigatórios ausentes: " + ", ".join(resultado.missing_required)
        )

    mapping_dict = {
        mapping.source_column: mapping.target_field
        for mapping in resultado.mappings
        if mapping.target_field
    }

    df = df.rename(columns=mapping_dict)

    return df


def save_bronze_xlsx_from_df(df: pd.DataFrame, file_name="arquivo.xlsx"):
    try:
        save_file_from_df(
            df=df,
            file_name=file_name,
            nivel="bronze",
            assunto="costs",
            extensao="xlsx",
        )
        return "Arquivo salvo com sucesso"
    except Exception as e:
        logger.error("Erro ao salvar arquivo bronze:", e)
        return ValueError("Erro ao salvar arquivo")
