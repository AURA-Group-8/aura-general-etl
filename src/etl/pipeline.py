from src.databases import create_execution_log_table, insert_execution_log, get_last_execution_time, create_costs_table, clean_execution_log, bulk_upsert_warehouse_db
from .extract import extract_files_from_upload_date
from .transform import transform_data, union_dataframes, verify_data_quality, fix_null_values_mysql
from .load import save_silver_file
from src.config import schema, schema_config

import pandas as pd

from data_logger import logging
logger = logging.getLogger(__name__)

def custos_pipeline():
    logger.info("Running cost analysis pipeline")
    
    clean_execution_log("analise_custos_pipeline")

    create_execution_log_table()
    last_execution_time = get_last_execution_time("analise_custos_pipeline")
    insert_execution_log("analise_custos_pipeline")

    dfs = extract_files_from_upload_date(upload_date=last_execution_time, folder_path="data/bronze/costs")
    dfs_transformados = []
    if not dfs:
        logger.info("No new files to process")
        logger.info("Cost analysis pipeline completed")
        return
    else:
        for df in dfs:
            logger.info(f"Processing file with {len(df)} rows")
            logger.info("Data Before Transformation Preview:")
            print(df.head(1))
            transformed_df = transform_data(df, schema)
            logger.info("Data After Transformation Preview:")
            print(transformed_df.head(1))
            dfs_transformados.append(transformed_df)

    df_unified = union_dataframes(dfs_transformados, filter_columns=schema.keys())
    df_unified = transform_data(df_unified, schema)
    df_unified = verify_data_quality(df_unified, schema_config)
    logger.info(f"Unified DataFrame has {len(df_unified)} rows and {len(df_unified.columns)} columns")
    logger.info("Unified DataFrame Preview:")
    print(df_unified.head())

    save_silver_file(df=df_unified, table="costs")
    create_costs_table()
    df_unified = fix_null_values_mysql(df_unified)
    df_unified['loaddate'] = pd.Timestamp.now()
    bulk_upsert_warehouse_db(df_unified, table_name="costs", primary_keys=schema_config["primary_keys"])

    logger.info("Cost analysis pipeline completed")

def dados_aplicacao_pipeline():
    logger.info("Running application data pipeline")
    # 1. Controle de execução (igual ao pipeline de custos)
    # Aqui a ideia é garantir que a gente só processe dados novos
    # - Limpa logs antigos desse pipeline (usar só pra debug)
    # - Cria a tabela de log se não existir
    # - Busca a última vez que esse pipeline rodou filtrando por nome (ex: "dados_aplicacao_pipeline")
    # - Registra que estamos rodando agora
    # Isso tudo serve pra usar a data da última execução como filtro na extração
    # Obs: se não existir execução anterior, considerar pegar tudo (last_execution_time = None)

    # 2. Extração dos dados do banco (principal diferença desse pipeline)
    # Em vez de ler arquivos CSV, aqui você vai:
    # - Conectar no banco do backend usando SQLAlchemy (usar engine do database.py)
    # - Fazer SELECT nas tabelas necessárias com os campos definidos na documentação
    # - Preferir fazer JOIN direto no SQL (mais performático que juntar depois no pandas)
    # - Filtrar os dados usando a última data de execução (ex: updated_at > last_execution_time)
    # - Converter o resultado em DataFrame do pandas
    # - Retornar uma lista de DataFrames (mesmo que seja só um)

    # 3. Definição de schema
    # Antes de transformar, você precisa definir o schema no config:
    # - Nome das colunas (já no padrão final esperado)
    # - Tipo de cada coluna
    # - Quais são obrigatórias (not_null)
    # - Quais são chaves primárias (primary_keys)
    # Isso vai ser usado nas funções de transformação e validação

    # 4. Transformação dos dados
    # Para cada DataFrame extraído:
    # - Aplicar a função transform_data passando o schema da tabela correspondente

    # 5. Unificação dos dados (wide table)
    # Se você tiver dados de mais de uma tabela:
    # - Juntar tudo em um único DataFrame usando union_dataframes
    # - Manter só as colunas definidas no schema (filter_columns)
    # Se for só uma tabela, pode pular essa etapa

    # 6. Validação de qualidade
    # Rodar verify_data_quality:
    # - Garante que colunas obrigatórias estão preenchidas
    # - Valida unicidade com base nas chaves
    # - Garante que os tipos estão corretos
    # Se falhar, o pipeline deve parar (evita subir dado quebrado)

    # 7. Salvar camada silver
    # Salvar o DataFrame tratado como arquivo (parquet/csv)
    # Usar save_silver_file(df, "dados_aplicacao")
    # Isso serve como uma camada intermediária já limpa e organizada (facilita debug e reprocessamento)

    # 8. Criar tabela no banco analítico
    # Criar a tabela de destino no warehouse (caso não exista)
    # Estrutura deve seguir o schema definido (mesmos nomes e tipos principais)
    # Criar função similar a create_costs_table()

    # 9. Ajustes finais antes de subir
    # - Tratar valores nulos (com a função fix_null_values_mysql)
    # - Converter tipos de data se necessário (MySQL não entende datetime do pandas)
    # - Adicionar coluna 'loaddate' com a data atual (pd.Timestamp.now())

    # 10. Upsert no banco analítico
    # Inserir os dados na tabela final:
    # - Se já existir (mesma primary key), faz UPDATE
    # - Se não existir, faz INSERT
    # - Usar bulk_upsert_warehouse_db(df, table_name, primary_keys)
    # Isso evita duplicação e mantém os dados atualizados

    logger.info("Application data pipeline completed")