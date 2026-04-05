from src.databases import create_execution_log_table, insert_execution_log, get_last_execution_time, create_costs_table, clean_execution_log, bulk_upsert_warehouse_db
from .extract import extract_files_from_upload_date
from .transform import transform_data, union_dataframes, verify_data_quality, fix_null_values_mysql
from .load import save_silver_file
from src.config import schema, schema_config

import pandas as pd

from data_logger import logging
logger = logging.getLogger(__name__)

def analise_custos_pipeline():
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
            transformed_df = transform_data(df)
            logger.info("Data After Transformation Preview:")
            print(transformed_df.head(1))
            dfs_transformados.append(transformed_df)

    df_unified = union_dataframes(dfs_transformados, filter_columns=schema.keys())
    df_unified = transform_data(df_unified)
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