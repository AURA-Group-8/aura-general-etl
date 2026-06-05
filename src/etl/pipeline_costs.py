from src.infraestructure.databases import bulk_upsert_warehouse_db
from src.infraestructure.execution_registry import (
    insert_execution_log,
    get_last_execution_time,
)
from src.infraestructure.datalake import (
    extract_files_from_upload_date,
    save_file_from_df,
)
from .transform import (
    transform_data,
    union_dataframes,
    verify_data_quality,
    fix_null_values_mysql,
)
from src.config import schema, schema_config

import pandas as pd

from data_logger import logging

logger = logging.getLogger(__name__)


def custos_pipeline():
    logger.info("Running cost analysis pipeline")

    # clean_execution_log("analise_custos_pipeline")
    last_execution_time = get_last_execution_time("analise_custos_pipeline")
    insert_execution_log("analise_custos_pipeline")

    dfs = extract_files_from_upload_date(
        upload_date=last_execution_time, folder_path="data/bronze/costs"
    )
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
    logger.info(
        f"Unified DataFrame has {len(df_unified)} rows and {len(df_unified.columns)} columns"
    )
    logger.info("Unified DataFrame Preview:")
    print(df_unified.head())

    save_file_from_df(df=df_unified, assunto="costs", nivel="silver", extensao="csv")

    df_unified = fix_null_values_mysql(df_unified)
    df_unified["loaddate"] = pd.Timestamp.now()
    bulk_upsert_warehouse_db(
        df_unified, table_name="costs", primary_keys=schema_config["primary_keys"]
    )

    logger.info("Cost analysis pipeline completed")
