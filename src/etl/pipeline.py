from src.databases import create_execution_log_table, insert_execution_log, get_last_execution_time, create_costs_table, clean_execution_log, bulk_upsert_warehouse_db, read_from_main_db, create_dados_aplicacao_table, overwrite_warehouse_db
from .extract import extract_files_from_upload_date
from .transform import transform_data, union_dataframes, verify_data_quality, fix_null_values_mysql
from .load import save_silver_file
from src.config import schema, schema_config, main_db_schema, main_db_schema_config

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
    clean_execution_log("dados_aplicacao_pipeline")
    create_execution_log_table() 
    last_execution_time = get_last_execution_time("dados_aplicacao_pipeline") 
    insert_execution_log("dados_aplicacao_pipeline") 
    query = """
        select 
            schedule.id as schedule_id,
            users.id as user_id,
            users.username,
            users.role_id,
            job.id as job_id,
            job.name,
            job.description,
            schedule.start_datetime,
            schedule.end_datetime,
            schedule.status,
            schedule.canceled,
            schedule_setting.days_of_week,
            schedule_setting.work_Start,
            schedule_setting.work_End,
            schedule_setting.break_Start,
            schedule_setting.break_End,
            js.current_price,
            schedule.total_price,
            (schedule.end_datetime - schedule.start_datetime) AS duracao
        from job_schedule js 
        right join job 
            on job.id = js.job_id
        right join schedule 
            on schedule.id = js.schedule_id
        right join users
            on users.id = schedule.users_id
        cross join schedule_setting
    """
    df = read_from_main_db(query)
    df = pd.DataFrame(df, columns=main_db_schema.keys())
    df = transform_data(df, main_db_schema)
    df = verify_data_quality(df, main_db_schema_config)
    save_silver_file(df=df, table="dados_aplicacao")
    create_dados_aplicacao_table()

    df = fix_null_values_mysql(df)
    df['loaddate'] = pd.Timestamp.now()

    overwrite_warehouse_db(df, table_name="dados_aplicacao")

    logger.info("Application data pipeline completed")
