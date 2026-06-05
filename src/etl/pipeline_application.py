from src.infraestructure.databases import (
    bulk_upsert_warehouse_db,
    execute_application_db_sql,
)
from src.infraestructure.execution_registry import (
    insert_execution_log,
    get_last_execution_time,
)
from src.infraestructure.datalake import save_file_from_df
from .transform import (
    transform_data,
    verify_data_quality,
    fix_null_values_mysql,
)
from src.config import application_db_schema, application_db_schema_config

import pandas as pd

from data_logger import logging

logger = logging.getLogger(__name__)


def dados_aplicacao_pipeline():
    logger.info("Running application data pipeline")
    # logger.debug("Cleaning execution log for application data pipeline")
    # clean_execution_log("dados_aplicacao_pipeline")

    last_execution_time = get_last_execution_time("dados_aplicacao_pipeline")
    last_execution_time_str = (
        last_execution_time.strftime("%Y-%m-%d %H:%M:%S")
        if last_execution_time
        else "1970-01-01 00:00:00"
    )
    logger.info(
        f"Last execution time for application data pipeline: {last_execution_time_str}"
    )
    insert_execution_log("dados_aplicacao_pipeline")
    query = f"""
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
        where 1=1
            and (
                job.modified_at > '{last_execution_time_str}'
                or schedule.modified_at > '{last_execution_time_str}'
                or users.modified_at > '{last_execution_time_str}'
            )
    """
    logger.info("Extracting data from application database")
    df = execute_application_db_sql(query)
    logger.info(f"Retrieved {len(df)} rows from application database")
    if len(df) == 0:
        logger.info("No new data to process for application data pipeline")
        return
    logger.info("Transforming application data")
    df = pd.DataFrame(df, columns=application_db_schema.keys())
    df = transform_data(df, application_db_schema)
    logger.info("Verifying data quality for application data")
    df = verify_data_quality(df, application_db_schema_config)

    logger.info("Saving application data to datalake")
    save_file_from_df(df=df, assunto="dados_aplicacao", nivel="silver", extensao="csv")

    logger.info("Fixing null values for MySQL compatibility in application data")
    df = fix_null_values_mysql(df)

    df["loaddate"] = pd.Timestamp.now()

    logger.info("Loading application data into warehouse database")
    bulk_upsert_warehouse_db(
        df,
        table_name="dados_aplicacao",
        primary_keys=application_db_schema_config["primary_keys"],
    )

    logger.info("Application data pipeline completed")
