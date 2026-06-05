from src.infraestructure.databases import execute_warehouse_sql

from data_logger import logging

logger = logging.getLogger(__name__)

PIPELINE_NAMES = {"dados_aplicacao_pipeline", "analise_custos_pipeline"}


def validate_pipeline_name(pipeline_name):
    if pipeline_name not in PIPELINE_NAMES:
        logger.error(f"Invalid pipeline name: {pipeline_name}")
        raise ValueError(f"Invalid pipeline name: {pipeline_name}")


def get_last_execution_time(pipeline_name):
    import pandas as pd

    validate_pipeline_name(pipeline_name)

    logger.debug(f"Getting last execution time for pipeline: {pipeline_name}")
    sql = """SELECT
                max(execution_time) as ultima_execucao
            FROM warehouse_db.pipeline_execution_log
            WHERE pipeline_name = :pipeline_name"""

    result = execute_warehouse_sql(sql, {"pipeline_name": pipeline_name})
    last_execution_time = result[0][0] if result else None
    last_execution_time = (
        pd.Timestamp(last_execution_time) if last_execution_time else None
    )
    logger.debug(
        f"Retrieved last execution time for pipeline {pipeline_name}: {last_execution_time}"
    )
    return last_execution_time


def insert_execution_log(pipeline_name):
    validate_pipeline_name(pipeline_name)
    logger.debug(f"Inserting execution log for pipeline: {pipeline_name}")
    sql = """INSERT INTO warehouse_db.pipeline_execution_log (pipeline_name, execution_time)
                VALUES (:pipeline_name, NOW())"""
    execute_warehouse_sql(sql, {"pipeline_name": pipeline_name})


def clean_execution_log(pipeline_name):
    validate_pipeline_name(pipeline_name)
    logger.debug(f"Cleaning execution log for pipeline: {pipeline_name}")
    sql = """DELETE FROM warehouse_db.pipeline_execution_log WHERE pipeline_name = :pipeline_name"""
    execute_warehouse_sql(sql, {"pipeline_name": pipeline_name})
