from src.infraestructure.databases import execute_warehouse_sql

from data_logger import logging

logger = logging.getLogger(__name__)


def create_execution_log_table():
    logger.debug("Creating execution log table if it does not exist")
    sql = """CREATE TABLE IF NOT EXISTS warehouse_db.pipeline_execution_log (
        id INT AUTO_INCREMENT PRIMARY KEY,
        pipeline_name VARCHAR(255) NOT NULL,
        execution_time DATETIME NOT NULL
    )"""
    execute_warehouse_sql(sql)


def create_costs_table():
    logger.debug("Creating costs table if it does not exist")
    sql = """CREATE TABLE IF NOT EXISTS warehouse_db.costs (
        data_compra DATETIME NOT NULL,
        fornecedor VARCHAR(255) NOT NULL,
        nome_material VARCHAR(255) NOT NULL,
        marca VARCHAR(255),
        data_validade DATETIME,
        quantidade_comprada INT,
        unidade_medida VARCHAR(50),
        valor_total FLOAT,
        observacoes TEXT,
        loaddate DATETIME,
        PRIMARY KEY (data_compra, fornecedor, nome_material)
    )"""
    execute_warehouse_sql(sql)


def create_dados_aplicacao_table():
    logger.debug("Creating dados_aplicacao table if it does not exist")
    sql = """CREATE TABLE IF NOT EXISTS warehouse_db.dados_aplicacao (
        schedule_id INT,
        user_id INT,
        username VARCHAR(255),
        role_id INT,
        job_id INT,
        name VARCHAR(255),
        description TEXT,
        start_datetime DATETIME,
        end_datetime DATETIME,
        status VARCHAR(50),
        canceled BOOLEAN,
        days_of_week VARCHAR(50),
        work_start TIME,
        work_end TIME,
        break_start TIME,
        break_end TIME,
        current_price FLOAT,
        total_price FLOAT,
        duracao INT,
        loaddate DATETIME,
        PRIMARY KEY (schedule_id, job_id)
    )"""
    execute_warehouse_sql(sql)


def create_all_tables():
    logger.debug("Creating all necessary tables in the warehouse database")
    create_execution_log_table()
    create_costs_table()
    create_dados_aplicacao_table()
