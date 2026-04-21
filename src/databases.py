from sqlalchemy import create_engine, text
import pandas as pd
from sqlalchemy.dialects.mysql import insert as mysql_insert
from .config import main_db_config, warehouse_db_config

from data_logger import logging
logger = logging.getLogger(__name__)

main_db_url = f"mysql+pymysql://{main_db_config['user']}:{main_db_config['password']}@{main_db_config['host']}:{main_db_config['port']}/{main_db_config['database']}"
warehouse_url = f"mysql+pymysql://{warehouse_db_config['user']}:{warehouse_db_config['password']}@{warehouse_db_config['host']}:{warehouse_db_config['port']}/{warehouse_db_config['database']}"

def create_main_db_engine():
    return create_engine(main_db_url)

def create_warehouse_db_engine():
    return create_engine(warehouse_url)

def read_from_main_db(query):
    sql = text(query)
    with create_main_db_engine().connect() as connection:
        result = connection.execute(sql)
        return result.fetchall()

def read_from_warehouse_db(query):
    sql = text(query)
    with create_warehouse_db_engine().connect() as connection:
        result = connection.execute(sql)
        return result.fetchall()

def bulk_upsert_warehouse_db(df, table_name, primary_keys):
    from sqlalchemy import MetaData, Table

    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=create_warehouse_db_engine())

    records = df.to_dict("records")
    insert_stmt = mysql_insert(table).values(records)

    update_columns = {c.name: c for c in insert_stmt.inserted if c.name not in primary_keys}

    upsert_stmt = insert_stmt.on_duplicate_key_update(update_columns)

    with create_warehouse_db_engine().begin() as connection:
        connection.execute(upsert_stmt)

def overwrite_warehouse_db(df, table_name):
    from sqlalchemy import MetaData, Table

    with create_warehouse_db_engine().begin() as connection:
        logger.info(f"Overwriting table {table_name} in warehouse database")
        # 1. Deletar registros existentes (opcional, dependendo do comportamento desejado)
        delete_sql = text(f"DELETE FROM {table_name}")
        connection.execute(delete_sql)

        # 2. Inserir novos registros
        records = df.to_dict("records")
        if records: 
            insert_stmt = mysql_insert(Table(table_name, MetaData(), autoload_with=create_warehouse_db_engine())).values(records)
            connection.execute(insert_stmt)

def create_execution_log_table():
    with create_warehouse_db_engine().begin() as connection:
        logger.info("Creating execution log table if it does not exist")
        sql = """CREATE TABLE IF NOT EXISTS warehouse_db.pipeline_execution_log (
            id INT AUTO_INCREMENT PRIMARY KEY,
            pipeline_name VARCHAR(255) NOT NULL,
            execution_time DATETIME NOT NULL
        )"""

        connection.execute(text(sql))
        connection.commit()

def create_costs_table():
    with create_warehouse_db_engine().begin() as connection:
        logger.info("Creating costs table if it does not exist")
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

        connection.execute(text(sql))
        connection.commit()

def create_dados_aplicacao_table():
    with create_warehouse_db_engine().begin() as connection:
        logger.info("Creating dados_aplicacao table if it does not exist")
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
            duracao TIME,
            loaddate DATETIME
        )"""

        connection.execute(text(sql))
        connection.commit()

def get_last_execution_time(pipeline_name):
    import pandas as pd

    with create_warehouse_db_engine().connect() as connection:
        sql = """SELECT 
                    max(execution_time) as ultima_execucao 
                FROM warehouse_db.pipeline_execution_log
                WHERE pipeline_name = :pipeline_name"""
        
        result = connection.execute(text(sql), {"pipeline_name": pipeline_name})
        last_execution_time = result.fetchone()[0]
        last_execution_time = pd.Timestamp(last_execution_time) if last_execution_time else None
        return last_execution_time

def insert_execution_log(pipeline_name):
    with create_warehouse_db_engine().begin() as connection:
        logger.info(f"Inserting execution log for pipeline: {pipeline_name}")
        sql = """INSERT INTO warehouse_db.pipeline_execution_log (pipeline_name, execution_time) 
                 VALUES (:pipeline_name, NOW())"""
        connection.execute(text(sql), {"pipeline_name": pipeline_name})
        connection.commit()

def clean_execution_log(pipeline_name):
    with create_warehouse_db_engine().begin() as connection:
        logger.debug(f"Cleaning execution log for pipeline: {pipeline_name}")
        sql = """DELETE FROM warehouse_db.pipeline_execution_log WHERE pipeline_name = :pipeline_name"""
        connection.execute(text(sql), {"pipeline_name": pipeline_name})
        connection.commit()