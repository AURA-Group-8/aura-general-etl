from sqlalchemy import create_engine, text
from sqlalchemy.dialects.mysql import insert as mysql_insert
from src.config import application_db_config, warehouse_db_config

from data_logger import logging

logger = logging.getLogger(__name__)

application_db_url = f"mysql+pymysql://{application_db_config['user']}:{application_db_config['password']}@{application_db_config['host']}:{application_db_config['port']}/{application_db_config['database']}"
warehouse_url = f"mysql+pymysql://{warehouse_db_config['user']}:{warehouse_db_config['password']}@{warehouse_db_config['host']}:{warehouse_db_config['port']}/{warehouse_db_config['database']}"


def create_application_db_engine():
    return create_engine(application_db_url)


def create_warehouse_db_engine():
    return create_engine(warehouse_url)


def execute_application_db_sql(query):
    with create_application_db_engine().begin() as connection:
        result = connection.execute(text(query))

        if result.returns_rows:
            return result.fetchall()

        return None


def execute_warehouse_sql(query: str, params: dict | None = None):
    with create_warehouse_db_engine().begin() as connection:
        result = connection.execute(text(query), params or {})

        if result.returns_rows:
            return result.fetchall()

        return None


def bulk_upsert_warehouse_db(df, table_name, primary_keys):
    from sqlalchemy import MetaData, Table

    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=create_warehouse_db_engine())

    records = df.to_dict("records")

    logger.info(
        f"Upserting {len(records)} records into warehouse database table {table_name}"
    )
    insert_stmt = mysql_insert(table).values(records)

    update_columns = {
        c.name: c
        for c in insert_stmt.inserted
        if c.name not in primary_keys and c.name != "loaddate"
    }

    logger.debug(f"Update columns for upsert: {list(update_columns.keys())}")
    upsert_stmt = insert_stmt.on_duplicate_key_update(update_columns)

    logger.info(f"Executing upsert statement for table {table_name}")
    with create_warehouse_db_engine().begin() as connection:
        result = connection.execute(upsert_stmt)

    logger.info(f"Affected rows: {result.rowcount}")
