import os
import dotenv

dotenv.load_dotenv()

main_db_config = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME')
}

warehouse_db_config = {
    'host': os.getenv('WAREHOUSE_DB_HOST'),
    'port': os.getenv('WAREHOUSE_DB_PORT'),
    'user': os.getenv('WAREHOUSE_DB_USER'),
    'password': os.getenv('WAREHOUSE_DB_PASSWORD'),
    'database': os.getenv('WAREHOUSE_DB_NAME')
}

HOST_API = os.getenv('HOST_API')
PORT_API = os.getenv('PORT_API')

schema = {
    "data_compra":"datetime64[ns]",
    "fornecedor":"string",
    "nome_material":"string",
    "marca":"string",
    "data_validade":"datetime64[ns]",
    "quantidade_comprada":"Int64",
    "unidade_medida":"string",
    "valor_total":"float",
    "observacoes":"string",
}

schema_config = {
    "unique": [
        "data_compra",
        "fornecedor",
        "nome_material",
        "marca"
    ],

    "not_null": [
        "data_compra",
        "fornecedor",
        "nome_material",
        "quantidade_comprada",
        "valor_total"
    ],

    "primary_keys": [
        "data_compra",
        "fornecedor",
        "nome_material"
    ],

    "types": schema
}

BRONZE_PATH = os.getenv('BRONZE_PATH')
SILVER_PATH = os.getenv('SILVER_PATH')

main_db_schema = {
    "schedule_id": "Int64",
    "user_id": "Int64",
    "username": "string",
    "role_id": "Int64",
    "job_id": "Int64",
    "name": "string",
    "description": "string",
    "start_datetime": "datetime64[ns]",
    "end_datetime": "datetime64[ns]",
    "status": "string",
    "canceled": "boolean",
    "days_of_week": "string",
    "work_start": "timedelta64[ns]",
    "work_end": "timedelta64[ns]",
    "break_start": "timedelta64[ns]",
    "break_end": "timedelta64[ns]",
    "current_price": "float",
    "total_price": "float",
    "duracao": "timedelta64[ns]"
}

main_db_schema_config = {
    "unique": [],
    "not_null": [],
    "primary_keys": [],
    "types": schema
}