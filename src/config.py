import os
import dotenv

dotenv.load_dotenv()

db_config = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME')
}

HOST_API = os.getenv('HOST_API')
PORT_API = os.getenv('PORT_API')

schema = {
    "data_compra":"datetime",
    "fornecedor":"string",
    "nome_material":"string",
    "marca":"string",
    "data_validade":"datetime",
    "quantidade_comprada":"int",
    "unidade_medida":"string",
    "valor_total":"float",
    "observacoes":"string",
}