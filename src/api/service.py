from src.etl.extract import get_latest_template_file_path
from src.etl.load import save_bronze_file

from datetime import datetime

def get_latest_template_file_path_service():
    path = get_latest_template_file_path()
    return path

def save_bronze_file_service(file):
    if file is None:
        return "Arquivo vazio"
    
    if not file.filename.endswith('.xlsx'):
        return "Só é aceito arquivos .xlsx"

    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    file_path = f"data/bronze/{date_str}_{file.filename}"

    save_bronze_file(file_path=file_path, file=file)

    return "Arquivo salvo com sucesso"
