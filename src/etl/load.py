import datetime
import shutil

def save_bronze_file(file_path, file):
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
