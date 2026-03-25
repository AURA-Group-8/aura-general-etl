import pandas as pdyy
from datetime import datetime 
import shutil

def save_bronze_file(file_path, file):
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

def save_silver_file(file_path, df):
    now = datetime.now()
    str_month_year = now.strftime("%m_%Y")
    file_name = f"custos_{str_month_year}.csv"
    full_path = file_path + file_name

    df.to_csv(full_path, index=False)
