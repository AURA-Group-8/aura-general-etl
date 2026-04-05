from pandas import DataFrame
from datetime import datetime 
import shutil
import os

def save_bronze_file(file_path, file):
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

def save_silver_file(file_path, df):
    now = datetime.now()
    str_month_year = now.strftime("%m_%Y")
    file_name = f"custos_{str_month_year}.csv"
    full_path = file_path + file_name

    df.to_csv(full_path, index=False)

def save_silver_file(table, df: DataFrame):
    now = datetime.now()
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")

    timestamp = now.strftime("%H_%M_%S")

    file_name = f"{table}_{timestamp}.csv"
    path = f"data/silver/{table}/year={year}/month={month}/day={day}"
    full_path = f"{path}/{file_name}"

    os.makedirs(path, exist_ok=True)

    df.to_csv(full_path, index=False)
