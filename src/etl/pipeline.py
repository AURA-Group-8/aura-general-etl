import logging
import pathlib
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_pipeline():
    from src.etl.extract import extract_data_from_path
    from src.etl.transform import transform_data
    from src.etl.load import save_silver_file

    root_dir = pathlib.Path(__file__).resolve().parents[2]
    logger.info(f"Root directory: {root_dir}")
    file_path = f'{root_dir}/data/bronze/2026-03-22_template.xlsx'  # Example file path
    df = extract_data_from_path(file_path)
    logger.info(f"Data loaded successfully from {file_path}")
    logger.info(f"Data preview:\n{df.head()}")

    normalized_df = transform_data(df)
    logger.info(f"Normalized data preview:\n{normalized_df.head()}")

    silver_path = f'{root_dir}/data/silver/'
    save_silver_file(file_path=silver_path, df=normalized_df)
