import pathlib
from data_logger import logging
logger = logging.getLogger(__name__)

# def main():
#     logger.info("Starting ETL process")
#     from src.etl.pipeline import run_pipeline
#     run_pipeline()
#     logger.info("ETL process completed")


if __name__ == "__main__":
    from src.etl.pipeline import custos_pipeline
    custos_pipeline()