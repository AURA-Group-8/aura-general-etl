import pathlib
import logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def main():
    logger.info("Starting ETL process")
    from src.etl.pipeline import run_pipeline
    run_pipeline()
    logger.info("ETL process completed")

if __name__ == "__main__":
    print("Running main.py")
    main()