from data_logger import logging

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    from src.infraestructure.schema_manager import create_all_tables
    from src.etl.pipeline_costs import custos_pipeline
    from src.etl.pipeline_application import dados_aplicacao_pipeline

    create_all_tables()

    logger.info("Starting ETL pipelines")
    custos_pipeline()
    logger.info("Cost analysis pipeline finished")
    logger.info("Starting application data pipeline")
    dados_aplicacao_pipeline()
    logger.info("Application data pipeline finished")
