import logging
from logging.handlers import RotatingFileHandler
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def get_spark_session(app_name: str ='Sales_data') -> SparkSession:
    """
    Creates a SparkSession.
    
    Args:
        app_name (str): Name of the Spark application.
        
    Returns:
        SparkSession: A SparkSession object.
    """

    builder = SparkSession.builder.appName(app_name) \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.network.timeout", "800s") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # Configure Spark with Delta using the imported method
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark

# Configure logging
def setup_logger(mode: str = 'Batch Upload') -> logging.Logger:
    """
    Setup and return a logger with a rotating file handler.
    
    Returns:
        Logger: Configured logger.
    """
    logger = logging.getLogger("SalesDataPipelineLogger")
    logger.setLevel(logging.INFO)

    if mode != 'Batch Upload':
        log_file = "increment_upload.log"
    else:
        log_file = "batch_upload.log"

    # Create handlers
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    file_handler = RotatingFileHandler(log_file, maxBytes=1*1024*1024, backupCount=5)
    file_handler.setLevel(logging.INFO)

    # Create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Add handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger

logger = setup_logger()