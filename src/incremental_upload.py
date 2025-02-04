from pyspark.sql import SparkSession
import os
import argparse
from pyspark.sql.functions import to_timestamp, year, month
from src.utils import logger, get_spark_session
from src.common_funcs import get_csv_files, write_to_file, remove_nulls, remove_duplicates, renaming_columns, save_full_table

def main():
    """
    Incremental processing pipeline for sales data.

    This script processes new CSV files from a specified folder, cleans the data,
    and appends it to an existing Delta table. It uses a tracking file to identify
    new files that need to be processed.

    Args:
        folder_path (str): Path to the folder containing CSV files. Defaults to './source'.
        file_path (str): Path to the tracking file that keeps track of processed files. Defaults to './list_of_files.txt'.
        main_df_path (str): Path to the Delta table folder where the cleansed data is stored. Defaults to './cleansed/Sales_Data'.

    Returns:
        None
    """

    # Set up argument parsing
    parser = argparse.ArgumentParser(description="Incremental upload pipeline")
    parser.add_argument(
        "folder_path",
        nargs="?",
        default="./source",
        help="Path to the folder containing CSV files (default: 'source')"
    )
    parser.add_argument(
        "main_df_path",
        nargs="?",
        default="./cleansed/Sales_Data",
        help="Path to the folder where the main delta file is (default: './cleansed/Sales_Data')"
    )
    parser.add_argument(
        "file_path",
        nargs="?",
        default="./list_of_files.txt",
        help="Path to the file that tracks processed files (default: 'list_of_files.txt')"
    )
    args = parser.parse_args()

    # Convert folder_path to a Path object and resolve it to an absolute path
    folder_path = args.folder_path
    file_path = args.file_path
    main_df_path = args.main_df_path

    logger.info(f"Starting incremental upload process for folder: {folder_path}")

    try:
        # Initialize Spark session
        spark = get_spark_session()

        logger.info(f"Processing folder: {folder_path}")
        # Get all CSV files in the source folder
        csv_files = set(get_csv_files(folder_path))

        logger.info(f"Checking for new file upload at {folder_path}...")
        # Read all file in the tracking file
        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                existing_files = set(file.read().splitlines())
        else:
            existing_files = set()

        new_file = csv_files - existing_files
     
        if new_file:
            logger.info(f"New files detected: {new_file}")

            # Append content to the file
            write_to_file(new_file, file_path, 'a')

            logger.info("Reading the new file and cleaning the data ...")
            # Reading the new file
            new_df = spark.read.option("header", "true").option("inferSchema", "true").csv(list(new_file))

            # Cleaning and processing the data
            new_df = remove_nulls(new_df)
            new_df = remove_duplicates(new_df)

            # Converting the 'Order Date' column to datetime
            new_df = new_df.withColumn('Order Date', to_timestamp('Order Date', 'MM/dd/yy HH:mm'))
        
            # Adding year and month column for better partitioning the data
            new_df = new_df.withColumn("year", year(new_df["Order Date"]))
            new_df = new_df.withColumn("month", month(new_df["Order Date"]))
            new_df = renaming_columns(new_df)

            # Read schema of the delta file
            abs_folder_path = os.path.abspath(main_df_path)
            reference_schema = spark.read.format("delta").load(abs_folder_path).schema
            
            logger.info("comparing schema of new data to the delta table schema...")
            if new_df.schema == reference_schema:
                try:    
                    logger.info("Updating the Delta table with the cleaned data...")
                    logger.info(f"Current working directory: {os.getcwd()}")
                    # Append the new records directly to the delta table
                    new_df.write.format("delta").mode("append").save(abs_folder_path)
                    logger.info(f"New data appended successfully.")
                except Exception as e:
                    logger.error(f"Error while appending new data: {e}")

            else:
                logger.error("Schema does not match for new data. "
                    "Expected schema:\n", reference_schema, "\nBut got:\n", new_df.schema)  
        else:
            logger.info(f"Data file up to date. No new files in the source: {folder_path}")

    except Exception as e:
        logger.error(f"Error during incremental upload: {e}")

    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()