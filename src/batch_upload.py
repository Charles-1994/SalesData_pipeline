from src.utils import logger, create_spark_session, get_csv_files, write_to_file, remove_nulls, remove_duplicates, renaming_columns, save_full_table
from pyspark.sql.functions import to_timestamp, year, month

def batch_process(folder_path: str = 'Sales_Data', file_path: str = 'list_of_files.txt') -> None:
    """
    Perform batch processing on CSV files in a folder, cleaning and saving them as a Delta table.

    Args:
        folder_path (str): Path to the folder containing CSV files.
        file_path (str): Path to the file that tracks processed files.

    Returns:
        None
    """
    logger.info(f"Starting batch processing for folder: {folder_path}")

    try:
        spark = create_spark_session()
        
        csv_files = get_csv_files(folder_path)

        write_to_file(csv_files, file_path, 'w')  # Write all file paths to the tracking file

        logger.info("Reading the first file to get the reference schema...")
        # Read the first file to get the reference schema
        main_df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_files[0])
        reference_schema = main_df.schema
        logger.info(f"Reference schema fetched from the first file: {csv_files[0]}")
        
        logger.info("Processing remaining files...")
        # Process remaining files
        for file in csv_files[1:]:
            try:
                df = spark.read.option("header", "true").option("inferSchema", "true").csv(file)
            
                if df.schema == reference_schema:  # Ensure schema matches
                    main_df = main_df.union(df)
                    logger.info(f"File {file} successfully processed and added to the main DataFrame.")
                else:
                    logger.warning(f"Schema mismatch for file: {file}. Skipping this file.")
            except Exception as e:
                logger.error(f"Error while processing file: {file} - {e}")

        # Clean and process data
        main_df = remove_nulls(main_df)

        logger.info("Removing duplicates from the DataFrame...")
        main_df = remove_duplicates(main_df)
        
        logger.info("Converting 'Order Date' column to timestamp format...")
        main_df = main_df.withColumn('Order Date', to_timestamp('Order Date', 'MM/dd/yy HH:mm'))
        
        # Add partitioning columns
        logger.info("Adding partitioning columns 'year' and 'month'...")
        main_df = main_df.withColumn("year", year(main_df["Order Date"]))
        main_df = main_df.withColumn("month", month(main_df["Order Date"]))

        # Rename columns and save the table
        main_df = renaming_columns(main_df)
        
        logger.info("Saving the Delta table...")
        save_full_table(main_df)
        
        # Show results and stop Spark session
        main_df.show()
    except Exception as e:
        logger.error(f"Error during batch processing: {e}")

    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark session stopped.")

if __name__ == "__main__":
    batch_process()