import os
from typing import List

# spark packages
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, desc, row_number
from pyspark.sql.window import Window

from src.utils import logger

## Functions

def get_csv_files(folder_path: str) -> List[str]:
    """
    Get a list of all CSV file paths in the specified folder.

    Args:
        folder_path (str): The path to the folder containing the CSV files.

    Returns:
        List[str]: A list of file paths for all CSV files in the folder.
    """
    logger.info(f"Fetching CSV files from folder....")

    # Resolve to absolute path
    abs_folder_path = os.path.abspath(folder_path)
    logger.info(f"Resolved absolute path: {abs_folder_path}")
    
    if not os.path.exists(abs_folder_path):
        raise FileNotFoundError(f"Folder '{abs_folder_path}' does not exist!")
    
    csv_files = [os.path.join(abs_folder_path, f) for f in os.listdir(abs_folder_path) if f.endswith('.csv')]
    logger.info(f"Found {len(csv_files)} CSV files.")
    return csv_files


def write_to_file(file_list: List[str], file_path: str, mode: str) -> None:
    """
    Write a list of strings to a file.

    Args:
        file_list (List[str]): The list of strings to be written to the file.
        file_path (str): The path to the file where data will be written.
        mode (str): The mode in which the file should be opened (e.g., 'w' for write, 'a' for append).

    Returns:
        None
    """
    logger.info(f"Writing {len(file_list)} items to file: {file_path} in mode: {mode}")
    try:
        with open(file_path, mode) as file:
            for item in file_list:
                file.write(item + '\n')
        logger.info("File writing completed successfully.")
    except Exception as e:
        logger.error(f"Error while writing to file: {e}")


def remove_nulls(df: DataFrame) -> DataFrame:
    """
    Remove rows with null values in the 'Order ID' column from a Spark DataFrame.

    Args:
        df (DataFrame): The input Spark DataFrame.

    Returns:
        DataFrame: A Spark DataFrame with rows containing null 'Order ID' values removed.
    """
    logger.info("Removing rows with null values in 'Order ID'.")
    return df.filter(col('Order ID').isNotNull())


def remove_duplicates(df: DataFrame) -> DataFrame:
    """
    Remove duplicate rows from a Spark DataFrame based on 'Order ID' and 'Product' columns,
    keeping only the row with the highest 'Quantity Ordered'.

    Args:
        df (DataFrame): The input Spark DataFrame.

    Returns:
        DataFrame: A Spark DataFrame with duplicates removed.
    """
    df = df.dropDuplicates()
    logger.info("Removing duplicate rows based on 'Order ID' and 'Product'.")    
    
    # Define the window specification
    windowSpec = Window.partitionBy("Order ID", "Product").orderBy(desc("Quantity Ordered"))
    
    # Apply the window function and filter the results
    df = df.withColumn("row_number", row_number().over(windowSpec)) \
           .filter(col("row_number") == 1) \
           .drop("row_number")
    
    logger.info("Duplicate removal completed.")
    return df

def renaming_columns(df: DataFrame) -> DataFrame:
    """
    Rename specific columns in a Spark DataFrame for consistency.

    Args:
        df (DataFrame): The input Spark DataFrame.

    Returns:
        DataFrame: A Spark DataFrame with renamed columns.
    """
    logger.info("Renaming columns for consistency.")
    df = df.withColumnRenamed("Order ID", "Order_ID") \
           .withColumnRenamed("Quantity Ordered", "Quantity_Ordered") \
           .withColumnRenamed("Price Each", "Price_Each") \
           .withColumnRenamed("Order Date", "Order_Date") \
           .withColumnRenamed("Purchase Address", "Purchase_Address")
    
    logger.info("Column renaming completed.")
    return df

def save_full_table(df: DataFrame, cleansed_path: str = "cleansed") -> None:
    """
    Save a Spark DataFrame as a Delta table partitioned by 'year' and 'month'.

    Args:
        df (DataFrame): The input Spark DataFrame to be saved.
        cleansed_path (str): The directory path where the Delta table will be saved.

    Returns:
        None
    """
    # Create the directory if it doesn't exist
    if not os.path.exists(cleansed_path):
        os.makedirs(cleansed_path, exist_ok=True)
    abs_folder_path = os.path.abspath(cleansed_path+'/Sales_Data')
    logger.info(f"Saving Delta table to path: {abs_folder_path}")

    # Write the DataFrame to the cleansed folder as a Delta table
    try:
        df.write.format("delta") \
            .option("delta.columnMapping.mode", "none") \
            .partitionBy("year", "month") \
            .mode("overwrite") \
            .save(abs_folder_path)
        logger.info("Data saved successfully.")
    except Exception as e:
        logger.error(f"Error saving Delta table: {e}")
