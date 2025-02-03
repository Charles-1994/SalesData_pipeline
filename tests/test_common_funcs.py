import pytest
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality
from src.common_funcs import get_csv_files, write_to_file, remove_nulls, remove_duplicates, renaming_columns

# Initialize a Spark session for testing
@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("Test").getOrCreate()

# Test for get_csv_files
def test_get_csv_files(tmp_path):
    # Create temporary CSV files
    csv1 = tmp_path / "file1.csv"
    csv2 = tmp_path / "file2.csv"
    csv1.write_text("data1")
    csv2.write_text("data2")

    # Call the function and check results
    result = get_csv_files(str(tmp_path))
    assert len(result) == 2
    assert str(csv1) in result
    assert str(csv2) in result

# Test for write_to_file
def test_write_to_file(tmp_path):
    file_list = ["file1.csv", "file2.csv"]
    output_file = tmp_path / "output.txt"

    # Call the function
    write_to_file(file_list, str(output_file), "w")

    # Check the written content
    with open(output_file, "r") as f:
        lines = f.read().splitlines()
        assert lines == file_list

# Test for remove_nulls
def test_remove_nulls(spark):
    data = [("123", "Product A"), (None, "Product B")]
    schema = ["Order ID", "Product"]
    df = spark.createDataFrame(data, schema)

    # Call the function
    result_df = remove_nulls(df)

    # Expected DataFrame
    expected_data = [("123", "Product A")]
    expected_df = spark.createDataFrame(expected_data, schema)

    # Compare DataFrames
    assert_df_equality(result_df, expected_df)

# Test for remove_duplicates
def test_remove_duplicates(spark):
    data = [
        ("123", "Product A", 5),
        ("123", "Product A", 10),
        ("124", "Product B", 3),
        ("124", "Product B", 3)
    ]
    schema = ["Order ID", "Product", "Quantity Ordered"]
    df = spark.createDataFrame(data, schema)

    # Call the function
    result_df = remove_duplicates(df)

    # Expected DataFrame
    expected_data = [
        ("123", "Product A", 10),
        ("124", "Product B", 3)
    ]
    expected_df = spark.createDataFrame(expected_data, schema)

    # Compare DataFrames
    assert_df_equality(result_df, expected_df)

# Test for renaming_columns
def test_renaming_columns(spark):
    data = [("123", 5, 20.0, "2025-02-03", "Address A")]
    schema = ["Order ID", "Quantity Ordered", "Price Each", "Order Date", "Purchase Address"]
    df = spark.createDataFrame(data, schema)

    # Call the function
    result_df = renaming_columns(df)

    # Expected DataFrame
    expected_schema = ["Order_ID", "Quantity_Ordered", "Price_Each", "Order_Date", "Purchase_Address"]
    expected_data = [("123", 5, 20.0, "2025-02-03", "Address A")]
    
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    # Compare DataFrames
    assert_df_equality(result_df, expected_df)
