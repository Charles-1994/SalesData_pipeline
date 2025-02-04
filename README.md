# SalesData_pipeline
This projects ingests data, transform and loads into a Parquet/Delta-table file with partitioning strategies to efficiently run on Analytical tools

Here is the **GitHub README.md** file written in markdown format based on the provided project details and our discussions:

---

# **Sales Data Pipeline**

## **Project Overview**
The Sales Data Pipeline is a data engineering solution designed to process retail sales data. It ingests monthly sales data from CSV files, cleanses and deduplicates the data, and stores it in a partitioned Delta Table format for efficient querying and reporting. The pipeline is automated using Airflow and packaged as a Python distribution for easy deployment.

### **Aim**
- Ingest sales data from CSV files in the `Sales_Data` folder into a PySpark DataFrame.
- Clean the data, remove duplicates, and store it in a desirable format (Delta Table) in Azure Data Lake Storage (ADLS) for analytical systems like Power BI or Tableau.

### **Data Sets**
- Monthly sales data files from January 2019 to December 2019 (e.g., `Sales_January_2019.csv` to `Sales_December_2019.csv`).

---

## **Tasks**
1. Design a process to read all files from the source folder using PySpark, combine them into a single file, and write it to the cleansed folder.
2. Choose an appropriate file format for storing the cleansed data and justify the choice.
3. Propose a data partitioning strategy for the table and explain its advantages.
4. Outline steps to implement the partitioning strategy, considering technical aspects and challenges.

---

## **Approach**

### **Batch Upload Process**
**Objective**: Process all CSV files in the source folder during the initial run and save the cleansed data in Delta Table format.

#### Steps:
1. Get a list of all CSV files from the source folder.
2. Write this list into a tracking file (`list_of_files.txt`) to monitor processed files.
3. Read each CSV file into a PySpark DataFrame.
4. Perform:
   - Null handling (remove records with invalid or null `Order ID`).
   - Deduplication (remove exact duplicates).
5. Combine all DataFrames into a single DataFrame.
6. Partition the data by `year` and `month`, then save it as a Delta Table in the cleansed folder.

**Trigger**: Manually initiated by the user.

---

### **Incremental Upload Process**
**Objective**: Process new CSV files added to the source folder monthly and append them to the Delta Table.

#### Steps:
1. Get a list of all CSV files from the source folder.
2. Compare this list with `list_of_files.txt` to identify new files.
3. For each new file:
   - Read it into a PySpark DataFrame.
   - Perform null handling, deduplication, and cleaning.
4. Append the processed DataFrame to the existing Delta Table while adhering to the partitioning strategy.

**Trigger**: Automatically triggered once a month when new files are added to the source folder.

---

## **Data Exploration**

### **Schema**
The schema inferred from CSV files:
```
Root
 |-- Order ID: integer (nullable = true)
 |-- Product: string (nullable = true)
 |-- Quantity Ordered: integer (nullable = true)
 |-- Price Each: double (nullable = true)
 |-- Order Date: string (nullable = true)
 |-- Purchase Address: string (nullable = true)
```

The desired schema after cleaning:
```
Root
 |-- Order ID: integer (nullable = False)
 |-- Product: string (nullable = False)
 |-- Quantity Ordered: integer (nullable = False)
 |-- Price Each: double (nullable = true)
 |-- Order Date: timestamp (nullable = False)
 |-- Purchase Address: string (nullable = true)
```

### **Null Handling**
- Records with null or invalid `Order ID` are removed since they represent incomplete or garbage data.

### **Deduplication**
- Total records before deduplication: 185,950
- Distinct records after deduplication: 185,686
- Deduplication logic:
  - Records with identical values in all columns are considered duplicates and removed.
  - Records with same `Order ID` and `Product` but different quantities are retained only if they have unique values across all columns.

---

## **File Format**

### **Chosen Format**: Delta Table

### **Justification**:
1. **ACID Transactions**: Ensures consistency during incremental updates.
2. **Schema Enforcement**: Prevents schema mismatches during writes.
3. **Schema Evolution**: Supports adding or modifying columns without breaking existing pipelines.
4. **Time Travel**: Enables querying historical states of the table.
5. **Efficient Metadata Handling**: Optimized for large-scale datasets with frequent updates.

---

## **Partitioning Strategy**

### **Proposed Strategy**
Partition by `year` and `month` based on the `Order Date`.

### **Justification**
- Time-based partitioning aligns with common query patterns (e.g., monthly or yearly sales reports).
- Reduces scan size for time-based queries while maintaining manageable partition sizes (~14,200 records per month).

### **Implementation Steps**
1. Extract `year` and `month` from the `Order Date` column using PySpark functions:
   ```python
   df = df.withColumn("year", year(df["Order Date"]))
   df = df.withColumn("month", month(df["Order Date"]))
   ```
2. During writing, specify partitioning columns:
   ```python
   df.write.format("delta").partitionBy("year", "month") \
       .mode("overwrite").save("cleansed/Sales_Data")
   ```

---

## **Unit Tests**
Unit tests for functions in `common_funcs.py` are implemented using Pytest and Chispa modules. These tests validate core functionalities like null handling, deduplication, column renaming, etc.

---

## **Automation with Airflow**

### **Batch Upload DAG**
- Triggered manually by users.
- Executes the batch upload pipeline.

### **Incremental Upload DAG**
- Scheduled to run automatically once a month.
- Executes incremental processing for new files.

---

## **CI/CD Pipeline**

### GitHub Workflows
1. Linting with Pylint for code quality checks.
2. Unit testing with Pytest and Chispa for validating transformations.
3. Building a Python package (`whl` file) for distribution.
4. Deploying artifacts as downloadable builds.

---

## **Instructions to Run**

Extract contents of distribution:
```
Sales_data_pipeline-1.0.tar.gz 
Sales_data_pipeline-1.0-py3-none-any.whl
```

### Using `.tar.gz`:
1. Extract `Sales_data_pipeline-1.0.tar.gz`.
2. Install using:
   ```bash
   pip install .
   ```
3. Run batch upload:
   ```bash
   batch_upload
   ```
4. Run batch upload with source folder as argument:
   ```bash
   batch_upload path/to/source
   ```
5. Run incremental upload:
   ```bash
   incremental_upload
   ```
6. Run incremental upload with source folder and delta table as arguments:
   ```bash
   incremental_upload path/to/source path/to/delta/table
   ```
   or if the delta tables in the same folder
   ```bash
   incremental_upload path/to/source
   ```

### Using `.whl`:
1. Install directly:
   ```bash
   pip install Sales_data_pipeline-1.0-py3-none-any.whl
   ```
2. Run batch upload with source folder as argument:
   ```bash
   batch_upload path/to/source
   ```
3. Run incremental upload with source folder and delta table as arguments:
   ```bash
   incremental_upload path/to/source path/to/delta/table
   ```

Both operations will create a `cleansed/Sales_Data/` folder containing data in Delta Table format.

---

## **Future Improvements**
1. Migrate storage to Azure Data Lake Storage Gen2 for scalability.
2. Use Z-ordering in Delta Lake for optimizing queries on secondary columns like `Product`.
3. Add monitoring and alerting for Airflow DAGs.
4. Add Mocker tests for Batch_upload and incremental_upload pipeline

---