# EtlFramework

## Overview
The **EtlFramework** is a metadata-driven ETL (Extract, Transform, Load) framework designed to streamline data processing and transformation workflows. It leverages **Microsoft Fabric** and **Apache Spark** to implement scalable and efficient data pipelines. The framework is built around a **Control DB** that dynamically orchestrates the ETL process based on metadata configurations.

![Framework Overview](image.png)

---

## Control DB
The **Control DB** is the backbone of the ETL framework. It contains an `ETLLoad` table that controls the end-to-end ETL process. This table defines the metadata required to dynamically configure pipelines and notebooks.

### Column Definitions:
1. **SourceZone**: Specifies the data source (e.g., SQL DB, Lakehouse, Internal, External).
2. **SinkZone**: Defines the destination layer (e.g., Bronze, Silver, Gold).
3. **SourceType**: Indicates the format of the source data (e.g., Table, API, JSON, CSV).
4. **SinkType**: Specifies the format of the destination data.
5. **SourceEntity**: Represents the data source value (e.g., API endpoint, Delta table name).
6. **SinkEntityName**: Represents the destination value.
7. **LoadType**: Defines the type of load (e.g., Incremental, Full Load, Fact Table, Dimension Table).
8. **LoadGroup**: Groups transformations (e.g., Stage, Dedup, Flatten).

![Control DB Schema](image-1.png)

---

## Medallion Architecture
The framework adopts the **Medallion Architecture** to organize data into three layers: **Bronze**, **Silver**, and **Gold**. This architecture ensures data quality and enables efficient data processing.

### Layers:
1. **Bronze Layer**: 
   - Raw data ingestion from various sources.
   - Minimal transformations (e.g., schema enforcement).
   - Stored in Delta Lake format for scalability.

2. **Silver Layer**:
   - Cleansed and enriched data.
   - Joins, deduplication, and filtering are applied.
   - Intermediate data used for analytics.

3. **Gold Layer**:
   - Aggregated and business-ready data.
   - Optimized for reporting and machine learning.

![Medallion Architecture](image-2.png)

---

## Apache Spark Optimization Techniques
The framework uses **Apache Spark** for data processing. Below are some optimization techniques implemented in the framework:

1. **Partitioning**:
   - Data is partitioned based on keys like `locRef` or `date` to improve parallelism.
   - Example: 
     ```python
     location_rdd = spark.sparkContext.parallelize(ls_location)
     ```

2. **Broadcast Variables**:
   - Small datasets (e.g., exclusion lists) are broadcasted to all nodes to reduce shuffle operations.
   - Example:
     ```python
     broadcast_exclusion_list = spark.sparkContext.broadcast(exclusion_list)
     ```

3. **Caching**:
   - Frequently accessed data is cached to avoid recomputation.
   - Example:
     ```python
     df.cache()
     ```

4. **Predicate Pushdown**:
   - SQL queries are optimized to filter data at the source.
   - Example:
     ```python
     df = spark.sql("SELECT * FROM table WHERE condition")
     ```

5. **Delta Lake**:
   - Delta Lake is used for ACID transactions and schema enforcement.
   - Example:
     ```python
     df.write.format("delta").saveAsTable("DE_LH_CONTROL.ETL_LastStagedDates")
     ```

6. **Parallelism**:
   - RDDs and DataFrames are processed in parallel using Spark's distributed computing capabilities.
   - Example:
     ```python
     response_rdd = location_rdd.map(stage_guestChecks)
     ```

---

## Addressing Schema Evolution
Schema evolution is a critical aspect of modern ETL frameworks, especially when dealing with dynamic and ever-changing data sources. The **EtlFramework** addresses schema evolution using the following techniques:

### 1. **Delta Lake Schema Evolution**
Delta Lake provides built-in support for schema evolution, allowing the framework to handle changes in the data schema without breaking the pipeline.

- **Automatic Schema Updates**:
  Delta Lake can automatically update the schema when new columns are added to the source data.
  ```python
  df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("target_table")
  ```

---

## Key Features
1. **Metadata-Driven**:
   - Dynamically configures pipelines based on metadata in the `ETLLoad` table.

2. **Parallel Processing**:
   - Uses Spark RDDs and DataFrames to process data in parallel.

3. **Dynamic Transformation Logic**:
   - Transformation logic is stored in text files and applied dynamically.

4. **Error Handling**:
   - Logs errors and retries failed operations.

5. **Integration with Microsoft Fabric**:
   - Leverages Fabric's Lakehouse for data storage and processing.

---

## Example Use Cases
1. **Staging Data**:
   - APIs like `getGuestChecks` and `getMenuItemDailyTotals` are used to fetch data and store it in the Bronze layer.
   - Example:
     ```python
     url = f"{Oracle_BaseURL}/bi/v1/{Oracle_APIOrgName}/{source_entity_name}"
     response = requests.post(url, headers=headers, data=payload)
     ```

2. **Data Deduplication**:
   - Deduplication logic is applied in the Silver layer using SQL scripts stored in text files.

3. **Archiving Processed Files**:
   - Processed files are moved to an archive folder.
   - Example:
     ```python
     shutil.move(source_file, target_file)
     ```

---

## Getting Started
1. **Set Up Notebook**:
   - Import required libraries and configure Spark session.
   - Example:
     ```python
     from pyspark.sql import SparkSession
     spark = SparkSession.builder.appName("ETL Framework").getOrCreate()
     ```

2. **Configure Metadata**:
   - Populate the `ETLLoad` table with metadata for your pipeline.

3. **Run Pipelines**:
   - Execute notebooks for staging, deduplication, and loading.

---

## Future Enhancements
1. Implement advanced Spark optimizations like **Adaptive Query Execution (AQE)**.
2. Add support for real-time streaming data ingestion.
3. Enhance monitoring and logging capabilities.

---

## References
1. [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
2. [Delta Lake Documentation](https://delta.io/)
3. [Microsoft Fabric Documentation](https://learn.microsoft.com/en-us/fabric/)

---

