# Databricks notebook source
bronze_path = "abfss://bronze@altechsg.dfs.core.windows.net/SalesLT"
display(dbutils.fs.ls(bronze_path))

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1: Bronze to Silver Transformations

# COMMAND ----------

from pyspark.sql.functions import col, to_date

bronze_path = "abfss://bronze@altechsg.dfs.core.windows.net/SalesLT"
silver_path = "abfss://silver@altechsg.dfs.core.windows.net/SalesLT"

tables = [file_info.name.strip("/") for file_info in dbutils.fs.ls(bronze_path)]

for table in tables:
    input_path = f"{bronze_path}/{table}/{table}.parquet"
    output_path = f"{silver_path}/{table}"

    print(f"Transforming and writing {table} to Silver...")

    df = spark.read.parquet(input_path)

    # Clean column headers
    col_map = {
        c: (
            c.replace(" ", "_")
            .replace("-", "_")
            .replace("/", "_")
            .replace("(", "")
            .replace(")", "")
        )
        for c in df.columns
    }
    df = df.select([col(c).alias(col_map[c]) for c in df.columns])

    # Standardize timestamps to Date format
    dtypes = df.dtypes
    cols = df.columns
    df = df.select([
        to_date(col(col_name)).alias(col_name) if "timestamp" in data_type else col(col_name)
        for col_name, data_type in dtypes
    ])

    df.write.format("delta").mode("overwrite").save(output_path)
    print(f"Done: {table}")

print("\nAll 10 tables have been transformed and written to Silver as Delta!")
