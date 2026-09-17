# Databricks notebook source
from pyspark.sql.functions import col, concat, lit

silver_path = "abfss://silver@altechsg.dfs.core.windows.net/SalesLT"
gold_path = "abfss://gold@altechsg.dfs.core.windows.net/SalesLT"

# 1. Load Silver Delta tables
df_customer = spark.read.format("delta").load(f"{silver_path}/Customer")
df_cust_addr = spark.read.format("delta").load(f"{silver_path}/CustomerAddress")
df_address = spark.read.format("delta").load(f"{silver_path}/Address")
df_order_hdr = spark.read.format("delta").load(f"{silver_path}/SalesOrderHeader")
df_order_det = spark.read.format("delta").load(f"{silver_path}/SalesOrderDetail")
df_product = spark.read.format("delta").load(f"{silver_path}/Product")
df_category = spark.read.format("delta").load(f"{silver_path}/ProductCategory")

# 2. Build Customer Dimension (DimCustomer)
df_dim_customer = (
    df_customer.join(df_cust_addr, "CustomerID", "left")
    .join(df_address, "AddressID", "left")
    .select(
        df_customer["CustomerID"],
        concat(
            col("FirstName"),
            lit(" "),
            col("MiddleName"),
            lit(" "),
            col("LastName"),
        ).alias("FullName"),
        df_customer["CompanyName"],
        df_customer["EmailAddress"],
        df_customer["Phone"],
        df_address["City"],
        df_address["StateProvince"],
        df_address["CountryRegion"],
    )
    .dropDuplicates(["CustomerID"])
)

df_dim_customer.write.format("delta").mode("overwrite").save(
    f"{gold_path}/DimCustomer"
)

print("Saved DimCustomer to Gold.")

# 3. Build Product Dimension (DimProduct)
df_dim_product = (
    df_product.join(
        df_category,
        df_product["ProductCategoryID"] == df_category["ProductCategoryID"],
        "left",
    )
    .select(
        df_product["ProductID"],
        df_product["Name"].alias("ProductName"),
        df_product["ProductNumber"],
        df_product["Color"],
        df_product["StandardCost"],
        df_product["ListPrice"],
        df_product["Size"],
        df_category["Name"].alias("CategoryName"),
    )
    .dropDuplicates(["ProductID"])
)

df_dim_product.write.format("delta").mode("overwrite").save(
    f"{gold_path}/DimProduct"
)
print("Saved DimProduct to Gold.")

# 4. Build Sales Fact Table (FactSales)
df_fact_sales = df_order_hdr.join(df_order_det, "SalesOrderID", "inner").select(
    df_order_hdr["SalesOrderID"],
    df_order_hdr["OrderDate"],
    df_order_hdr["CustomerID"],
    df_order_det["ProductID"],
    df_order_det["OrderQty"],
    df_order_det["UnitPrice"],
    df_order_det["LineTotal"],
    df_order_hdr["SubTotal"],
    df_order_hdr["TaxAmt"],
    df_order_hdr["Freight"],
    df_order_hdr["TotalDue"],
)

df_fact_sales.write.format("delta").mode("overwrite").save(
    f"{gold_path}/FactSales"
)
print("Saved FactSales to Gold.")
print("\nAll Gold tables successfully written!")

# COMMAND ----------

display(
    dbutils.fs.ls("abfss://gold@altechsg.dfs.core.windows.net/SalesLT/FactSales")
)