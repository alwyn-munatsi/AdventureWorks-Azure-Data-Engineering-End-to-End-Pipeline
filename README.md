# Enterprise Azure Data Engineering Pipeline: AdventureWorks Sales Analytics

[![Azure Data Factory](https://img.shields.io/badge/Orchestration-Azure%20Data%20Factory-blue?logo=microsoftazure)](https://azure.microsoft.com/services/data-factory/)
[![Azure Databricks](https://img.shields.io/badge/Compute-Databricks%20Serverless-FF3621?logo=databricks)](https://databricks.com/)
[![Azure Synapse](https://img.shields.io/badge/Serving-Synapse%20Serverless%20SQL-0089D6?logo=microsoftazure)](https://azure.microsoft.com/services/synapse-analytics/)
[![Power BI](https://img.shields.io/badge/Analytics-Power%20BI%20Desktop-F2C811?logo=powerbi)](https://powerbi.microsoft.com/)

An end-to-end, enterprise-grade cloud data engineering and business intelligence solution built on Microsoft Azure. This project demonstrates the ingestion of transactional OLTP data from an on-premises Microsoft SQL Server database (`AdventureWorksLT`) into **Azure Data Lake Storage Gen2 (ADLS Gen2)** using **Azure Data Factory (ADF)** and a **Self-Hosted Integration Runtime (SHIR)**. Data is processed through a modern **Medallion Lakehouse Architecture (Bronze → Silver → Gold)** using **Azure Databricks Serverless Compute**, queried via virtualized views in **Azure Synapse Analytics Serverless SQL Pools**, and modeled into an interactive executive sales performance dashboard in **Power BI Desktop**.

---

## 1. System Architecture & End-to-End Data Flow

The following diagram illustrates the complete infrastructure and data lifecycle from on-premises ingestion to cloud data virtualization and business intelligence consumption:

![Azure Architecture Diagram](Architecture%20Diagram.jpg)

### End-to-End Execution Flow
$$\text{On-Premises SQL Server} \xrightarrow{\text{ADF + SHIR}} \text{Bronze (Raw Parquet)} \xrightarrow{\text{Databricks Serverless}} \text{Silver (Cleaned Delta)} \xrightarrow{\text{Databricks Serverless}} \text{Gold (Star Schema Delta)} \xrightarrow{\text{Synapse Serverless SQL}} \text{Power BI}$$

1. **On-Premises SQL Server:** Transactional OLTP records reside across relational tables in the `SalesLT` schema.
2. **Self-Hosted Integration Runtime (SHIR):** Functions as an encrypted outbound communication gateway between the local on-premises network and Azure over port 443 without opening inbound firewall ports.
3. **Azure Data Factory (`altech-df`):** Executes metadata-driven pipeline orchestration to dynamically discover tables and stage raw records into ADLS Gen2 as Parquet.
4. **Bronze Layer (`altechsg/bronze`):** Stores append-only, raw Parquet snapshots preserving source schema fidelity.
5. **Databricks Serverless Compute (`altech-databricks`):** Authenticates to ADLS Gen2 via the **Access Connector for Azure Databricks** (`databricks-access-connector`) using Managed Identity.
6. **Silver Layer (`altechsg/silver`):** Stores cleansed, typed, standardized, and deduplicated records in **Delta Lake** format.
7. **Gold Layer (`altechsg/gold`):** Stores an analytics-optimized dimensional **Star Schema** (`DimCustomer`, `DimProduct`, `FactSales`) in **Delta Lake** format.
8. **Synapse Serverless SQL (`altech-synapse`):** Virtualizes Gold Delta tables using `OPENROWSET` views inside the `gold_db` logical database without requiring dedicated cluster compute.
9. **Power BI Desktop:** Connects via Direct/Import query to Synapse Serverless SQL to model relationships, compute business DAX measures, and present visual sales analytics.

---

## 2. Infrastructure & Azure Resource Directory

All project cloud resources were provisioned within resource group **`altech-rg`** in Microsoft Azure:

| Resource Name | Azure Service Type | Purpose / Function in Solution | Configuration & Security Details |
| :--- | :--- | :--- | :--- |
| **`altech-rg`** | Resource Group | Unified logical boundary and RBAC container | Centralized region deployment |
| **`altech-df`** | Azure Data Factory (V2) | Hybrid ETL/ELT pipeline orchestrator | Integrated with local SHIR, Azure Key Vault, and GitHub repository |
| **`altechsg`** | Storage Account (ADLS Gen2) | Scalable hierarchical cloud data lake | Hierarchical Namespace (HNS) Enabled; contains `bronze`, `silver`, and `gold` containers |
| **`databricks-access-connector`** | Access Connector for Databricks | Managed Identity bridge for storage authentication | Assigned `Storage Blob Data Contributor` role on `altechsg` |
| **`altech-databricks`** | Azure Databricks Service | Distributed PySpark compute engine | Runs on **Serverless Compute** |
| **`altech-synapse`** | Synapse Analytics Workspace | Serverless SQL data virtualization and serving | Assigned `Storage Blob Data Contributor` role on `altechsg` |
| **`altech-keyvault`** | Azure Key Vault | Centralized cryptographic secrets management | Stores SQL Server credentials, storage keys, and API tokens |

---

## 3. Engineering Modernization (Current Standards vs. Legacy Tutorials)

Earlier video walkthroughs and legacy guides often rely on outdated Azure patterns. This implementation updates every phase of the architecture to align with modern cloud data engineering standards:

| Architectural Component | Legacy Tutorial Pattern | Our Modern Implementation | Technical Justification |
| :--- | :--- | :--- | :--- |
| **Databricks Compute** | Standard All-Purpose Clusters (Single Node VM with auto-terminate) | **Azure Databricks Serverless Compute** | Eliminates 5–7 minute cluster boot times; executes instantly with per-second resource billing and automated scaling. |
| **Lakehouse Storage Access** | Hardcoded Storage Account Keys or DBFS directory mounts (`/mnt/`) | **Azure Access Connector for Databricks** (`databricks-access-connector`) | Uses managed identity authentication and direct Azure Blob Filesystem (`abfss://`) URIs, eliminating persistent credentials. |
| **ADF Pipeline Triggering** | Direct Databricks Linked Service running interactive clusters | **ADF Web Activity calling Databricks Jobs 2.1 API** | Decouples orchestration from cluster state; enables asynchronous execution and automated callback monitoring via `/api/2.1/jobs/run-now`. |
| **Serving Layer Architecture**| Dedicated SQL Pools (Provisioned Data Warehouses) | **Synapse Serverless SQL (`OPENROWSET`)** | Zero idle infrastructure expenses; provides a cost-effective, pay-per-query virtualized relational layer directly over Delta Lake files. |
| **Source Control & Git CI/CD** | Committing live configurations with hardcoded tokens | **ARM Parameterization & Sanitization** | Mitigates GitHub Secret Scanning and Push Protection blocks by abstracting sensitive tokens (`dapi...`) into Key Vault placeholders. |

---

## 4. Phase 1: Source Dataset Download & On-Premises SQL Server Setup

### 1. Dataset Source & Download
The project uses the Microsoft **AdventureWorksLT** database (Lightweight OLTP relational schema representing a bicycle and apparel manufacturing and retail company):
* **Official Microsoft Download URL:** [Microsoft SQL Server Samples - Releases](https://github.com/Microsoft/sql-server-samples/releases/download/adventureworks/AdventureWorksLT2019.bak)
* **File Name:** `AdventureWorksLT2019.bak` (or `AdventureWorksLT2022.bak`)
* **Format:** Microsoft SQL Server Native Database Backup file (`.bak`).

### 2. Loading the Backup into SQL Server via SSMS
1. Open **SQL Server Management Studio (SSMS)** and connect to the local SQL Server instance.
2. In **Object Explorer**, right-click **Databases** and select **Restore Database...**.
3. In the **Source** section, select **Device**, click the browse (`...`) button, click **Add**, and navigate to the directory containing `AdventureWorksLT2019.bak`.
4. Set the **Destination Database** name to `AdventureWorksLT`.
5. Under **Files**, verify that the data (`.mdf`) and log (`.ldf`) restore paths match your local SQL Server storage directories.
6. Click **OK** to execute the restore. Verify that the tables under the `SalesLT` schema appear in Object Explorer:
   * `SalesLT.Customer`
   * `SalesLT.CustomerAddress`
   * `SalesLT.Address`
   * `SalesLT.Product`
   * `SalesLT.ProductCategory`
   * `SalesLT.ProductModel`
   * `SalesLT.ProductDescription`
   * `SalesLT.ProductModelProductDescription`
   * `SalesLT.SalesOrderHeader`
   * `SalesLT.SalesOrderDetail`

### 3. Network and Security Configuration for Gateway Ingestion
1. **Enable Mixed Authentication Mode:**
   * In SSMS, right-click the server instance root node > **Properties** > **Security**.
   * Under **Server authentication**, select **SQL Server and Windows Authentication mode**.
   * Click **OK** and restart the SQL Server service.
2. **Create Service User:**
   * Expand **Security** > right-click **Logins** > **New Login...**.
   * Login name: `adfsrvuser`.
   * Select **SQL Server authentication**, set a strong password, and uncheck **Enforce password expiration**.
   * Under **User Mapping**, check `AdventureWorksLT` and grant the database role `db_datareader`.
3. **Enable TCP/IP Networking:**
   * Open **SQL Server Configuration Manager**.
   * Expand **SQL Server Network Configuration** > **Protocols for SQLEXPRESS**.
   * Right-click **TCP/IP** and select **Enable**.
   * Open **TCP/IP Properties** > **IP Addresses** tab > scroll down to **IPAll** > set **TCP Port** to `1433` (leave **TCP Dynamic Ports** blank).
   * Restart the SQL Server instance service via Windows Services (`services.msc`).

---

## 5. Phase 2: Hybrid Ingestion with Azure Data Factory

### 1. Provisioning the Self-Hosted Integration Runtime (SHIR)
Because the SQL Server is hosted locally behind a firewall, an Azure-hosted runtime cannot reach it directly:
1. In ADF Studio, navigate to **Manage** > **Integration runtimes** > click **+ New**.
2. Select **Azure, Self-Hosted** > **Self-Hosted**.
3. Name: `Shir-OnPremises-SQL`.
4. Click **Create**, then click **Option 1: Express setup** (or download the gateway manually, run `IntegrationRuntime.msi`, and register the node using Authentication Key 1).
5. Confirm that the node status in ADF changes to **Running / Connected**.

### 2. Secret Configuration in Azure Key Vault
1. In `altech-keyvault`, create the following secrets:
   * `SqlServerPassword`: Plaintext password for the `adfsrvuser` SQL account.
   * `DatabricksToken`: Databricks Personal Access Token (`dapi...`) with permission to execute jobs.
2. In ADF, create an Azure Key Vault linked service (`AzureKeyVaultAlwyn`) granting the ADF Managed Identity the **Key Vault Secrets User** role.

### 3. Linked Services Setup
* **`SqlServerOnPremLinkedService`**: Connects via `Shir-OnPremises-SQL` runtime, points to local server instance name, database `AdventureWorksLT`, user `adfsrvuser`, with password retrieved dynamically from `altech-keyvault`.
* **`AzureBlobStorageLinkedService`**: Connects to `altechsg` using system-assigned Managed Identity targeting ADLS Gen2 endpoints.

### 4. Dynamic Pipeline Design (`copy_all_tables`)
Instead of building ten separate hardcoded pipelines for each table, a single dynamic metadata-driven pipeline was built:

1. **Lookup Activity (`Get_All_SalesLT_Tables`):**
   * Source: `SqlServerOnPremLinkedService`
   * Query:
     ```sql
     SELECT TABLE_SCHEMA, TABLE_NAME
     FROM INFORMATION_SCHEMA.TABLES
     WHERE TABLE_SCHEMA = 'SalesLT' AND TABLE_TYPE = 'BASE TABLE';
     ```
2. **ForEach Activity (`ForEach_Table`):**
   * Input items expression: `@activity('Get_All_SalesLT_Tables').output.value`
   * Execution mode: Concurrent (Batch execution enabled).
3. **Copy Data Activity (Inside ForEach loop):**
   * **Source:** Dynamic query reading each table:
     ```sql
     SELECT * FROM [@{item().TABLE_SCHEMA}].[@{item().TABLE_NAME}]
     ```
   * **Sink:** Parquet dataset pointing to `altechsg`.
   * **File Path Expression:**
     * File system: `bronze`
     * Directory: `@concat('SalesLT/', item().TABLE_NAME)`
     * File Name: `@concat(item().TABLE_NAME, '.parquet')`
4. **Web Activity (`Trigger_Databricks_Job`):**
   * Chained to the ForEach activity upon successful completion.
   * URL: `https://<databricks-instance-id>.azuredatabricks.net/api/2.1/jobs/run-now`
   * Method: `POST`
   * Headers:
     * `Authorization`: `Bearer @{activity('Get_Token_From_KeyVault').output.value}`
     * `Content-Type`: `application/json`
   * Body:
     ```json
     {
       "job_id": 123456789012345
     }
     ```

---

## 6. Phase 3: Lakehouse Medallion Architecture on Azure Databricks

Data processing is handled by PySpark notebooks executing on **Databricks Serverless Compute**, eliminating traditional virtual machine boot latency.

### 1. Storage Authentication Configuration
Databricks authenticates to ADLS Gen2 using the **Azure Access Connector for Databricks** with direct ABFSS URIs:
```python
storage_account_name = "altechsg"
bronze_base_path = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/SalesLT"
silver_base_path = f"abfss://silver@{storage_account_name}.dfs.core.windows.net/SalesLT"
gold_base_path   = f"abfss://gold@{storage_account_name}.dfs.core.windows.net/SalesLT"
```

---

### 2. Bronze to Silver Transformation (`1_Bronze_to_Silver.py`)
Cleanses raw extracts, strips metadata, standardizes schema types, and writes to Delta format:

```python
from pyspark.sql.functions import col, current_timestamp, to_date

tables = [
    "Customer", "CustomerAddress", "Address", "Product", 
    "ProductCategory", "ProductModel", "SalesOrderHeader", "SalesOrderDetail"
]

for table in tables:
    input_path = f"{bronze_base_path}/{table}/{table}.parquet"
    output_path = f"{silver_base_path}/{table}"
    
    # Read raw parquet from Bronze
    df = spark.read.format("parquet").load(input_path)
    
    # Drop internal system tracking columns
    cols_to_drop = [c for c in ["rowguid", "ModifiedDate"] if c in df.columns]
    df_cleaned = df.drop(*cols_to_drop)
    
    # Standardize column naming to snake_case
    for column_name in df_cleaned.columns:
        clean_name = column_name.lower()
        df_cleaned = df_cleaned.withColumnRenamed(column_name, clean_name)
    
    # Add audit processing timestamp
    df_silver = df_cleaned.withColumn("ingestion_timestamp", current_timestamp())
    
    # Write to Silver as Delta Lake
    df_silver.write.format("delta").mode("overwrite").save(output_path)
    print(f"Table {table} successfully processed to Silver layer.")
```

---

### 3. Silver to Gold Dimensional Transformation (`2_Silver_to_Gold.py`)
Transforms normalized tables into an analytics-ready **Star Schema** optimized for business intelligence queries:

```python
from pyspark.sql.functions import col, concat_ws, coalesce, lit, round

# 1. Load Silver Delta Tables
customer         = spark.read.format("delta").load(f"{silver_base_path}/Customer")
customer_address = spark.read.format("delta").load(f"{silver_base_path}/CustomerAddress")
address          = spark.read.format("delta").load(f"{silver_base_path}/Address")
product          = spark.read.format("delta").load(f"{silver_base_path}/Product")
product_category = spark.read.format("delta").load(f"{silver_base_path}/ProductCategory")
product_model    = spark.read.format("delta").load(f"{silver_base_path}/ProductModel")
sales_header     = spark.read.format("delta").load(f"{silver_base_path}/SalesOrderHeader")
sales_detail     = spark.read.format("delta").load(f"{silver_base_path}/SalesOrderDetail")

# 2. Build Customer Dimension (DimCustomer)
dim_customer = customer.join(
    customer_address, customer.customerid == customer_address.customerid, "left"
).join(
    address, customer_address.addressid == address.addressid, "left"
).select(
    customer.customerid.alias("CustomerID"),
    concat_ws(" ", customer.firstname, customer.middlename, customer.lastname).alias("FullName"),
    customer.companyname.alias("CompanyName"),
    customer.emailaddress.alias("EmailAddress"),
    customer.phone.alias("Phone"),
    coalesce(address.city, lit("Unknown")).alias("City"),
    coalesce(address.stateprovince, lit("Unknown")).alias("StateProvince"),
    coalesce(address.countryregion, lit("Unknown")).alias("CountryRegion"),
    coalesce(address.postalcode, lit("Unknown")).alias("PostalCode")
).dropDuplicates(["CustomerID"])

# 3. Build Product Dimension (DimProduct)
dim_product = product.join(
    product_category, product.productcategoryid == product_category.productcategoryid, "left"
).join(
    product_model, product.productmodelid == product_model.productmodelid, "left"
).select(
    product.productid.alias("ProductID"),
    product.name.alias("ProductName"),
    product.productnumber.alias("ProductNumber"),
    product.color.alias("Color"),
    product.standardcost.alias("StandardCost"),
    product.listprice.alias("ListPrice"),
    product.size.alias("Size"),
    product.weight.alias("Weight"),
    coalesce(product_category.name, lit("Unassigned")).alias("CategoryName"),
    coalesce(product_model.name, lit("Standard")).alias("ModelName")
)

# 4. Build Sales Fact (FactSales)
fact_sales = sales_header.join(
    sales_detail, sales_header.salesorderid == sales_detail.salesorderid, "inner"
).select(
    sales_detail.salesorderdetailid.alias("SalesOrderDetailID"),
    sales_header.salesorderid.alias("SalesOrderID"),
    sales_header.orderdate.alias("OrderDate"),
    sales_header.customerid.alias("CustomerID"),
    sales_detail.productid.alias("ProductID"),
    sales_detail.orderqty.alias("OrderQty"),
    sales_detail.unitprice.alias("UnitPrice"),
    sales_detail.unitpricediscount.alias("UnitPriceDiscount"),
    sales_detail.linetotal.alias("LineTotal"),
    sales_header.taxamt.alias("TaxAmt"),
    sales_header.freight.alias("Freight"),
    sales_header.subtotal.alias("SubTotal"),
    sales_header.totaldue.alias("TotalDue")
)

# 5. Persist to Gold as Delta Lake
dim_customer.write.format("delta").mode("overwrite").save(f"{gold_base_path}/DimCustomer")
dim_product.write.format("delta").mode("overwrite").save(f"{gold_base_path}/DimProduct")
fact_sales.write.format("delta").mode("overwrite").save(f"{gold_base_path}/FactSales")
```

---

## 7. Phase 4: Data Virtualization with Azure Synapse Serverless SQL

Instead of maintaining expensive Dedicated SQL Pools, Azure Synapse Serverless SQL acts as a virtual relational layer over the data lake files.

### 1. Permission Assignment
In the Azure Portal, navigate to `altechsg` > **Access Control (IAM)** > **Add role assignment** > select **Storage Blob Data Contributor** > assign to the Synapse Workspace managed identity `altech-synapse`.

### 2. SQL DDL Execution
In Synapse Studio, connect to the **Built-in** Serverless SQL pool and execute:

```sql
-- Step 1: Create Logical Serverless Database
CREATE DATABASE gold_db;
GO

USE gold_db;
GO

-- Step 2: Virtualize DimCustomer
CREATE OR ALTER VIEW dbo.DimCustomer
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://altechsg.dfs.core.windows.net/gold/SalesLT/DimCustomer/',
    FORMAT = 'DELTA'
) AS rows;
GO

-- Step 3: Virtualize DimProduct
CREATE OR ALTER VIEW dbo.DimProduct
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://altechsg.dfs.core.windows.net/gold/SalesLT/DimProduct/',
    FORMAT = 'DELTA'
) AS rows;
GO

-- Step 4: Virtualize FactSales
CREATE OR ALTER VIEW dbo.FactSales
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://altechsg.dfs.core.windows.net/gold/SalesLT/FactSales/',
    FORMAT = 'DELTA'
) AS rows;
GO
```

---

## 8. Phase 5: Power BI Semantic Modeling & Executive Dashboard

The consumption layer connects directly to Azure Synapse Serverless SQL to provide an interactive dashboard for business stakeholders.

### 1. Connection & Data Loading
* **Connector:** Azure Synapse Analytics SQL (Serverless).
* **Server Endpoint:** `altech-synapse-ondemand.sql.azuresynapse.net`
* **Database:** `gold_db`
* **Data Connectivity Mode:** Import (or DirectQuery depending on SLA).
* **Authentication:** Microsoft Account (Entra ID).

### 2. Star Schema Relationship Architecture
In Power BI Model View, relationships were established with **Single** cross-filtering direction and $1:\infty$ cardinality:

```text
       +-----------------------+           +---------------------+
       |      DimCustomer      |           |     DimProduct      |
       +-----------------------+           +---------------------+
       | CustomerID (PK)       |           | ProductID (PK)      |
       | FullName              |           | ProductName         |
       | CompanyName           |           | ProductNumber       |
       | CountryRegion         |           | CategoryName        |
       | City, PostalCode      |           | ModelName, ListPrice|
       +-----------+-----------+           +----------+----------+
                   | 1                                | 1
                   |                                  |
                   | *                                | *
       +-----------+----------------------------------+----------+
       |                       FactSales                         |
       +---------------------------------------------------------+
       | SalesOrderDetailID (PK)                                 |
       | SalesOrderID                                            |
       | CustomerID (FK)                                         |
       | ProductID (FK)                                          |
       | OrderDate                                               |
       | OrderQty                                                |
       | UnitPrice, UnitPriceDiscount                            |
       | LineTotal, TaxAmt, Freight                              |
       +---------------------------------------------------------+
```

---

### 3. Core Business DAX Calculations

* **Total Revenue:**
  ```dax
  Total Revenue = SUM(FactSales[LineTotal])
  ```
  *Format: Currency (`$#,##0.00`)*

* **Total Orders:**
  ```dax
  Total Orders = DISTINCTCOUNT(FactSales[SalesOrderID])
  ```
  *Format: Whole Number (`#,##0`)*

* **Total Units Sold:**
  ```dax
  Total Units Sold = SUM(FactSales[OrderQty])
  ```
  *Format: Whole Number (`#,##0`)*

* **Average Order Value (AOV):**
  ```dax
  Average Order Value = DIVIDE([Total Revenue], [Total Orders], 0)
  ```
  *Format: Currency (`$#,##0.00`)*

---

### 4. Dashboard Canvas Layout & Visual Architecture

The dashboard visualizes business operations across four key zones:

```text
+--------------------------------------------------------------------------------------------------+
|  ADVENTUREWORKS SALES PERFORMANCE DASHBOARD             [ Filter: Country ]  [ Filter: Date ]    |
+--------------------------------------------------------------------------------------------------+
|  [ TOTAL REVENUE ]       [ TOTAL ORDERS ]       [ UNITS SOLD ]       [ AVERAGE ORDER VALUE ]     |
|     $708.69K                   32                   2,087                    $22.15K             |
+---------------------------------------------------+----------------------------------------------+
|  REVENUE BY PRODUCT CATEGORY                      |  SALES BY GEOGRAPHIC REGION                  |
|  Touring Bikes   ████████████████████ $260.8K     |                                              |
|  Road Bikes      █████████████████    $230.1K     |                 United States                |
|  Mountain Bikes  ██████████           $140.2K     |                   $425.98K                   |
|  Mountain Frames ████                 $ 45.0K     |                   (60.11%)                   |
|  Road Frames     ██                   $ 20.2K     |               [Donut Chart]                  |
|  Touring Frames  █                    $  8.5K     |                                              |
|  Jerseys         ▏                    $  2.8K     |                 United Kingdom               |
|  Vests           ▏                    $  1.1K     |                   $282.71K                   |
|                                                   |                   (39.89%)                   |
+---------------------------------------------------+----------------------------------------------+
|  TOP 10 PRODUCTS BY REVENUE                       |  TOP SPENDERS MATRIX                         |
|  Touring-1000 Blue, 60     ████████ $44.0K        |  Customer Name        Revenue     Orders     |
|  Touring-1000 Blue, 46     ███████  $38.2K        |  Christopher R. Beck  $520,252.72   15       |
|  Road-250 Black, 44        ██████   $33.5K        |  Rosmarie Carroll     $ 25,600.50    3       |
|  Road-250 Black, 48        ██████   $33.1K        |  Donald L. Blanton    $ 22,410.00    2       |
|  Touring-1000 Yellow, 46   █████    $29.8K        |  Michael Bowling      $ 18,205.10    2       |
|  Mountain-200 Silver, 38   █████    $28.4K        |  Pamela O. Moreno     $ 16,980.20    2       |
|  Road-350-W Yellow, 48     ████     $24.2K        |  David J. Looney      $ 14,350.00    1       |
+---------------------------------------------------+----------------------------------------------+
```

#### Detailed Visual Specifications
1. **Executive Headline KPI Strip (Cards):**
   * **Total Revenue:** Card visual showing aggregated sales (`$708.69K`).
   * **Total Orders:** Card visual tracking transactional scale (`32` orders).
   * **Units Sold:** Card visual tracking warehouse dispatch volume (`2,087` items).
   * **Average Order Value:** Card visual highlighting order value efficiency (`$22.15K`).
2. **Category Performance (Horizontal Clustered Bar Chart):**
   * **Y-Axis:** `DimProduct[CategoryName]`
   * **X-Axis:** `[Total Revenue]`
   * Highlights product category dominance: Touring Bikes and Road Bikes generate over 69% of all revenue, while apparel lines (Jerseys, Vests) serve as cross-sell items.
3. **Regional Market Split (Donut Chart):**
   * **Legend:** `DimCustomer[CountryRegion]`
   * **Values:** `[Total Revenue]`
   * **Custom Color Palette:** Two-tone layout (United States in Red, United Kingdom in Green).
   * Displays market distribution: United States represents **$425.98K (60.11%)**, while the United Kingdom accounts for **$282.71K (39.89%)**.
4. **Top 10 Products by Revenue (Clustered Column Chart):**
   * **X-Axis:** `DimProduct[ProductName]` (Filtered via Top N = 10 by `[Total Revenue]`).
   * **Y-Axis:** `[Total Revenue]`
   * Visualizes best-selling models: *Touring-1000 Blue* and *Road-250 Black* generate the highest individual product revenue.
5. **Top Spenders Matrix (Table Visual):**
   * **Rows:** `DimCustomer[FullName]`
   * **Columns:** `[Total Revenue]`, `[Total Orders]`
   * **Data Bars:** Added to the `Total Revenue` column for inline magnitude comparisons.
   * Identifies customer concentration: Top buyer *Christopher R. Beck* generated **$520,252.72** across 15 separate purchase orders.

---

## 9. Repository Structure

```text
AdventureWorks-Azure-Data-Engineering-End-to-End-Pipeline/
├── README.md                               <-- Comprehensive project documentation
├── Architecture Diagram.jpg                <-- Visual architecture diagram
├── adf/
│   ├── ARMTemplateForFactory.json          <-- Sanitized production deployment ARM template
│   ├── ARMTemplateParametersForFactory.json<-- Deployment parameter definitions
│   └── publish_config.json                 <-- Azure Data Factory Git configurations
├── databricks/
│   ├── 1_Bronze_to_Silver.py               <-- PySpark data cleansing notebook
│   └── 2_Silver_to_Gold.py                 <-- PySpark star schema transformation notebook
├── synapse/
│   └── 01_gold_views.sql                   <-- Synapse Serverless OPENROWSET DDL scripts
└── powerbi/
    └── AdventureWorks_Sales_Analytics.pbix  <-- Power BI data model, DAX measures, and dashboard
```

---

## 10. Complete Step-by-Step Reproduction Guide

Follow these sequential steps to reproduce this pipeline in your own Azure environment:

### Step 1: Prepare Source Database
1. Download `AdventureWorksLT2019.bak` from Microsoft's GitHub repository.
2. In SSMS, restore the file to a local SQL Server instance as database `AdventureWorksLT`.
3. Configure SQL authentication, create user `adfsrvuser`, grant `db_datareader`, and verify TCP/IP port `1433` is enabled.

### Step 2: Deploy Cloud Resources
1. In the Azure Portal, create resource group `altech-rg`.
2. Provision an ADLS Gen2 Storage Account (`altechsg`) and create containers `bronze`, `silver`, and `gold`.
3. Provision Azure Key Vault (`altech-keyvault`) and add your SQL Server credentials and Databricks API token.
4. Provision Azure Data Factory (`altech-df`), Azure Databricks (`altech-databricks`), and Azure Synapse Analytics (`altech-synapse`).

### Step 3: Configure Ingestion & Orchestration
1. In ADF Studio, create a Self-Hosted Integration Runtime (`Shir-OnPremises-SQL`) and install the gateway software on your local database host machine.
2. Import the ARM template files from `/adf` or configure the `copy_all_tables` pipeline.
3. Validate and trigger the pipeline. Verify that all ten tables are extracted as Parquet files inside `altechsg/bronze/SalesLT/`.

### Step 4: Execute Lakehouse Transformations
1. In Azure Databricks, attach to Serverless compute.
2. Verify access to ADLS Gen2 using the Access Connector managed identity.
3. Import and run `/databricks/1_Bronze_to_Silver.py` to populate Delta tables in `silver/`.
4. Import and run `/databricks/2_Silver_to_Gold.py` to generate the Star Schema in `gold/`.

### Step 5: Establish Synapse Serving Layer
1. In Azure Synapse Studio, connect to the Built-in Serverless SQL pool.
2. Execute the script in `/synapse/01_gold_views.sql` to create database `gold_db` and the three serving views (`DimCustomer`, `DimProduct`, `FactSales`).
3. Query `SELECT TOP 10 * FROM gold_db.dbo.FactSales;` to confirm data accessibility.

### Step 6: Load Power BI Dashboard
1. Open Power BI Desktop.
2. Open `/powerbi/AdventureWorks_Sales_Analytics.pbix`.
3. If prompted for credentials, select **Microsoft Account**, sign in with your Azure tenant credentials, and click **Connect**.
4. In the Home ribbon, click **Refresh** to pull the latest transformed data through the serverless endpoint.
