# Enterprise Azure Data Engineering Pipeline: AdventureWorks Sales Analytics

[![Azure Data Factory](https://img.shields.io/badge/Orchestration-Azure%20Data%20Factory-blue?logo=microsoftazure)](https://azure.microsoft.com/services/data-factory/)
[![Azure Databricks](https://img.shields.io/badge/Compute-Databricks%20Serverless-FF3621?logo=databricks)](https://databricks.com/)
[![Azure Synapse](https://img.shields.io/badge/Serving-Synapse%20Serverless%20SQL-0089D6?logo=microsoftazure)](https://azure.microsoft.com/services/synapse-analytics/)
[![Power BI](https://img.shields.io/badge/Analytics-Power%20BI%20Desktop-F2C811?logo=powerbi)](https://powerbi.microsoft.com/)

An end-to-end, cloud-native ETL/ELT pipeline and dimensional analytics platform. This project ingests transactional OLTP data from an on-premises Microsoft SQL Server into **Azure Data Lake Storage (ADLS Gen2)** via a **Self-Hosted Integration Runtime (SHIR)**, implements a **Medallion Lakehouse Architecture (Bronze $\rightarrow$ Silver $\rightarrow$ Gold)** using **Azure Databricks Serverless Compute**, exposes dimensional models via **Azure Synapse Serverless SQL**, and visualizes executive KPIs in **Power BI**.

---

## Architecture Diagram

![Azure Architecture Diagram](Architecture%20Diagram.jpg)

### End-to-End Data Flow
$$\text{On-Prem SQL Server} \xrightarrow{\text{SHIR / ADF}} \text{Bronze (Raw Parquet)} \xrightarrow{\text{Databricks}} \text{Silver (Cleaned Delta)} \xrightarrow{\text{Databricks}} \text{Gold (Star Schema)} \xrightarrow{\text{Synapse SQL}} \text{Power BI}$$

---

## Executive Summary & Tech Stack

| Domain | Azure Technology | Implementation Purpose |
| :--- | :--- | :--- |
| **Source Data** | Microsoft SQL Server Express | On-premises OLTP database hosting `AdventureWorksLT` |
| **Hybrid Ingestion** | Azure Data Factory (`altech-df`) | Parameterized orchestration utilizing Self-Hosted Integration Runtime |
| **Data Lakehouse** | ADLS Gen2 (`altechsg`) | Hierarchical storage tier hosting `bronze/`, `silver/`, and `gold/` containers |
| **Distributed Compute** | Azure Databricks (`altech-databricks`) | Serverless PySpark engine executing Medallion curation logic |
| **Data Serving Layer** | Synapse Analytics (`altech-synapse`) | Cost-optimized Serverless SQL views over Gold Delta tables (`OPENROWSET`) |
| **BI & Analytics** | Power BI Desktop | Executive reporting, Star Schema semantic modeling, and DAX KPIs |
| **Security & Secrets** | Azure Key Vault (`altech-keyvault`) & Entra ID | RBAC, managed identities, and secure credential storage |

---

## Modern Adaptations (Current Best Practices vs. Legacy Tutorials)

Earlier tutorials and walkthroughs often rely on legacy tools and unoptimized setups. This project adapts the pipeline to adhere to modern cloud-engineering standards:

| Component / Pattern | Legacy Tutorial Approach | Our Modern Implementation | Technical Justification |
| :--- | :--- | :--- | :--- |
| **Databricks Compute** | Standard All-Purpose Clusters (Single Node VM) | **Databricks Serverless Compute** | Eliminates 5–7 min spin-up cold starts; scales instantaneously with per-second billing. |
| **Lakehouse Storage Access** | Hardcoded Storage Account Keys / DBFS mounts (`/mnt/`) | **Azure Access Connector for Databricks** (`databricks-access-connector`) | Uses managed identities and native ABFSS endpoints (`abfss://container@storage...`) instead of vulnerable shared keys. |
| **ADF-to-Databricks Trigger** | Legacy linked service invoking interactive clusters | **ADF Web Activity calling Databricks Jobs 2.1 API** | Decouples orchestration from cluster state; triggers serverless jobs on demand. |
| **SQL Serving Layer** | Dedicated SQL Pools (Provisioned Data Warehouses) | **Synapse Serverless SQL (`OPENROWSET`)** | Zero idle infrastructure costs; executes pay-per-query ad-hoc SQL over raw Delta Parquet. |
| **Version Control & CI/CD** | Committing cleartext tokens in source templates | **ARM Sanitization & Parameterization** | Hardens repository against GitHub Secret Scanning alerts by abstracting sensitive tokens into Key Vault placeholders. |

---

## Step-by-Step Implementation Guide

### 1. Source Database Acquisition & On-Premises Configuration
1. **Download:** Downloaded the official Microsoft OLTP database backup file (`AdventureWorksLT2019.bak`) from the official [Microsoft SQL Server Samples repository](https://github.com/Microsoft/sql-server-samples/releases/).
2. **Restore Database:** Connected to the local SQL Server instance using **SQL Server Management Studio (SSMS)**, created a database restore task targeting `AdventureWorksLT`, and restored all `SalesLT` transactional schemas.
3. **Database Security & Network Setup:**
   * Enabled **SQL Server and Windows Authentication mode**.
   * Created a dedicated service account user with `db_datareader` rights.
   * Enabled **TCP/IP** protocol on port `1433` via **SQL Server Configuration Manager** to accept incoming connection requests from the integration gateway.

---

### 2. Hybrid Ingestion with Azure Data Factory
1. **Self-Hosted Integration Runtime (SHIR):** Provisioned a SHIR inside `altech-df` and installed the client gateway software on the on-premises database host machine, establishing an encrypted outbound tunnel over HTTPS (Port 443) without opening inbound firewall ports.
2. **Key Vault Integration:** Configured `altech-keyvault` to manage SQL passwords and storage secrets. Linked services referenced secrets without hardcoding plaintext values.
3. **Dynamic Orchestration Pipeline (`copy_all_tables`):**
   * **Lookup Activity:** Queried `INFORMATION_SCHEMA.TABLES` to dynamically discover all transactional tables under the `SalesLT` schema.
   * **ForEach Activity:** Iterated over table metadata concurrently.
   * **Copy Activity:** Read each relational table and partitioned the raw data as Parquet files into `abfss://bronze@altechsg.dfs.core.windows.net/SalesLT/<TableName>/<TableName>.parquet`.
   * **Web Activity:** Made an authenticated REST call to `/api/2.1/jobs/run-now` to trigger the transformation workflow upon ingestion completion.

---

### 3. Lakehouse Medallion Transformation (Azure Databricks)
Transformations were executed in PySpark on **Databricks Serverless Compute**:

1. **Bronze to Silver (`1_Bronze_to_Silver.py`):**
   * Ingested raw Parquet files from the `bronze` container.
   * Standardized datetime fields, cast numerical columns, handled null anomalies, and trimmed system tracking columns.
   * Converted column naming conventions from PascalCase to snake_case.
   * Written to the `silver` storage container in **Delta Lake format**.

2. **Silver to Gold (`2_Silver_to_Gold.py`):**
   * Converted normalized OLTP relational tables into an analytics-optimized **Star Schema**:
     * **`DimCustomer`**: Denormalized customer profile data, joining address records and constructing primary location metrics.
     * **`DimProduct`**: Enriched product records with product category and model parent descriptions.
     * **`FactSales`**: Denormalized `SalesOrderHeader` and `SalesOrderDetail` lines, aggregating order quantities, line item totals, tax values, and freight costs.
   * Written to `abfss://gold@altechsg.dfs.core.windows.net/SalesLT/` as optimized Delta Lake tables.

---

### 4. Serving Layer Virtualization (Azure Synapse Serverless)
1. **RBAC Delegation:** Assigned the **Storage Blob Data Contributor** role to the Synapse managed identity over storage account `altechsg`.
2. **Serverless Logical Schema:** Created a serverless reporting database:
   ```sql
   CREATE DATABASE gold_db;

