CREATE DATABASE gold_db;
GO

USE gold_db;
GO

-- Customer Dimension View
CREATE OR ALTER VIEW dbo.DimCustomer
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://altechsg.dfs.core.windows.net/gold/SalesLT/DimCustomer/',
    FORMAT = 'DELTA'
) AS rows;
GO

-- Product Dimension View
CREATE OR ALTER VIEW dbo.DimProduct
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://altechsg.dfs.core.windows.net/gold/SalesLT/DimProduct/',
    FORMAT = 'DELTA'
) AS rows;
GO

-- Sales Fact View
CREATE OR ALTER VIEW dbo.FactSales
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://altechsg.dfs.core.windows.net/gold/SalesLT/FactSales/',
    FORMAT = 'DELTA'
) AS rows;
GO
