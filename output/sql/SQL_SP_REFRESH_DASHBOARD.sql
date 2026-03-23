-- ============================================================================
-- Converted from: C:\Users\pidoudet\OneDrive - Microsoft\Boulot\PBI SME\OracleToPostgre\InformaticaToDBFabric\input\sql\SP_REFRESH_DASHBOARD.sql
-- DB Type: SQLSERVER
-- Conversion: SQL Server → Spark SQL
-- Date: 2026-03-23
-- Agent: sql-migration (automated)
-- ============================================================================
-- Review all TODO comments before deploying to Fabric.
-- ============================================================================

-- ============================================================
-- Stored Procedure: SP_REFRESH_DASHBOARD
-- Source DB: SQL Server (MSSQL)
-- Purpose: Refresh dashboard summary tables after nightly load
-- Tests: SQL Server pattern detection, T-SQL constructs
-- ============================================================

CREATE PROCEDURE [dbo].[SP_REFRESH_DASHBOARD]
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @LoadDate TIMESTAMP = current_timestamp();
    DECLARE @RowCount INT = 0;
    DECLARE @TotalRevenue MONEY = 0;

    -- Step 1: Truncate staging
    TRUNCATE TABLE dbo.STG_DASHBOARD_SUMMARY;

    -- Step 2: Calculate daily metrics using CTEs
    ;WITH DailyMetrics AS (
        SELECT
            CAST(OrderDate AS DATE) AS OrderDay,
            COUNT(DISTINCT OrderID) AS OrderCount,
            SUM(COALESCE(Quantity, 0) * COALESCE(UnitPrice, 0)) AS Revenue,
            AVG(CAST(Quantity AS DECIMAL(10,2))) AS AvgQty,
            CONCAT_WS(', ', COLLECT_LIST(CAST(Channel AS STRING))) AS Channels
        FROM dbo.Orders 
        WHERE OrderStatus != 'CANCELLED'
          AND OrderDate >= DATEADD(DAY, -30, @LoadDate)
        GROUP BY CAST(OrderDate AS DATE)
    ),
    CustomerSegments AS (
        SELECT
            CustomerID,
            COUNT(DISTINCT OrderID) AS TotalOrders,
            SUM(CONVERT(DECIMAL(18,2), Quantity * UnitPrice)) AS LifetimeValue,
            CASE WHEN COUNT(DISTINCT OrderID) >= 10 THEN 'Platinum' ELSE IIF(COUNT(DISTINCT OrderID END >= 5, 'Gold',
                    CASE WHEN COUNT(DISTINCT OrderID) >= 2 THEN 'Silver' ELSE 'Bronze' END)) AS Segment,
            DATEDIFF(DAY, MIN(OrderDate), MAX(OrderDate)) AS CustomerLifespanDays
        FROM dbo.Orders 
        WHERE OrderStatus != 'CANCELLED'
        GROUP BY CustomerID
    )
    INSERT INTO dbo.STG_DASHBOARD_SUMMARY (
        ReportDate, OrderDay, OrderCount, Revenue, AvgQuantity,
        ChannelList, SegmentCounts, LoadTimestamp
    )
    SELECT
        @LoadDate,
        dm.OrderDay,
        dm.OrderCount,
        dm.Revenue,
        dm.AvgQty,
        dm.Channels,
        (SELECT COUNT(*) FROM CustomerSegments WHERE Segment = 'Platinum'),
        current_timestamp()
    FROM DailyMetrics dm;

    SET @RowCount = @@ROWCOUNT;

    -- Step 3: Update top products using -- TODO: CROSS APPLY → LATERAL VIEW EXPLODE or lateral join
    INSERT INTO dbo.TopProducts (ProductID, ProductName, OrderCount, Revenue)
    SELECT
        p.ProductID,
        p.ProductName,
        oa.OrderCount,
        oa.Revenue
    FROM dbo.Products p
    -- TODO: CROSS APPLY → LATERAL VIEW EXPLODE or lateral join (
        SELECT /* TOP 1 → add LIMIT 1 at end */
            COUNT(*) AS OrderCount,
            SUM(o.Quantity * o.UnitPrice) AS Revenue
        FROM dbo.Orders o
        WHERE o.ProductID = p.ProductID
          AND o.OrderDate >= DATEADD(MONTH, -3, current_timestamp())
        GROUP BY o.ProductID
    ) oa
    WHERE oa.OrderCount >= 10;

    -- Step 4: Generate unique batch ID
    DECLARE @BatchID UNIQUEIDENTIFIER = uuid();

    -- Step 5: Log execution using IDENTITY tracking
    INSERT INTO dbo.ETL_Log (BatchID, ProcedureName, ExecutionDate, RowsAffected, Status)
    VALUES (@BatchID, 'SP_REFRESH_DASHBOARD', @LoadDate, @RowCount, 'SUCCESS');

    -- Step 6: Compute running totals with window functions
    SELECT
        CustomerID,
        OrderDate,
        LENGTH(RTRIM(CustomerName)) AS NameLength,
        LOCATE('@', Email) AS AtPosition,
        STUFF(Phone, 1, 3, '***') AS MaskedPhone,
        Revenue,
        SUM(Revenue) OVER (PARTITION BY CustomerID ORDER BY OrderDate
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS RunningTotal,
        LAG(Revenue, 1, 0) OVER (PARTITION BY CustomerID ORDER BY OrderDate) AS PrevRevenue,
        LEAD(Revenue, 1, 0) OVER (PARTITION BY CustomerID ORDER BY OrderDate) AS NextRevenue,
        ROW_NUMBER() OVER (PARTITION BY CustomerID ORDER BY OrderDate DESC) AS RowNum
    INTO temp_TempRunningTotals  -- TODO: use createOrReplaceTempView
    FROM dbo.CustomerOrders;

    -- Step 7: Use -- TODO: OUTER APPLY → LATERAL VIEW OUTER EXPLODE or left lateral join for optional address lookup
    INSERT INTO dbo.CustomerAddressSummary
    SELECT
        c.CustomerID,
        c.CustomerName,
        COALESCE(addr.FullAddress, 'No Address') AS Address,
        CAST(c.IsActive AS BOOLEAN) AS IsActive
    FROM dbo.Customers c
    -- TODO: OUTER APPLY → LATERAL VIEW OUTER EXPLODE or left lateral join (
        SELECT /* TOP 1 → add LIMIT 1 at end */
            a.Street + ', ' + a.City + ' ' + a.Zip AS FullAddress
        FROM dbo.Addresses a
        WHERE a.CustomerID = c.CustomerID
        ORDER BY a.IsPrimary DESC
    ) addr;

    -- Step 8: Execute a sub-procedure
    EXEC dbo.SP_ARCHIVE_OLD_RECORDS @CutoffDays = 365;

END
GO
