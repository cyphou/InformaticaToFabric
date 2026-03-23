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

    DECLARE @LoadDate DATETIME = GETDATE();
    DECLARE @RowCount INT = 0;
    DECLARE @TotalRevenue MONEY = 0;

    -- Step 1: Truncate staging
    TRUNCATE TABLE dbo.STG_DASHBOARD_SUMMARY;

    -- Step 2: Calculate daily metrics using CTEs
    ;WITH DailyMetrics AS (
        SELECT
            CAST(OrderDate AS DATE) AS OrderDay,
            COUNT(DISTINCT OrderID) AS OrderCount,
            SUM(ISNULL(Quantity, 0) * ISNULL(UnitPrice, 0)) AS Revenue,
            AVG(CAST(Quantity AS DECIMAL(10,2))) AS AvgQty,
            STRING_AGG(CAST(Channel AS NVARCHAR(MAX)), ', ') AS Channels
        FROM dbo.Orders WITH (NOLOCK)
        WHERE OrderStatus != 'CANCELLED'
          AND OrderDate >= DATEADD(DAY, -30, @LoadDate)
        GROUP BY CAST(OrderDate AS DATE)
    ),
    CustomerSegments AS (
        SELECT
            CustomerID,
            COUNT(DISTINCT OrderID) AS TotalOrders,
            SUM(CONVERT(DECIMAL(18,2), Quantity * UnitPrice)) AS LifetimeValue,
            IIF(COUNT(DISTINCT OrderID) >= 10, 'Platinum',
                IIF(COUNT(DISTINCT OrderID) >= 5, 'Gold',
                    IIF(COUNT(DISTINCT OrderID) >= 2, 'Silver', 'Bronze'))) AS Segment,
            DATEDIFF(DAY, MIN(OrderDate), MAX(OrderDate)) AS CustomerLifespanDays
        FROM dbo.Orders WITH (NOLOCK)
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
        GETDATE()
    FROM DailyMetrics dm;

    SET @RowCount = @@ROWCOUNT;

    -- Step 3: Update top products using CROSS APPLY
    INSERT INTO dbo.TopProducts (ProductID, ProductName, OrderCount, Revenue)
    SELECT
        p.ProductID,
        p.ProductName,
        oa.OrderCount,
        oa.Revenue
    FROM dbo.Products p
    CROSS APPLY (
        SELECT TOP 1
            COUNT(*) AS OrderCount,
            SUM(o.Quantity * o.UnitPrice) AS Revenue
        FROM dbo.Orders o
        WHERE o.ProductID = p.ProductID
          AND o.OrderDate >= DATEADD(MONTH, -3, GETDATE())
        GROUP BY o.ProductID
    ) oa
    WHERE oa.OrderCount >= 10;

    -- Step 4: Generate unique batch ID
    DECLARE @BatchID UNIQUEIDENTIFIER = NEWID();

    -- Step 5: Log execution using IDENTITY tracking
    INSERT INTO dbo.ETL_Log (BatchID, ProcedureName, ExecutionDate, RowsAffected, Status)
    VALUES (@BatchID, 'SP_REFRESH_DASHBOARD', @LoadDate, @RowCount, 'SUCCESS');

    -- Step 6: Compute running totals with window functions
    SELECT
        CustomerID,
        OrderDate,
        LEN(CustomerName) AS NameLength,
        CHARINDEX('@', Email) AS AtPosition,
        STUFF(Phone, 1, 3, '***') AS MaskedPhone,
        Revenue,
        SUM(Revenue) OVER (PARTITION BY CustomerID ORDER BY OrderDate
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS RunningTotal,
        LAG(Revenue, 1, 0) OVER (PARTITION BY CustomerID ORDER BY OrderDate) AS PrevRevenue,
        LEAD(Revenue, 1, 0) OVER (PARTITION BY CustomerID ORDER BY OrderDate) AS NextRevenue,
        ROW_NUMBER() OVER (PARTITION BY CustomerID ORDER BY OrderDate DESC) AS RowNum
    INTO #TempRunningTotals
    FROM dbo.CustomerOrders;

    -- Step 7: Use OUTER APPLY for optional address lookup
    INSERT INTO dbo.CustomerAddressSummary
    SELECT
        c.CustomerID,
        c.CustomerName,
        ISNULL(addr.FullAddress, 'No Address') AS Address,
        CAST(c.IsActive AS BIT) AS IsActive
    FROM dbo.Customers c
    OUTER APPLY (
        SELECT TOP 1
            a.Street + ', ' + a.City + ' ' + a.Zip AS FullAddress
        FROM dbo.Addresses a
        WHERE a.CustomerID = c.CustomerID
        ORDER BY a.IsPrimary DESC
    ) addr;

    -- Step 8: Execute a sub-procedure
    EXEC dbo.SP_ARCHIVE_OLD_RECORDS @CutoffDays = 365;

END
GO
