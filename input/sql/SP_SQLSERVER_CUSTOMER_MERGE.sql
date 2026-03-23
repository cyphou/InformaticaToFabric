-- ============================================================
-- Stored Procedure: SP_SQLSERVER_CUSTOMER_MERGE
-- Source DB: SQL Server 2019+
-- Purpose: Customer dimension merge with SQL Server specifics
-- Tests: GETDATE, ISNULL, CONVERT, TOP, NOLOCK, CROSS APPLY,
--        STRING_AGG, TRY_CONVERT, IIF, FORMAT, MERGE,
--        OUTPUT clause, @@ROWCOUNT, RAISERROR, TRY/CATCH
-- ============================================================

BEGIN TRY

    -- Step 1: Staging with TOP and NOLOCK
    SELECT TOP 50000
        c.customer_id,
        ISNULL(c.first_name, 'N/A') AS first_name,
        ISNULL(c.last_name, 'N/A') AS last_name,
        c.email,
        IIF(c.status = 'A', 'Active', IIF(c.status = 'I', 'Inactive', 'Unknown')) AS status_label,
        CONVERT(VARCHAR(10), c.registration_date, 120) AS reg_date_str,
        TRY_CONVERT(DECIMAL(15,2), c.credit_limit) AS credit_limit,
        FORMAT(c.registration_date, 'yyyy-MM-dd HH:mm:ss') AS reg_formatted,
        GETDATE() AS load_timestamp
    INTO #stg_customers
    FROM dbo.customers c WITH (NOLOCK)
    WHERE c.status != 'D'
      AND c.registration_date >= DATEADD(YEAR, -2, GETDATE())
    ORDER BY c.customer_id;

    -- Step 2: Enrich with CROSS APPLY and STRING_AGG
    SELECT
        s.customer_id,
        s.first_name,
        s.last_name,
        s.email,
        s.status_label,
        s.credit_limit,
        addr.full_address,
        tags.all_tags
    INTO #enriched_customers
    FROM #stg_customers s
    CROSS APPLY (
        SELECT TOP 1
            CONCAT(a.street, ', ', a.city, ', ', a.state, ' ', a.zip) AS full_address
        FROM dbo.addresses a WITH (NOLOCK)
        WHERE a.customer_id = s.customer_id
          AND a.is_primary = 1
    ) addr
    OUTER APPLY (
        SELECT STRING_AGG(t.tag_name, ', ') WITHIN GROUP (ORDER BY t.tag_name) AS all_tags
        FROM dbo.customer_tags t WITH (NOLOCK)
        WHERE t.customer_id = s.customer_id
    ) tags;

    -- Step 3: MERGE into dimension
    MERGE dbo.dim_customer AS tgt
    USING #enriched_customers AS src
        ON tgt.customer_id = src.customer_id
    WHEN MATCHED AND (
        tgt.email != src.email
        OR ISNULL(tgt.credit_limit, 0) != ISNULL(src.credit_limit, 0)
        OR ISNULL(tgt.status_label, '') != ISNULL(src.status_label, '')
    ) THEN
        UPDATE SET
            tgt.email = src.email,
            tgt.status_label = src.status_label,
            tgt.credit_limit = src.credit_limit,
            tgt.full_address = src.full_address,
            tgt.all_tags = src.all_tags,
            tgt.updated_at = GETDATE()
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (customer_id, first_name, last_name, email, status_label,
                credit_limit, full_address, all_tags, created_at, updated_at)
        VALUES (src.customer_id, src.first_name, src.last_name, src.email,
                src.status_label, src.credit_limit, src.full_address,
                src.all_tags, GETDATE(), GETDATE())
    OUTPUT
        $action AS merge_action,
        INSERTED.customer_id,
        INSERTED.updated_at;

    PRINT 'Rows affected: ' + CAST(@@ROWCOUNT AS VARCHAR(20));

    -- Step 4: Cleanup
    DROP TABLE IF EXISTS #stg_customers;
    DROP TABLE IF EXISTS #enriched_customers;

END TRY
BEGIN CATCH
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
    RAISERROR(@ErrorMessage, @ErrorSeverity, 1);
END CATCH
