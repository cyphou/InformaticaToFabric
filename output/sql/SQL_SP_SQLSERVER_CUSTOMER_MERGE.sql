-- ============================================================================
-- Converted from: C:\Users\pidoudet\OneDrive - Microsoft\Boulot\PBI SME\OracleToPostgre\InformaticaToDBFabric\input\sql\SP_SQLSERVER_CUSTOMER_MERGE.sql
-- DB Type: SQLSERVER
-- Conversion: SQL Server → Spark SQL
-- Date: 2026-04-02
-- Agent: sql-migration (automated)
-- ============================================================================
-- Review all TODO comments before deploying to Fabric.
-- ============================================================================

-- ============================================================
-- Stored Procedure: SP_SQLSERVER_CUSTOMER_MERGE
-- Source DB: SQL Server 2019+
-- Purpose: Customer dimension merge with SQL Server specifics
-- Tests: GETDATE, ISNULL, CONVERT, TOP, NOLOCK, -- TODO: CROSS APPLY → LATERAL VIEW EXPLODE or lateral join,
--        STRING_AGG, TRY_CONVERT, IIF, FORMAT, MERGE,
--        OUTPUT clause, @@ROWCOUNT, RAISERROR, TRY/CATCH
-- ============================================================

BEGIN TRY

    -- Step 1: Staging with TOP and NOLOCK
    SELECT /* TOP 50000 → add LIMIT 50000 at end */
        c.customer_id,
        COALESCE(c.first_name, 'N/A') AS first_name,
        COALESCE(c.last_name, 'N/A') AS last_name,
        c.email,
        CASE WHEN c.status = 'A' THEN 'Active' ELSE IIF(c.status = 'I', 'Inactive', 'Unknown' END) AS status_label,
        CONVERT(VARCHAR(10), c.registration_date, 120) AS reg_date_str,
        TRY_CONVERT(DECIMAL(15,2), c.credit_limit) AS credit_limit,
        FORMAT(c.registration_date, 'yyyy-MM-dd HH:mm:ss') AS reg_formatted,
        current_timestamp() AS load_timestamp
    INTO temp_stg_customers  -- TODO: use createOrReplaceTempView
    FROM dbo.customers c 
    WHERE c.status != 'D'
      AND c.registration_date >= DATEADD(YEAR, -2, current_timestamp())
    ORDER BY c.customer_id;

    -- Step 2: Enrich with -- TODO: CROSS APPLY → LATERAL VIEW EXPLODE or lateral join and STRING_AGG
    SELECT
        s.customer_id,
        s.first_name,
        s.last_name,
        s.email,
        s.status_label,
        s.credit_limit,
        addr.full_address,
        tags.all_tags
    INTO temp_enriched_customers  -- TODO: use createOrReplaceTempView
    FROM temp_stg_customers  -- TODO: use createOrReplaceTempView s
    -- TODO: CROSS APPLY → LATERAL VIEW EXPLODE or lateral join (
        SELECT /* TOP 1 → add LIMIT 1 at end */
            CONCAT(a.street, ', ', a.city, ', ', a.state, ' ', a.zip) AS full_address
        FROM dbo.addresses a 
        WHERE a.customer_id = s.customer_id
          AND a.is_primary = 1
    ) addr
    -- TODO: OUTER APPLY → LATERAL VIEW OUTER EXPLODE or left lateral join (
        SELECT CONCAT_WS(', ', COLLECT_LIST(t.tag_name)) WITHIN GROUP (ORDER BY t.tag_name) AS all_tags
        FROM dbo.customer_tags t 
        WHERE t.customer_id = s.customer_id
    ) tags;

    -- Step 3: MERGE into dimension
    MERGE dbo.dim_customer AS tgt
    USING temp_enriched_customers  -- TODO: use createOrReplaceTempView AS src
        ON tgt.customer_id = src.customer_id
    WHEN MATCHED AND (
        tgt.email != src.email
        OR COALESCE(tgt.credit_limit, 0) != COALESCE(src.credit_limit, 0)
        OR COALESCE(tgt.status_label, '') != COALESCE(src.status_label, '')
    ) THEN
        UPDATE SET
            tgt.email = src.email,
            tgt.status_label = src.status_label,
            tgt.credit_limit = src.credit_limit,
            tgt.full_address = src.full_address,
            tgt.all_tags = src.all_tags,
            tgt.updated_at = current_timestamp()
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (customer_id, first_name, last_name, email, status_label,
                credit_limit, full_address, all_tags, created_at, updated_at)
        VALUES (src.customer_id, src.first_name, src.last_name, src.email,
                src.status_label, src.credit_limit, src.full_address,
                src.all_tags, current_timestamp(), current_timestamp())
    OUTPUT
        $action AS merge_action,
        INSERTED.customer_id,
        INSERTED.updated_at;

    PRINT 'Rows affected: ' + CAST(@@ROWCOUNT AS VARCHAR(20));

    -- Step 4: Cleanup
    DROP TABLE IF EXISTS temp_stg_customers  -- TODO: use createOrReplaceTempView;
    DROP TABLE IF EXISTS temp_enriched_customers  -- TODO: use createOrReplaceTempView;

END TRY
BEGIN CATCH
    DECLARE @ErrorMessage STRING = ERROR_MESSAGE();
    DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
    RAISERROR(@ErrorMessage, @ErrorSeverity, 1);
END CATCH
