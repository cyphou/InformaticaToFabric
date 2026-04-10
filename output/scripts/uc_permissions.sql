-- Unity Catalog Permissions for catalog: main
-- Generated: 2026-04-10 07:23:53 UTC

-- Catalog-level grants
GRANT USE CATALOG ON CATALOG main TO `data-engineers`;
GRANT USE CATALOG ON CATALOG main TO `data-analysts`;


-- Function grants for UDFs
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA main.silver TO `data-engineers`;
