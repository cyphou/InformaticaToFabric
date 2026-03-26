-- Unity Catalog Permissions for catalog: main
-- Generated: 2026-03-26 17:28:15 UTC

-- Catalog-level grants
GRANT USE CATALOG ON CATALOG main TO `data-engineers`;
GRANT USE CATALOG ON CATALOG main TO `data-analysts`;

-- Schema: main.gold
GRANT USE SCHEMA ON SCHEMA main.gold TO `data-engineers`;
GRANT USE SCHEMA ON SCHEMA main.gold TO `data-analysts`;
GRANT CREATE TABLE ON SCHEMA main.gold TO `data-engineers`;

-- Schema: main.silver
GRANT USE SCHEMA ON SCHEMA main.silver TO `data-engineers`;
GRANT USE SCHEMA ON SCHEMA main.silver TO `data-analysts`;
GRANT CREATE TABLE ON SCHEMA main.silver TO `data-engineers`;

GRANT SELECT ON TABLE main.gold.agg_orders_by_customer TO `data-analysts`;
GRANT MODIFY ON TABLE main.gold.agg_orders_by_customer TO `data-engineers`;
GRANT SELECT ON TABLE main.gold.fact_orders TO `data-analysts`;
GRANT MODIFY ON TABLE main.gold.fact_orders TO `data-engineers`;
GRANT SELECT ON TABLE main.gold.fact_txn_high TO `data-analysts`;
GRANT MODIFY ON TABLE main.gold.fact_txn_high TO `data-engineers`;
GRANT SELECT ON TABLE main.gold.fact_txn_low TO `data-analysts`;
GRANT MODIFY ON TABLE main.gold.fact_txn_low TO `data-engineers`;
GRANT SELECT ON TABLE main.gold.fact_txn_tags TO `data-analysts`;
GRANT MODIFY ON TABLE main.gold.fact_txn_tags TO `data-engineers`;
GRANT SELECT ON TABLE main.gold.tgt_lh_contacts TO `data-analysts`;
GRANT MODIFY ON TABLE main.gold.tgt_lh_contacts TO `data-engineers`;
GRANT SELECT ON TABLE main.silver.dim_customer TO `data-analysts`;
GRANT MODIFY ON TABLE main.silver.dim_customer TO `data-engineers`;
GRANT SELECT ON TABLE main.silver.dim_employee TO `data-analysts`;
GRANT MODIFY ON TABLE main.silver.dim_employee TO `data-engineers`;
GRANT SELECT ON TABLE main.silver.dim_inventory TO `data-analysts`;
GRANT MODIFY ON TABLE main.silver.dim_inventory TO `data-engineers`;
GRANT SELECT ON TABLE main.silver.lakehouse_bronze TO `data-analysts`;
GRANT MODIFY ON TABLE main.silver.lakehouse_bronze TO `data-engineers`;
GRANT SELECT ON TABLE main.silver.lakehouse_silver TO `data-analysts`;
GRANT MODIFY ON TABLE main.silver.lakehouse_silver TO `data-engineers`;
GRANT SELECT ON TABLE main.silver.tgt_accounts TO `data-analysts`;
GRANT MODIFY ON TABLE main.silver.tgt_accounts TO `data-engineers`;

-- Function grants for UDFs
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA main.silver TO `data-engineers`;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA main.gold TO `data-engineers`;
