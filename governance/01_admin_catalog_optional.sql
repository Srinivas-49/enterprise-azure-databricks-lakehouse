-- OPTIONAL ADMIN-ONLY STEP. Do NOT run in Free Edition without permission review.
-- This is an example for an isolated paid/test Unity Catalog metastore.
-- Creating new catalogs requires CREATE CATALOG privileges and a managed storage setup.
-- Do not use customer or regulated data in this portfolio.
CREATE CATALOG IF NOT EXISTS de_portfolio
COMMENT 'Synthetic data engineering portfolio; not production data';
-- If created, manually select the catalog before executing notebooks.
-- This statement is intentionally NOT used by the paused job bundle.
