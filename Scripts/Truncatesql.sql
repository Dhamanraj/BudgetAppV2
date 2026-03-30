-- 1. Disable checks
SET FOREIGN_KEY_CHECKS = 0;

-- 2. Clear the tables
TRUNCATE TABLE TRANSACTIONS;

-- 3. Re-enable checks
SET FOREIGN_KEY_CHECKS = 1;