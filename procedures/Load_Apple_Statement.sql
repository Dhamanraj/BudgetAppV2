DROP PROCEDURE IF EXISTS BudgetApp.`SP_RAW_LOAD_APPLE_TRANSACTIONS`;

CREATE OR REPLACE PROCEDURE BudgetApp.`SP_RAW_LOAD_APPLE_TRANSACTIONS`(
    IN inUserName VARCHAR(50),
    IN inBankName VARCHAR(50),
    IN inCARD_LAST_4 VARCHAR(4)
)
BEGIN
    DECLARE varMemberId INT;
    DECLARE varBankId INT;
    DECLARE varCardId INT;
    DECLARE varLogId INT;
    DECLARE varRowCount INT DEFAULT 0;

    -- Error Handling
    DECLARE EXIT HANDLER FOR SQLEXCEPTION 
    BEGIN
        GET DIAGNOSTICS CONDITION 1 @p1 = RETURNED_SQLSTATE, @p2 = MESSAGE_TEXT;
        ROLLBACK;
        IF varLogId IS NOT NULL THEN
            UPDATE BudgetApp.ETL_LOG 
            SET STATUS = 'FAILED', ERROR_MSG = @p2, END_TIME = NOW() 
            WHERE LOG_ID = varLogId;
        END IF;
        RESIGNAL;
    END;

    START TRANSACTION;

    -- 1. VALIDATION (SCD Type 2: Fetch current active versions)
    SELECT m.MEMBER_ID INTO varMemberId FROM MEMBERS m WHERE m.FIRST_NAME = inUserName AND m.IS_CURRENT = 1 LIMIT 1;
    SELECT b.BANK_ID INTO varBankId FROM BANKS b WHERE b.BANK_NAME = inBankName AND b.IS_CURRENT = 1 LIMIT 1;
    SELECT c.CARD_ID INTO varCardId FROM CARDS c WHERE c.LAST_4 = inCARD_LAST_4 AND c.IS_CURRENT = 1 LIMIT 1;
    
    IF varMemberId IS NULL OR varBankId IS NULL OR varCardId IS NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'VALIDATION FAILED: NO ACTIVE MEMBER, BANK, OR CARD FOUND.';
    END IF;

    -- Initialize Audit Log
    INSERT INTO BudgetApp.ETL_LOG (PROC_NAME, BANK_ID, STATUS) 
    VALUES ('SP_RAW_LOAD_APPLE_TRANSACTIONS', varBankId, 'STARTED');
    SET varLogId = LAST_INSERT_ID();
    
    -- 2. PHASE 1: Auto-Onboard Parent Categories
    INSERT INTO MCC_CATEGORY (CATG_NAME, ADDED_USER, ADDED_DATETIME)
    SELECT DISTINCT 
        src.CATG_NAME,
        'SYSTEM_AUTO',
        NOW()
    FROM (
        SELECT DISTINCT 
            CASE 
                WHEN lt.STATEMENT_DESC = 'Payment' THEN 'CREDIT CARD PAYMENT'
                WHEN lt.STATEMENT_DESC = 'Credit'  THEN 'MERCHANT REFUND'
                ELSE 'APPLE CARD EXPENDITURE' 
            END AS CATG_NAME
        FROM LND_TRANSACTIONS lt
    ) src
    WHERE NOT EXISTS (SELECT 1 FROM MCC_CATEGORY mc WHERE mc.CATG_NAME = src.CATG_NAME);

    -- 3. PHASE 2: Auto-Onboard Sub-Categories
    INSERT INTO MCC_SUB_CATEGORY (SUB_CATG_NAME, CATG_ID, ADDED_USER, ADDED_DATETIME)
    SELECT DISTINCT 
        src.SUB_CATG_NAME,
        mc.CATG_ID, 
        'SYSTEM_AUTO', 
        NOW()
    FROM (
        SELECT DISTINCT 
            CASE 
                WHEN lt.STATEMENT_DESC = 'Payment' THEN 'CREDIT CARD PAYMENT'
                WHEN lt.STATEMENT_DESC = 'Credit'  THEN 'MERCHANT REFUND'
                ELSE UPPER(TRIM(lt.category)) 
            END AS SUB_CATG_NAME,
            CASE 
                WHEN lt.STATEMENT_DESC = 'Payment' THEN 'CREDIT CARD PAYMENT'
                WHEN lt.STATEMENT_DESC = 'Credit'  THEN 'MERCHANT REFUND'
                ELSE 'APPLE CARD EXPENDITURE' 
            END AS PARENT_CATG_NAME
        FROM LND_TRANSACTIONS lt
        WHERE lt.category IS NOT NULL OR lt.STATEMENT_DESC IN ('Payment', 'Credit')
    ) src
    JOIN (SELECT CATG_NAME, MIN(CATG_ID) AS CATG_ID FROM MCC_CATEGORY GROUP BY CATG_NAME) mc 
        ON mc.CATG_NAME = src.PARENT_CATG_NAME
    WHERE NOT EXISTS (
        SELECT 1 FROM MCC_SUB_CATEGORY msc WHERE msc.SUB_CATG_NAME = src.SUB_CATG_NAME AND msc.CATG_ID = mc.CATG_ID
    );
    
    -- 4. PHASE 3: Load Final Transactions
    INSERT IGNORE INTO TRANSACTIONS 
    (
        TRANSACTION_ID, TRANSACTION_DATE, POSTED_DATE, TRANSACTION_TYPE, TRANSACTION_AMOUNT,
        DESCRIPTION, CATG_ID, SUB_CATG_ID, BANK_ID, CARD_ID,
        MEMBER_ID, ADDED_USER, ADDED_DATETIME    
    )
    SELECT 
        -- Hash-based ID
        ABS(CAST(CONV(SUBSTRING(MD5(CONCAT(lt.transaction_date, lt.amount, lt.description,ROW_NUMBER() OVER(ORDER BY lt.transaction_date, lt.amount, lt.description))), 1, 16), 16, 10) AS SIGNED)),
        STR_TO_DATE(lt.transaction_date, "%m/%d/%Y"),
        STR_TO_DATE(lt.POSTED_DATE , "%m/%d/%Y"),
        -- Type Logic based on Statement Description
        CASE 
            WHEN lt.STATEMENT_DESC IN ('Payment', 'Credit') THEN 'CDT'
            ELSE 'DBT' 
        END,
        ABS(CAST(lt.amount AS DECIMAL(19,4))),
        UPPER(lt.description),
        mc.CATG_ID,
        msc.SUB_CATG_ID, 
        varBankId,
        varCardId,
        varMemberId,
        'DKAKKE',
        CURRENT_TIMESTAMP()
    FROM LND_TRANSACTIONS lt 
    LEFT JOIN (SELECT CATG_NAME, MIN(CATG_ID) AS CATG_ID FROM MCC_CATEGORY GROUP BY CATG_NAME) mc 
        ON mc.CATG_NAME = CASE 
                            WHEN lt.STATEMENT_DESC = 'Payment' THEN 'CREDIT CARD PAYMENT'
                            WHEN lt.STATEMENT_DESC = 'Credit'  THEN 'MERCHANT REFUND'
                            ELSE 'APPLE CARD EXPENDITURE' 
                        END
    LEFT JOIN (SELECT SUB_CATG_NAME, CATG_ID, MIN(SUB_CATG_ID) AS SUB_CATG_ID FROM MCC_SUB_CATEGORY GROUP BY SUB_CATG_NAME, CATG_ID) msc 
        ON msc.SUB_CATG_NAME = CASE 
                                WHEN lt.STATEMENT_DESC = 'Payment' THEN 'CREDIT CARD PAYMENT'
                                WHEN lt.STATEMENT_DESC = 'Credit'  THEN 'MERCHANT REFUND'
                                ELSE UPPER(TRIM(lt.category)) 
                            END
       AND msc.CATG_ID = mc.CATG_ID;
        
    SET varRowCount = ROW_COUNT();

    COMMIT;
    
    -- Finalize Log
    UPDATE BudgetApp.ETL_LOG 
    SET STATUS = 'SUCCESS', ROWS_PROCESSED = varRowCount, END_TIME = NOW() 
    WHERE LOG_ID = varLogId;
END ;;
