CREATE  PROCEDURE `Load_WellsFargo_Debit_Statement`(
    IN inUserName VARCHAR(50),
    IN inBankName VARCHAR(50),
    IN inCARD_LAST_4 VARCHAR(4)
)
BEGIN
    DECLARE varMemberId INT;
    DECLARE varBankId INT;
    DECLARE varCardId INT;

    -- 1. VALIDATION (SCD Type 2: Fetch current active versions)
    SELECT m.MEMBER_ID INTO varMemberId FROM MEMBERS m WHERE m.FIRST_NAME = inUserName AND m.IS_CURRENT = 1 LIMIT 1;
    SELECT b.BANK_ID INTO varBankId FROM BANKS b WHERE b.BANK_NAME = inBankName AND b.IS_CURRENT = 1 LIMIT 1;
    SELECT c.CARD_ID INTO varCardId FROM CARDS c WHERE c.LAST_4 = inCARD_LAST_4 AND c.IS_CURRENT = 1 LIMIT 1;
    
    IF varMemberId IS NULL OR varBankId IS NULL OR varCardId IS NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'VALIDATION FAILED: NO ACTIVE MEMBER, BANK, OR CARD FOUND.';
    END IF;
	
    START TRANSACTION;
    
    -- 2. PHASE 1: Auto-Onboard Parent Categories (Debit Card Specific)
    INSERT INTO MCC_CATEGORY (CATG_NAME, ADDED_USER, ADDED_DATETIME)
    SELECT DISTINCT 
        src.CATG_NAME,
        'SYSTEM_AUTO',
        NOW()
    FROM (
        SELECT DISTINCT 
            CASE 
                WHEN lt.description LIKE '%ATM%' OR lt.description LIKE '%CASH WDL%' THEN 'CASH & ATM'
                WHEN lt.description LIKE '%TRANSFER%' OR lt.description LIKE '%XFER%' THEN 'TRANSFERS'
                WHEN lt.description LIKE '%SERVICE CHG%' OR lt.description LIKE '%FEE%' THEN 'BANK FEES'
                WHEN CAST(lt.amount AS DECIMAL(19,4)) > 0 THEN 'INCOME & DEPOSITS'
                ELSE 'DEBIT CARD PURCHASE' 
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
                WHEN lt.description LIKE '%ATM%' THEN 'ATM WITHDRAWAL'
                WHEN lt.description LIKE '%CASH WDL%' THEN 'TELLER WITHDRAWAL'
                WHEN lt.description LIKE '%TRANSFER%' OR lt.description LIKE '%XFER%' THEN 'ACCOUNT TRANSFER'
                WHEN lt.description LIKE '%SERVICE CHG%' OR lt.description LIKE '%FEE%' THEN 'MONTHLY SERVICE FEE'
                WHEN CAST(lt.amount AS DECIMAL(19,4)) > 0 THEN 'DIRECT DEPOSIT / CREDIT'
                ELSE 'GENERAL MERCHANDISE'
            END AS SUB_CATG_NAME,
            CASE 
                WHEN lt.description LIKE '%ATM%' OR lt.description LIKE '%CASH WDL%' THEN 'CASH & ATM'
                WHEN lt.description LIKE '%TRANSFER%' OR lt.description LIKE '%XFER%' THEN 'TRANSFERS'
                WHEN lt.description LIKE '%SERVICE CHG%' OR lt.description LIKE '%FEE%' THEN 'BANK FEES'
                WHEN CAST(lt.amount AS DECIMAL(19,4)) > 0 THEN 'INCOME & DEPOSITS'
                ELSE 'DEBIT CARD PURCHASE' 
            END AS PARENT_CATG_NAME
        FROM LND_TRANSACTIONS lt
    ) src
    JOIN (SELECT CATG_NAME, MIN(CATG_ID) AS CATG_ID FROM MCC_CATEGORY GROUP BY CATG_NAME) mc 
        ON mc.CATG_NAME = src.PARENT_CATG_NAME
    WHERE NOT EXISTS (
        SELECT 1 FROM MCC_SUB_CATEGORY msc 
        WHERE msc.SUB_CATG_NAME = src.SUB_CATG_NAME 
          AND msc.CATG_ID = mc.CATG_ID
    );
    
    -- 4. PHASE 3: Load Final Transactions
    INSERT IGNORE INTO TRANSACTIONS 
    (
        TRANSACTION_ID, TRANSACTION_DATE, TRANSACTION_TYPE, TRANSACTION_AMOUNT,
        DESCRIPTION, CATG_ID, SUB_CATG_ID, BANK_ID, CARD_ID,
        MEMBER_ID, ADDED_USER, ADDED_DATETIME    
    )
    SELECT 
        -- Deterministic Hash ID + Row Number tie-breaker
        ABS(CAST(CONV(SUBSTRING(MD5(CONCAT(TRIM(REPLACE(lt.transaction_date, '"', '')), CASE WHEN CAST(REPLACE(lt.amount, '"', '') AS DECIMAL(19,4)) > 0 THEN 'CDT' ELSE 'DBT' END, ABS(REPLACE(lt.amount, '"', '')), TRIM(REPLACE(lt.description, '"', '')), ROW_NUMBER() OVER(ORDER BY lt.transaction_date, lt.amount, lt.description))), 1, 16), 16, 10) AS SIGNED)),
        -- Flexible date parsing
        CASE 
            WHEN lt.transaction_date LIKE '%-%' THEN STR_TO_DATE(NULLIF(TRIM(REPLACE(lt.transaction_date, '"', '')), ''), "%d-%m-%Y")
            ELSE STR_TO_DATE(NULLIF(TRIM(REPLACE(lt.transaction_date, '"', '')), ''), "%m/%d/%Y")
        END,
        -- Type Logic: Positive is Credit/Deposit (CDT), Negative is Debit/Withdrawal (DBT)
        CASE 
            WHEN CAST(REPLACE(lt.amount, '"', '')  AS DECIMAL(19,4)) > 0 THEN 'CDT'
            ELSE 'DBT' 
        END,
        ABS(CAST(REPLACE(lt.amount, '"', '') AS DECIMAL(19,4))),
        UPPER(REPLACE(lt.description, '"', '')),
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
                            WHEN REPLACE(lt.description, '"', '') LIKE '%ATM%' OR lt.description LIKE '%CASH WDL%' THEN 'CASH & ATM'
                            WHEN REPLACE(lt.description, '"', '') LIKE '%TRANSFER%' OR lt.description LIKE '%XFER%' THEN 'TRANSFERS'
                            WHEN REPLACE(lt.description, '"', '') LIKE '%SERVICE CHG%' OR lt.description LIKE '%FEE%' THEN 'BANK FEES'
                            WHEN CAST(REPLACE(lt.amount, '"', '') AS DECIMAL(19,4)) > 0 THEN 'INCOME & DEPOSITS'
                            ELSE 'DEBIT CARD PURCHASE' 
                        END
    LEFT JOIN (SELECT SUB_CATG_NAME, CATG_ID, MIN(SUB_CATG_ID) AS SUB_CATG_ID FROM MCC_SUB_CATEGORY GROUP BY SUB_CATG_NAME, CATG_ID) msc 
        ON msc.SUB_CATG_NAME = CASE 
                                WHEN lt.description LIKE '%ATM%' THEN 'ATM WITHDRAWAL'
                                WHEN lt.description LIKE '%CASH WDL%' THEN 'TELLER WITHDRAWAL'
                                WHEN lt.description LIKE '%TRANSFER%' OR lt.description LIKE '%XFER%' THEN 'ACCOUNT TRANSFER'
                                WHEN lt.description LIKE '%SERVICE CHG%' OR lt.description LIKE '%FEE%' THEN 'MONTHLY SERVICE FEE'
                                WHEN CAST(REPLACE(lt.amount, '"', '') AS DECIMAL(19,4)) > 0 THEN 'DIRECT DEPOSIT / CREDIT'
                                ELSE 'GENERAL MERCHANDISE'
                            END
       AND msc.CATG_ID = mc.CATG_ID;
        
    COMMIT;
    
END ;;
