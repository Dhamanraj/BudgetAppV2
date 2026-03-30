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
    INSERT IGNORE INTO MCC_CATEGORY (CATG_NAME, ADDED_USER, ADDED_DATETIME)
    SELECT DISTINCT 
        CASE 
            WHEN lt.description LIKE '%ATM%' OR lt.description LIKE '%CASH WDL%' THEN 'CASH & ATM'
            WHEN lt.description LIKE '%TRANSFER%' OR lt.description LIKE '%XFER%' THEN 'TRANSFERS'
            WHEN lt.description LIKE '%SERVICE CHG%' OR lt.description LIKE '%FEE%' THEN 'BANK FEES'
            WHEN CAST(lt.amount AS DECIMAL(19,4)) > 0 THEN 'INCOME & DEPOSITS'
            ELSE 'DEBIT CARD PURCHASE' 
        END,
        'SYSTEM_AUTO',
        NOW()
    FROM LND_TRANSACTIONS lt;

    -- 3. PHASE 2: Auto-Onboard Sub-Categories
    INSERT IGNORE INTO MCC_SUB_CATEGORY (SUB_CATG_NAME, CATG_ID, ADDED_USER, ADDED_DATETIME)
    SELECT DISTINCT 
        CASE 
            WHEN lt.description LIKE '%ATM%' THEN 'ATM WITHDRAWAL'
            WHEN lt.description LIKE '%CASH WDL%' THEN 'TELLER WITHDRAWAL'
            WHEN lt.description LIKE '%TRANSFER%' OR lt.description LIKE '%XFER%' THEN 'ACCOUNT TRANSFER'
            WHEN lt.description LIKE '%SERVICE CHG%' OR lt.description LIKE '%FEE%' THEN 'MONTHLY SERVICE FEE'
            WHEN CAST(lt.amount AS DECIMAL(19,4)) > 0 THEN 'DIRECT DEPOSIT / CREDIT'
            ELSE 'GENERAL MERCHANDISE' -- Default for POS Purchases
        END,
        mc.CATG_ID, 
        'SYSTEM_AUTO', 
        NOW()
    FROM LND_TRANSACTIONS lt
    JOIN MCC_CATEGORY mc 
        ON mc.CATG_NAME = CASE 
                            WHEN lt.description LIKE '%ATM%' OR lt.description LIKE '%CASH WDL%' THEN 'CASH & ATM'
                            WHEN lt.description LIKE '%TRANSFER%' OR lt.description LIKE '%XFER%' THEN 'TRANSFERS'
                            WHEN lt.description LIKE '%SERVICE CHG%' OR lt.description LIKE '%FEE%' THEN 'BANK FEES'
                            WHEN CAST(lt.amount AS DECIMAL(19,4)) > 0 THEN 'INCOME & DEPOSITS'
                            ELSE 'DEBIT CARD PURCHASE' 
                        END;
    
    -- 4. PHASE 3: Load Final Transactions
    INSERT IGNORE INTO TRANSACTIONS 
    (
        TRANSACTION_ID, TRANSACTION_DATE, TRANSACTION_TYPE, TRANSACTION_AMOUNT,
        DESCRIPTION, CATG_ID, SUB_CATG_ID, BANK_ID, CARD_ID,
        MEMBER_ID, ADDED_USER, ADDED_DATETIME    
    )
    SELECT 
        ABS(CAST(CONV(SUBSTRING(MD5(CONCAT(lt.transaction_date, lt.amount, lt.description)), 1, 16), 16, 10) AS SIGNED)),
        STR_TO_DATE(lt.transaction_date, "%m/%d/%Y"),
        -- Type Logic: Positive is Credit/Deposit (CDT), Negative is Debit/Withdrawal (DBT)
        CASE 
            WHEN CAST(lt.amount AS DECIMAL(19,4)) > 0 THEN 'CDT'
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
    LEFT JOIN MCC_CATEGORY mc 
        ON mc.CATG_NAME = CASE 
                            WHEN lt.description LIKE '%ATM%' OR lt.description LIKE '%CASH WDL%' THEN 'CASH & ATM'
                            WHEN lt.description LIKE '%TRANSFER%' OR lt.description LIKE '%XFER%' THEN 'TRANSFERS'
                            WHEN lt.description LIKE '%SERVICE CHG%' OR lt.description LIKE '%FEE%' THEN 'BANK FEES'
                            WHEN CAST(lt.amount AS DECIMAL(19,4)) > 0 THEN 'INCOME & DEPOSITS'
                            ELSE 'DEBIT CARD PURCHASE' 
                        END
    LEFT JOIN MCC_SUB_CATEGORY msc 
        ON msc.SUB_CATG_NAME = CASE 
                                WHEN lt.description LIKE '%ATM%' THEN 'ATM WITHDRAWAL'
                                WHEN lt.description LIKE '%CASH WDL%' THEN 'TELLER WITHDRAWAL'
                                WHEN lt.description LIKE '%TRANSFER%' OR lt.description LIKE '%XFER%' THEN 'ACCOUNT TRANSFER'
                                WHEN lt.description LIKE '%SERVICE CHG%' OR lt.description LIKE '%FEE%' THEN 'MONTHLY SERVICE FEE'
                                WHEN CAST(lt.amount AS DECIMAL(19,4)) > 0 THEN 'DIRECT DEPOSIT / CREDIT'
                                ELSE 'GENERAL MERCHANDISE'
                            END
       AND msc.CATG_ID = mc.CATG_ID;
        
    COMMIT;
    
END ;;
