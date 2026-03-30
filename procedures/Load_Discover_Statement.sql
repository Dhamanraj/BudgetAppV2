CREATE  PROCEDURE `Load_Discover_Statement`(
    IN inUserName VARCHAR(50),
    IN inBankName VARCHAR(50),
    IN inCARD_LAST_4 VARCHAR(4)
)
BEGIN
    DECLARE varMemberId INT;
    DECLARE varBankId INT;
    DECLARE varCardId INT;

    -- 1. VALIDATION (SCD Type 2: Must fetch the CURRENT active version)
    SELECT m.MEMBER_ID INTO varMemberId 
    FROM MEMBERS m 
    WHERE m.FIRST_NAME = inUserName 
      AND m.IS_CURRENT = 1 LIMIT 1;

    SELECT b.BANK_ID INTO varBankId 
    FROM BANKS b 
    WHERE b.BANK_NAME = inBankName 
      AND b.IS_CURRENT = 1 LIMIT 1;

    SELECT c.CARD_ID INTO varCardId 
    FROM CARDS c 
    WHERE c.LAST_4 = inCARD_LAST_4 
      AND c.IS_CURRENT = 1 LIMIT 1;
    
    -- Error if any active entities are missing
    IF varMemberId IS NULL OR varBankId IS NULL OR varCardId IS NULL THEN
        SIGNAL SQLSTATE '45000' 
        SET MESSAGE_TEXT = 'VALIDATION FAILED: NO ACTIVE (IS_CURRENT=1) MEMBER, BANK, OR CARD FOUND.';
    END IF;
	
    START TRANSACTION;
    
    -- 2. PHASE 1: Auto-Onboard Parent Categories
    INSERT IGNORE INTO MCC_CATEGORY (CATG_NAME, ADDED_USER, ADDED_DATETIME)
    SELECT DISTINCT 
        CASE 
            WHEN CAST(lt.amount AS DECIMAL(30,2)) >= 0 THEN 'DISCOVER EXPENDITURE' 
            WHEN lt.description LIKE '%PAYMENT%' THEN 'CREDIT CARD PAYMENT' 
            ELSE 'MERCHANT REFUND' 
        END,
        'SYSTEM_AUTO',
        NOW()
    FROM LND_TRANSACTIONS lt;

    -- 3. PHASE 2: Auto-Onboard Sub-Categories
    INSERT IGNORE INTO MCC_SUB_CATEGORY (SUB_CATG_NAME, CATG_ID, ADDED_USER, ADDED_DATETIME)
    SELECT DISTINCT 
        CASE 
            WHEN CAST(lt.amount AS DECIMAL(30,2)) >= 0 THEN UPPER(TRIM(lt.Category)) 
            WHEN lt.description LIKE '%PAYMENT%' THEN 'CREDIT CARD PAYMENT' 
            ELSE 'MERCHANT REFUND' 
        END,
        mc.CATG_ID, 
        'SYSTEM_AUTO', 
        NOW()
    FROM LND_TRANSACTIONS lt
    JOIN MCC_CATEGORY mc 
        ON mc.CATG_NAME = CASE 
                            WHEN CAST(lt.amount AS DECIMAL(30,2)) >= 0 THEN 'DISCOVER EXPENDITURE' 
                            WHEN lt.description LIKE '%PAYMENT%' THEN 'CREDIT CARD PAYMENT' 
                            ELSE 'MERCHANT REFUND' 
                        END
    WHERE lt.Category IS NOT NULL OR lt.description LIKE '%PAYMENT%';
    
    -- 4. PHASE 3: Load Final Transactions (Now including POSTED_DATE)
    INSERT IGNORE INTO TRANSACTIONS 
    (
        TRANSACTION_ID, 
        TRANSACTION_DATE, 
        POSTED_DATE,        -- Included as requested
        TRANSACTION_TYPE, 
        TRANSACTION_AMOUNT,
        DESCRIPTION, 
        EXT_DESCRIPTION, 
        ADDRESS_1, 
        ADDRESS_2, 
        CITY, 
        STATE,
        COUNTRY, 
        ZIPCODE, 
        CATG_ID, 
        SUB_CATG_ID, 
        BANK_ID, 
        CARD_ID,
        MEMBER_ID, 
        ADDED_USER, 
        ADDED_DATETIME    
    )
    SELECT 
        -- Hash-based ID to prevent duplicates
        ABS(CAST(CONV(SUBSTRING(MD5(CONCAT(lt.transaction_date, lt.amount, lt.description)), 1, 16), 16, 10) AS SIGNED)),
        STR_TO_DATE(lt.transaction_date, "%m/%d/%Y"),
        STR_TO_DATE(lt.posted_date, "%m/%d/%Y"), -- Discover specific mapping
        CASE WHEN CAST(lt.amount AS DECIMAL(30,2)) >= 0 THEN 'DBT' ELSE 'CDT' END,
        ABS(CAST(lt.amount AS DECIMAL(19,4))),
        UPPER(lt.description),
        NULL, 
        NULL, 
        NULL, 
        NULL, 
        NULL, 
        'US', 
        NULL, 
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
                            WHEN CAST(lt.amount AS DECIMAL(30,2)) >= 0 THEN 'DISCOVER EXPENDITURE' 
                            WHEN lt.description LIKE '%PAYMENT%' THEN 'CREDIT CARD PAYMENT' 
                            ELSE 'MERCHANT REFUND' 
                        END
    LEFT JOIN MCC_SUB_CATEGORY msc 
        ON msc.SUB_CATG_NAME = CASE 
                                WHEN CAST(lt.amount AS DECIMAL(30,2)) >= 0 THEN UPPER(TRIM(lt.Category)) 
                                WHEN lt.description LIKE '%PAYMENT%' THEN 'CREDIT CARD PAYMENT' 
                                ELSE 'MERCHANT REFUND' 
                            END
       AND msc.CATG_ID = mc.CATG_ID;
        
    COMMIT;
    
END ;;
