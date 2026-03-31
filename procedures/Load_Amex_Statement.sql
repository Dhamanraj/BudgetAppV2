CREATE  PROCEDURE `Load_Amex_Statement`(
    IN inUserName VARCHAR(50),
    IN inBankName VARCHAR(50),
    IN inCARD_LAST_4 VARCHAR(4)
)
BEGIN
	
    DECLARE varMemberId INT;
    DECLARE varBankId INT;
    DECLARE varCardId INT;

    -- PHASE 0: VALIDATION (Must filter for IS_CURRENT = 1)
    -- We select the ID of the ACTIVE version of these entities
    SELECT m.MEMBER_ID INTO varMemberId 
    FROM MEMBERS m 
    WHERE m.FIRST_NAME = inUserName 
      AND m.IS_CURRENT = 1 LIMIT 1;

    SELECT BANK_ID INTO varBankId 
    FROM BANKS b 
    WHERE b.BANK_NAME = inBankName 
      AND b.IS_CURRENT = 1 LIMIT 1;

    SELECT CARD_ID INTO varCardId 
    FROM CARDS c 
    WHERE c.LAST_4 = inCARD_LAST_4 
      AND c.IS_CURRENT = 1 LIMIT 1;
    
    -- Error if active versions aren't found
    IF varMemberId IS NULL OR varBankId IS NULL OR varCardId IS NULL THEN
        SIGNAL SQLSTATE '45000' 
        SET MESSAGE_TEXT = 'VALIDATION FAILED: NO ACTIVE (IS_CURRENT=1) MEMBER, BANK, OR CARD FOUND.';
    END IF;
	
    START TRANSACTION;
    
    -- PHASE 1: Auto-Onboard Categories
    INSERT INTO MCC_CATEGORY (CATG_NAME, ADDED_USER, ADDED_DATETIME)
    SELECT DISTINCT 
        src.CATG_NAME,
        'SYSTEM_AUTO',
        NOW()
    FROM (
        SELECT DISTINCT 
            CASE 
                WHEN CAST(lt.AMOUNT AS DECIMAL(30,2)) >= 0 THEN UPPER(TRIM(SUBSTRING_INDEX(lt.CATEGORY, '-', 1))) 
                WHEN lt.DESCRIPTION LIKE '%PAYMENT%' THEN 'CREDIT CARD PAYMENT' 
                ELSE 'MERCHANT REFUND' 
            END AS CATG_NAME
        FROM LND_TRANSACTIONS lt
        WHERE lt.CATEGORY IS NOT NULL
    ) src
    WHERE NOT EXISTS (SELECT 1 FROM MCC_CATEGORY mc WHERE mc.CATG_NAME = src.CATG_NAME);

    -- PHASE 2: Auto-Onboard Sub-Categories
    INSERT INTO MCC_SUB_CATEGORY (SUB_CATG_NAME, CATG_ID, ADDED_USER, ADDED_DATETIME)
    SELECT DISTINCT 
        src.SUB_CATG_NAME,
        mc.CATG_ID, 
        'SYSTEM_AUTO', 
        NOW()
    FROM (
        SELECT DISTINCT 
            CASE 
                WHEN CAST(lt.AMOUNT AS DECIMAL(30,2)) >= 0 THEN UPPER(TRIM(SUBSTRING_INDEX(lt.CATEGORY, '-', -1))) 
                WHEN lt.DESCRIPTION LIKE '%PAYMENT%' THEN 'CREDIT CARD PAYMENT' 
                ELSE 'MERCHANT REFUND' 
            END AS SUB_CATG_NAME,
            CASE 
                WHEN CAST(lt.AMOUNT AS DECIMAL(30,2)) >= 0 THEN UPPER(TRIM(SUBSTRING_INDEX(lt.CATEGORY, '-', 1))) 
                WHEN lt.DESCRIPTION LIKE '%PAYMENT%' THEN 'CREDIT CARD PAYMENT' 
                ELSE 'MERCHANT REFUND' 
            END AS PARENT_CATG_NAME
        FROM LND_TRANSACTIONS lt
        WHERE lt.CATEGORY IS NOT NULL
    ) src
    JOIN (SELECT CATG_NAME, MIN(CATG_ID) AS CATG_ID FROM MCC_CATEGORY GROUP BY CATG_NAME) mc 
        ON mc.CATG_NAME = src.PARENT_CATG_NAME
    WHERE NOT EXISTS (
        SELECT 1 FROM MCC_SUB_CATEGORY msc WHERE msc.SUB_CATG_NAME = src.SUB_CATG_NAME AND msc.CATG_ID = mc.CATG_ID
    );
    
    -- PHASE 3: Load Final Transactions
    -- These will now be hard-linked to the specific version IDs found in Phase 0
    INSERT IGNORE INTO TRANSACTIONS 
    (
        TRANSACTION_ID, TRANSACTION_DATE, TRANSACTION_TYPE, TRANSACTION_AMOUNT,
        DESCRIPTION, EXT_DESCRIPTION, ADDRESS_1, ADDRESS_2, CITY, STATE,
        COUNTRY, ZIPCODE, CATG_ID, SUB_CATG_ID, BANK_ID, CARD_ID,
        MEMBER_ID, ADDED_USER, ADDED_DATETIME    
    )
    SELECT 
        CAST(SUBSTRING(lt.REFERENCE, 2, LENGTH(lt.REFERENCE)-1) AS DECIMAL(30,0)),
        STR_TO_DATE(lt.TRANSACTION_DATE, "%m/%d/%Y"),
        CASE WHEN CAST(lt.AMOUNT AS DECIMAL(30,2)) >= 0 THEN 'DBT' ELSE 'CDT' END,
        ABS(CAST(lt.AMOUNT AS DECIMAL(19,4))),
        UPPER(lt.DESCRIPTION),
        UPPER(CONCAT_WS(" ", IFNULL(lt.EXT_DETAILS, ""), "STATEMENT:", IFNULL(lt.STATEMENT_DESC, ""))),
        UPPER(lt.ADDRESS),
        NULL,
        UPPER(TRIM(SUBSTRING(lt.CITY_STATE, 1, LENGTH(lt.CITY_STATE) - 3))),
        UPPER(TRIM(SUBSTRING(lt.CITY_STATE, LENGTH(lt.CITY_STATE) - 1))),
        UPPER(lt.COUNTRY),
        lt.ZIPCODE,
        mc.CATG_ID,
        msc.SUB_CATG_ID, 
        varBankId,  -- Active version ID
        varCardId,  -- Active version ID
        varMemberId, -- Active version ID
        'DKAKKE',
        CURRENT_TIMESTAMP()
    FROM LND_TRANSACTIONS lt 
    LEFT JOIN (SELECT CATG_NAME, MIN(CATG_ID) AS CATG_ID FROM MCC_CATEGORY GROUP BY CATG_NAME) mc 
        ON mc.CATG_NAME = CASE 
                            WHEN CAST(lt.AMOUNT AS DECIMAL(30,2)) >= 0 THEN UPPER(TRIM(SUBSTRING_INDEX(lt.CATEGORY, '-', 1))) 
                            WHEN lt.DESCRIPTION LIKE '%PAYMENT%' THEN 'CREDIT CARD PAYMENT' 
                            ELSE 'MERCHANT REFUND' 
                        END
    LEFT JOIN (SELECT SUB_CATG_NAME, CATG_ID, MIN(SUB_CATG_ID) AS SUB_CATG_ID FROM MCC_SUB_CATEGORY GROUP BY SUB_CATG_NAME, CATG_ID) msc 
        ON msc.SUB_CATG_NAME = CASE 
                                WHEN CAST(lt.AMOUNT AS DECIMAL(30,2)) >= 0 THEN UPPER(TRIM(SUBSTRING_INDEX(lt.CATEGORY, '-', -1))) 
                                WHEN lt.DESCRIPTION LIKE '%PAYMENT%' THEN 'CREDIT CARD PAYMENT' 
                                ELSE 'MERCHANT REFUND' 
                            END
       AND msc.CATG_ID = mc.CATG_ID
    WHERE lt.REFERENCE IS NOT NULL;
        
    COMMIT;
    
END ;;
