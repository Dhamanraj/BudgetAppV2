CREATE  PROCEDURE `Load_Citi_Statement`(
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
    
    -- 2. PHASE 1: Auto-Onboard Parent Categories
    INSERT INTO MCC_CATEGORY (CATG_NAME, ADDED_USER, ADDED_DATETIME)
    SELECT DISTINCT 
        src.CATG_NAME,
        'SYSTEM_AUTO',
        NOW()
    FROM (
        SELECT DISTINCT 
            CASE 
                WHEN lt.credit IS NOT NULL AND (lt.description LIKE '%PAYMENT%') THEN 'CREDIT CARD PAYMENT'
                WHEN lt.credit IS NOT NULL AND lt.description NOT LIKE '%PAYMENT%' THEN 'MERCHANT REFUND'
                ELSE 'CITI EXPENDITURE' 
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
                WHEN lt.credit IS NOT NULL AND (lt.description LIKE '%PAYMENT%') THEN 'CREDIT CARD PAYMENT'
                WHEN lt.credit IS NOT NULL AND lt.description NOT LIKE '%PAYMENT%' THEN 'MERCHANT REFUND'
                ELSE UPPER(TRIM(COALESCE(lt.category, 'UNCATEGORIZED'))) 
            END AS SUB_CATG_NAME,
            CASE 
                WHEN lt.credit IS NOT NULL AND ( lt.description LIKE '%PAYMENT%') THEN 'CREDIT CARD PAYMENT'
                WHEN lt.credit IS NOT NULL AND lt.description NOT LIKE '%PAYMENT%' THEN 'MERCHANT REFUND'
                ELSE 'CITI EXPENDITURE' 
            END AS PARENT_CATG_NAME
        FROM LND_TRANSACTIONS lt
    ) src
    JOIN (SELECT CATG_NAME, MIN(CATG_ID) AS CATG_ID FROM MCC_CATEGORY GROUP BY CATG_NAME) mc 
        ON mc.CATG_NAME = src.PARENT_CATG_NAME
    WHERE NOT EXISTS (
        SELECT 1 FROM MCC_SUB_CATEGORY msc WHERE msc.SUB_CATG_NAME = src.SUB_CATG_NAME AND msc.CATG_ID = mc.CATG_ID
    );
    
    -- 4. PHASE 3: Load Final Transactions
    INSERT IGNORE INTO TRANSACTIONS 
    (
        TRANSACTION_ID, TRANSACTION_DATE, TRANSACTION_TYPE, TRANSACTION_AMOUNT,
        DESCRIPTION, CATG_ID, SUB_CATG_ID, BANK_ID, CARD_ID,
        MEMBER_ID, ADDED_USER, ADDED_DATETIME    
    )
    SELECT 
        -- Hash ID based on Date + Debit + Credit + Description
        ABS(CAST(CONV(SUBSTRING(MD5(CONCAT(lt.transaction_date, IFNULL(lt.debit,'0'), IFNULL(lt.credit,'0'), lt.description,ROW_NUMBER() OVER(ORDER BY lt.transaction_date, IFNULL(lt.debit,'0'), IFNULL(lt.credit,'0'), lt.description))), 1, 16), 16, 10) AS SIGNED)),
        -- Date format mm/dd/yyyy
        STR_TO_DATE(lt.transaction_date, "%m/%d/%Y"),
        -- Type Logic: If credit is populated, it's a Credit (CDT), else Debit (DBT)
        CASE 
            WHEN lt.credit IS NOT NULL AND lt.credit <> '' THEN 'CDT'
            ELSE 'DBT' 
        END,
        -- Amount Logic: Take ABS of whichever column is populated
        CASE 
            WHEN lt.credit IS NOT NULL AND lt.credit <> '' THEN ABS(CAST(lt.credit AS DECIMAL(19,4)))
            ELSE ABS(CAST(lt.debit AS DECIMAL(19,4)))
        END,
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
                            WHEN lt.credit IS NOT NULL AND ( lt.description LIKE '%PAYMENT%') THEN 'CREDIT CARD PAYMENT'
                            WHEN lt.credit IS NOT NULL AND lt.description NOT LIKE '%PAYMENT%' THEN 'MERCHANT REFUND'
                            ELSE 'CITI EXPENDITURE' 
                        END
    LEFT JOIN (SELECT SUB_CATG_NAME, CATG_ID, MIN(SUB_CATG_ID) AS SUB_CATG_ID FROM MCC_SUB_CATEGORY GROUP BY SUB_CATG_NAME, CATG_ID) msc 
        ON msc.SUB_CATG_NAME = CASE 
                                WHEN lt.credit IS NOT NULL AND (lt.description LIKE '%PAYMENT%') THEN 'CREDIT CARD PAYMENT'
                                WHEN lt.credit IS NOT NULL AND lt.description NOT LIKE '%PAYMENT%' THEN 'MERCHANT REFUND'
                                ELSE UPPER(TRIM(COALESCE(lt.category, 'UNCATEGORIZED'))) 
                            END
       AND msc.CATG_ID = mc.CATG_ID;
        
    COMMIT;
    
END ;;
