DELIMITER ;;

DROP PROCEDURE IF EXISTS BudgetApp.SP_PROCESS_AMEX_REPORTING ;;

CREATE PROCEDURE BudgetApp.SP_PROCESS_AMEX_REPORTING()
BEGIN
    DECLARE varBankId INT;
    DECLARE varLogId INT;
    DECLARE varRowCount INT DEFAULT 0;

    SELECT BANK_ID INTO varBankId FROM BudgetApp.BANKS WHERE BANK_NAME = 'American Express' AND IS_CURRENT = 1 LIMIT 1;
    INSERT INTO BudgetApp.ETL_LOG (PROC_NAME, BANK_ID, STATUS) VALUES ('SP_PROCESS_AMEX_REPORTING', varBankId, 'STARTED');
    SET varLogId = LAST_INSERT_ID();

    BEGIN
        DECLARE EXIT HANDLER FOR SQLEXCEPTION 
        BEGIN
            GET DIAGNOSTICS CONDITION 1 @p1 = RETURNED_SQLSTATE, @p2 = MESSAGE_TEXT;
            ROLLBACK;
            UPDATE BudgetApp.ETL_LOG SET STATUS = 'FAILED', ERROR_MSG = @p2, END_TIME = NOW() WHERE LOG_ID = varLogId;
        END;

        IF varBankId IS NULL THEN
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'American Express Bank ID not found.';
        END IF;

        START TRANSACTION;

        REPLACE INTO BudgetApp.TRANSACTION_REPORTING 
        (TRANSACTION_ID, BANK_ID, CARD_ID, MEMBER_ID, TRANSACTION_DATE, POSTED_DATE, TRANSACTION_AMOUNT, TRANSACTION_TYPE, 
         PAYMENT_CHANNEL, ENTITY_NAME, GEOGRAPHY, CATG_NAME, SUB_CATG_NAME, DETAILED_CATEGORY)
        SELECT 
            t.transaction_id, t.bank_id, t.card_id, t.member_id, t.transaction_date, t.posted_date, t.transaction_amount, t.transaction_type,
            CASE 
                WHEN DESCRIPTION LIKE 'MOBILE PAYMENT%' THEN 'PAYMENT'
                WHEN DESCRIPTION LIKE 'APLPAY %' THEN 'APPLE PAY'
                WHEN DESCRIPTION LIKE '%.COM%' OR EXT_DESCRIPTION LIKE '%.COM%' THEN 'ONLINE PURCHASE'
                ELSE 'CREDIT CARD'
            END,
            TRIM(CASE 
                WHEN DESCRIPTION LIKE 'MOBILE PAYMENT%' THEN 'AMERICAN EXPRESS'
                ELSE TRIM(REGEXP_REPLACE(
                         REPLACE(REPLACE(DESCRIPTION, 'APLPAY ', ''), 'TST* ', ''),
                         '[0-9]{4,}.*|[ ]{2,}.*', ''
                     ))
            END),
            TRIM(CASE 
                WHEN DESCRIPTION LIKE 'MOBILE PAYMENT%' THEN 'ONLINE'
                ELSE TRIM(CONCAT(
                         TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(DESCRIPTION, '  ', -2), '  ', 1)), 
                         ' ', 
                         TRIM(SUBSTRING_INDEX(DESCRIPTION, '  ', -1))
                     ))
            END),
            mc.CATG_NAME,
            msc.SUB_CATG_NAME,
            TRIM(CASE 
                WHEN EXT_DESCRIPTION LIKE '%DESCRIPTION : %' THEN 
                    SUBSTRING_INDEX(SUBSTRING_INDEX(EXT_DESCRIPTION, 'DESCRIPTION : ', -1), ' PRICE', 1)
                WHEN EXT_DESCRIPTION LIKE '%FAST FOOD REST%' THEN 'DINING'
                WHEN EXT_DESCRIPTION LIKE '%GROCERY STORE%' THEN 'GROCERIES'
                WHEN EXT_DESCRIPTION LIKE '%DISCOUNT STORE%' THEN 'SHOPPING'
                WHEN EXT_DESCRIPTION LIKE '%LODGING%' THEN 'TRAVEL'
                WHEN EXT_DESCRIPTION LIKE '%PASSENGER TICKET%' THEN 'AIRLINE'
                ELSE 'RETAIL'
            END)
        FROM BudgetApp.TRANSACTIONS t
        LEFT JOIN BudgetApp.MCC_CATEGORY mc ON t.CATG_ID = mc.CATG_ID
        LEFT JOIN BudgetApp.MCC_SUB_CATEGORY msc ON t.SUB_CATG_ID = msc.SUB_CATG_ID AND t.CATG_ID = msc.CATG_ID
        WHERE t.BANK_ID = varBankId;

        SET varRowCount = ROW_COUNT();
        COMMIT;

        UPDATE BudgetApp.ETL_LOG SET STATUS = 'SUCCESS', ROWS_PROCESSED = varRowCount, END_TIME = NOW() WHERE LOG_ID = varLogId;
    END;
END ;;
DELIMITER ;