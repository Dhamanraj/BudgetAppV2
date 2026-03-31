DELIMITER ;;

CREATE PROCEDURE BudgetApp.SP_PROCESS_DISCOVER_REPORTING()
BEGIN
    DECLARE varBankId INT;
    DECLARE varLogId INT;
    DECLARE varRowCount INT DEFAULT 0;

    SELECT BANK_ID INTO varBankId FROM BudgetApp.BANKS WHERE BANK_NAME LIKE '%Discover%' AND IS_CURRENT = 1 LIMIT 1;
    INSERT INTO BudgetApp.ETL_LOG (PROC_NAME, BANK_ID, STATUS) VALUES ('SP_PROCESS_DISCOVER_REPORTING', varBankId, 'STARTED');
    SET varLogId = LAST_INSERT_ID();

    BEGIN
        DECLARE EXIT HANDLER FOR SQLEXCEPTION 
        BEGIN
            GET DIAGNOSTICS CONDITION 1 @p1 = RETURNED_SQLSTATE, @p2 = MESSAGE_TEXT;
            ROLLBACK;
            UPDATE BudgetApp.ETL_LOG SET STATUS = 'FAILED', ERROR_MSG = @p2, END_TIME = NOW() WHERE LOG_ID = varLogId;
        END;

        IF varBankId IS NULL THEN
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Discover Bank ID not found.';
        END IF;

        START TRANSACTION;

        REPLACE INTO BudgetApp.TRANSACTION_REPORTING 
        (TRANSACTION_ID, BANK_ID, MEMBER_ID, TRANSACTION_DATE, TRANSACTION_AMOUNT, TRANSACTION_TYPE, 
         PAYMENT_CHANNEL, ENTITY_NAME, GEOGRAPHY, DETAILED_CATEGORY, TRACE_ID)
        SELECT 
            transaction_id, bank_id, member_id, transaction_date, transaction_amount, transaction_type,
            CASE 
                WHEN DESCRIPTION LIKE 'INTERNET PAYMENT%' OR EXT_DESCRIPTION = 'PAYMENTS AND CREDITS' THEN 'PAYMENT'
                WHEN DESCRIPTION LIKE 'AMAZON%' OR DESCRIPTION LIKE 'AMZN%' THEN 'ONLINE PURCHASE'
                WHEN EXT_DESCRIPTION = 'GASOLINE' THEN 'GAS STATION'
                ELSE 'CREDIT CARD'
            END,
            TRIM(REGEXP_REPLACE(
                REGEXP_REPLACE(DESCRIPTION, 'AMAZON MKTPL\\*[^ ]+|AMAZON\\.COM\\*[^ ]+|AMAZON MKTPLACE PMTS', 'AMAZON'),
                '([ ]+([A-Z.]{2,}( [A-Z.]{2,})?) [A-Z]{2}([0-9]+)?$)|([ ]+([0-9#]{3,}|[0-9]{2,}-[0-9]+).*)|(AMZN.COM/BILL.*)', ''
            )),
            CASE 
                WHEN DESCRIPTION LIKE '% PAYMENT %' THEN 'ONLINE'
                WHEN DESCRIPTION REGEXP '([A-Z.]{2,}( [A-Z.]{2,})? [A-Z]{2}([0-9]+)?$)' THEN 
                    TRIM(REGEXP_SUBSTR(DESCRIPTION, '([A-Z.]{2,}( [A-Z.]{2,})? [A-Z]{2}([0-9]+)?$)'))
                ELSE 'UNKNOWN'
            END,
            UPPER(TRIM(EXT_DESCRIPTION)),
            REGEXP_SUBSTR(DESCRIPTION, '[0-9]{10,}')
        FROM BudgetApp.TRANSACTIONS 
        WHERE BANK_ID = varBankId;

        SET varRowCount = ROW_COUNT();
        COMMIT;

        UPDATE BudgetApp.ETL_LOG SET STATUS = 'SUCCESS', ROWS_PROCESSED = varRowCount, END_TIME = NOW() WHERE LOG_ID = varLogId;
    END;
END ;;
DELIMITER ;