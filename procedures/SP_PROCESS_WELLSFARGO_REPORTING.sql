DELIMITER ;;

DROP PROCEDURE IF EXISTS BudgetApp.SP_PROCESS_WELLSFARGO_REPORTING ;;

CREATE PROCEDURE BudgetApp.SP_PROCESS_WELLSFARGO_REPORTING()
BEGIN
    DECLARE varBankId INT;
    DECLARE varLogId INT;
    DECLARE varRowCount INT DEFAULT 0;

    -- Initialize Log
    SELECT BANK_ID INTO varBankId FROM BudgetApp.BANKS WHERE BANK_NAME LIKE '%Wells%' LIMIT 1;
    INSERT INTO BudgetApp.ETL_LOG (PROC_NAME, BANK_ID, STATUS) VALUES ('SP_PROCESS_WELLSFARGO_REPORTING', varBankId, 'STARTED');
    SET varLogId = LAST_INSERT_ID();

    -- Error Handling
    BEGIN
        DECLARE EXIT HANDLER FOR SQLEXCEPTION 
        BEGIN
            GET DIAGNOSTICS CONDITION 1 @p1 = RETURNED_SQLSTATE, @p2 = MESSAGE_TEXT;
            ROLLBACK;
            UPDATE BudgetApp.ETL_LOG SET STATUS = 'FAILED', ERROR_MSG = @p2, END_TIME = NOW() WHERE LOG_ID = varLogId;
        END;

        IF varBankId IS NULL THEN
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Wells Fargo Bank ID not found in BANKS table.';
        END IF;

        START TRANSACTION;

        REPLACE INTO BudgetApp.TRANSACTION_REPORTING 
        (TRANSACTION_ID, BANK_ID, CARD_ID, MEMBER_ID, TRANSACTION_DATE, POSTED_DATE, TRANSACTION_AMOUNT, TRANSACTION_TYPE, 
         PAYMENT_CHANNEL, ENTITY_NAME, GEOGRAPHY, CATG_NAME, SUB_CATG_NAME, TRANSACTION_MEMO, TRACE_ID, ASSOCIATED_BANK)
        SELECT 
            t.transaction_id, t.bank_id, t.card_id, t.member_id, t.transaction_date, t.posted_date, t.transaction_amount, t.transaction_type,
            CASE 
                WHEN t.DESCRIPTION LIKE 'PURCHASE AUTHORIZED%' THEN 'DEBIT CARD'
                WHEN t.DESCRIPTION LIKE 'ZELLE%' THEN 'ZELLE'
                WHEN t.DESCRIPTION LIKE 'ATM CASH%' THEN 'CASH'
                ELSE 'ACH/TRANSFER'
            END,
            TRIM(CASE 
                WHEN t.DESCRIPTION LIKE 'ZELLE FROM %' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(t.DESCRIPTION, ' FROM ', -1), ' ON ', 1)
                WHEN t.DESCRIPTION LIKE 'ZELLE TO %' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(t.DESCRIPTION, ' TO ', -1), ' ON ', 1)
                WHEN t.DESCRIPTION LIKE 'PURCHASE AUTHORIZED%' THEN SUBSTRING_INDEX(TRIM(SUBSTRING(t.DESCRIPTION, 26)), ' ', 1)
                ELSE SUBSTRING_INDEX(t.DESCRIPTION, ' ', 2)
            END),
            CASE WHEN t.DESCRIPTION LIKE '% MCKINNEY %' THEN 'MCKINNEY TX' ELSE 'ONLINE' END,
            mc.CATG_NAME,
            msc.SUB_CATG_NAME,
            CASE 
                WHEN t.DESCRIPTION LIKE 'ZELLE % REF # %' THEN 
                     CASE 
                        WHEN SUBSTRING_INDEX(t.DESCRIPTION, ' REF # ', -1) LIKE '% %' THEN 
                            SUBSTRING(SUBSTRING_INDEX(t.DESCRIPTION, ' REF # ', -1), LOCATE(' ', SUBSTRING_INDEX(t.DESCRIPTION, ' REF # ', -1)) + 1)
                        ELSE NULL 
                     END
                ELSE NULL 
            END,
            SUBSTRING_INDEX(SUBSTRING_INDEX(t.DESCRIPTION, ' REF # ', -1), ' ', 1),
            CASE WHEN t.DESCRIPTION LIKE '%JPM%' THEN 'CHASE' WHEN t.DESCRIPTION LIKE '%BAC%' THEN 'BOFA' ELSE NULL END
        FROM BudgetApp.TRANSACTIONS t
        LEFT JOIN BudgetApp.MCC_CATEGORY mc ON t.CATG_ID = mc.CATG_ID
        LEFT JOIN BudgetApp.MCC_SUB_CATEGORY msc ON t.SUB_CATG_ID = msc.SUB_CATG_ID AND t.CATG_ID = msc.CATG_ID
        WHERE t.BANK_ID = varBankId;

        SET varRowCount = ROW_COUNT();
        
        COMMIT;

        -- Finalize Log
        UPDATE BudgetApp.ETL_LOG SET STATUS = 'SUCCESS', ROWS_PROCESSED = varRowCount, END_TIME = NOW() WHERE LOG_ID = varLogId;
    END;
END ;;

DELIMITER ;