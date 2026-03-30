CREATE  PROCEDURE `Onboard_Member`(
    -- Member & Address
    IN p_FirstName      VARCHAR(50),
    IN p_LastName       VARCHAR(50),
    IN p_Gender         VARCHAR(10),
    IN p_DOB            DATE,         
    IN p_Address1       VARCHAR(150),
    IN p_Address2       VARCHAR(150),
    IN p_City           VARCHAR(50),
    IN p_State          VARCHAR(50),
    IN p_ZipCode        VARCHAR(10),
    IN p_Country        VARCHAR(50),
    -- Bank & Card Details
    IN p_BankName       VARCHAR(50),
    IN p_AccountType    VARCHAR(20),
    IN p_CardType       VARCHAR(20),
    IN p_FullCardNumber VARCHAR(20),  
    IN p_ExpDate        VARCHAR(5),
    IN p_CVV            VARCHAR(4),
    -- Encryption & Audit
    IN p_EncryptionKey  VARCHAR(100),
    IN p_AddedUser      VARCHAR(50)
)
BEGIN
    DECLARE varAddressId   INT;
    DECLARE varAddressKey  INT;
    DECLARE varMemberId    INT;
    DECLARE varMemberKey   INT;
    DECLARE varBankId      INT;
    DECLARE varBankKey     INT;
    DECLARE varCardKey     INT; -- Added variable for Card Key
    DECLARE varLast4       VARCHAR(4);
    DECLARE varCurrentTime DATETIME DEFAULT NOW();
    DECLARE varExists      INT DEFAULT 0;

    -- Exception Handling
    DECLARE EXIT HANDLER FOR SQLEXCEPTION 
    BEGIN
        GET DIAGNOSTICS CONDITION 1 @sqlstate = RETURNED_SQLSTATE, @msg = MESSAGE_TEXT;
        ROLLBACK;
        SELECT CONCAT('FAILED: ', @sqlstate, ' - ', @msg) AS ErrorStatus;
    END;

    SET varLast4 = RIGHT(p_FullCardNumber, 4);

    START TRANSACTION;

    -- ==========================================================
    -- 1. ADDRESS SCD TYPE 2 LOGIC
    -- ==========================================================
    SELECT ADDRESS_ID, ADDRESS_KEY INTO varAddressId, varAddressKey 
    FROM MEMBER_ADDRESS 
    WHERE ADDRESS_1 = p_Address1 AND ZIPCODE = p_ZipCode AND IS_CURRENT = 1 LIMIT 1;

    IF varAddressId IS NULL THEN
        -- Fetch next Key into variable first to avoid the "Target Table" error
        SELECT COALESCE(MAX(ADDRESS_KEY), 1000) + 1 INTO varAddressKey FROM MEMBER_ADDRESS;
        
        INSERT INTO MEMBER_ADDRESS (ADDRESS_KEY, ADDRESS_1, ADDRESS_2, CITY, STATE, ZIPCODE, ADDED_USER, EFF_START_DATE, IS_CURRENT)
        VALUES (varAddressKey, p_Address1, p_Address2, p_City, p_State, p_ZipCode, p_AddedUser, varCurrentTime, 1);
        SET varAddressId = LAST_INSERT_ID();
    END IF;

    -- ==========================================================
    -- 2. MEMBER SCD TYPE 2 LOGIC
    -- ==========================================================
    SELECT MEMBER_ID, MEMBER_KEY INTO varMemberId, varMemberKey 
    FROM MEMBERS 
    WHERE FIRST_NAME = p_FirstName AND LAST_NAME = p_LastName AND IS_CURRENT = 1 LIMIT 1;

    IF varMemberId IS NULL THEN
        -- Fetch next Key into variable
        SELECT COALESCE(MAX(MEMBER_KEY), 5000) + 1 INTO varMemberKey FROM MEMBERS;
        
        INSERT INTO MEMBERS (MEMBER_KEY, FIRST_NAME, LAST_NAME, GENDER, DATE_OF_BIRTH, ADDRESS_ID, ADDED_USER, EFF_START_DATE, IS_CURRENT)
        VALUES (varMemberKey, p_FirstName, p_LastName, p_Gender, AES_ENCRYPT(CAST(p_DOB AS CHAR), p_EncryptionKey), varAddressId, p_AddedUser, varCurrentTime, 1);
        SET varMemberId = LAST_INSERT_ID();
    ELSE
        IF (SELECT ADDRESS_ID FROM MEMBERS WHERE MEMBER_ID = varMemberId) <> varAddressId THEN
            UPDATE MEMBERS SET IS_CURRENT = 0, EFF_END_DATE = varCurrentTime WHERE MEMBER_ID = varMemberId;
            
            INSERT INTO MEMBERS (MEMBER_KEY, FIRST_NAME, LAST_NAME, GENDER, DATE_OF_BIRTH, ADDRESS_ID, ADDED_USER, EFF_START_DATE, IS_CURRENT)
            VALUES (varMemberKey, p_FirstName, p_LastName, p_Gender, AES_ENCRYPT(CAST(p_DOB AS CHAR), p_EncryptionKey), varAddressId, p_AddedUser, varCurrentTime, 1);
            SET varMemberId = LAST_INSERT_ID();
        END IF;
    END IF;

    -- ==========================================================
    -- 3. BANK SCD TYPE 2 LOGIC
    -- ==========================================================
    SELECT BANK_ID, BANK_KEY INTO varBankId, varBankKey 
    FROM BANKS 
    WHERE BANK_NAME = p_BankName AND ACCOUNT_TYPE = p_AccountType AND IS_CURRENT = 1 LIMIT 1;

    IF varBankId IS NULL THEN
        -- Fetch next Key into variable
        SELECT COALESCE(MAX(BANK_KEY), 2000) + 1 INTO varBankKey FROM BANKS;
        
        INSERT INTO BANKS (BANK_KEY, BANK_NAME, ACCOUNT_TYPE, ADDED_USER, EFF_START_DATE, IS_CURRENT)
        VALUES (varBankKey, p_BankName, p_AccountType, p_AddedUser, varCurrentTime, 1);
        SET varBankId = LAST_INSERT_ID();
    END IF;

    -- ==========================================================
    -- 4. SECURE CARD LOGIC (Fixed Target Table Error)
    -- ==========================================================
    -- Check if exists first
    SELECT COUNT(*) INTO varExists FROM CARDS 
    WHERE LAST_4 = varLast4 AND MEMBER_ID = varMemberId AND IS_CURRENT = 1;

    IF varExists = 0 THEN
        -- PRE-FETCH the new Card Key to avoid using SELECT inside the INSERT
        SELECT COALESCE(MAX(CARD_KEY), 8000) + 1 INTO varCardKey FROM CARDS;

        INSERT INTO CARDS (CARD_KEY, CARD_NUMBER, LAST_4, CARD_TYPE, EXP_DATE, CVV, BANK_ID, MEMBER_ID, ADDED_USER, EFF_START_DATE, IS_CURRENT)
        VALUES (
            varCardKey,
            AES_ENCRYPT(p_FullCardNumber, p_EncryptionKey), 
            varLast4,
            p_CardType, 
            AES_ENCRYPT(p_ExpDate, p_EncryptionKey), 
            AES_ENCRYPT(p_CVV, p_EncryptionKey),
            varBankId, 
            varMemberId, 
            p_AddedUser, 
            varCurrentTime, 
            1
        );
    END IF;

    COMMIT;

    SELECT 'SUCCESS' AS Status, varMemberId AS Active_Member_ID, varLast4 AS Card_Stored;

END ;;
