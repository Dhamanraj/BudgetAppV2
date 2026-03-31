-- This query demonstrates how to parse the unstructured bank description strings
-- into structured reporting columns.

-- =============================================================================
-- 1. AXIS BANK / STANDARD PATTERN (Slash-Delimited)
-- =============================================================================
SELECT 
    DESCRIPTION AS RAW_DESC,
    CASE 
        WHEN DESCRIPTION LIKE 'POS/%' THEN 'DEBIT CARD'
        WHEN DESCRIPTION LIKE 'ECOM PUR/%' THEN 'ONLINE PURCHASE'
        WHEN DESCRIPTION LIKE 'PURCHASE AUTHORIZED ON %' THEN 'DEBIT CARD'
        WHEN DESCRIPTION LIKE 'UPI/%' THEN 'UPI'
        WHEN DESCRIPTION LIKE 'IMPS/%' THEN 'IMPS'
        WHEN DESCRIPTION LIKE 'ATM-%' THEN 'CASH WITHDRAWAL'
        WHEN DESCRIPTION LIKE 'ZELLE %' THEN 'ZELLE'
        WHEN DESCRIPTION LIKE 'ATM-%' OR DESCRIPTION LIKE 'ATM CASH %' THEN 'CASH WITHDRAWAL'
        WHEN DESCRIPTION LIKE 'MOB/%' THEN 'MOBILE BANKING'
        WHEN DESCRIPTION LIKE 'ECS/%' THEN 'AUTOPAY/ECS'
        WHEN DESCRIPTION LIKE 'NBSM/%' THEN 'NET BANKING'
        WHEN DESCRIPTION LIKE 'PUR-REV/%' THEN 'REVERSAL'
        WHEN DESCRIPTION LIKE '%PAYMENT%' OR DESCRIPTION LIKE '% PMT%' OR DESCRIPTION LIKE '% PAY %' 
             OR DESCRIPTION LIKE '% PREM %' OR DESCRIPTION LIKE '% ACH %' THEN 'BILL PAY/ACH'
        WHEN DESCRIPTION LIKE '%PAYROLL%' THEN 'PAYROLL'
        WHEN DESCRIPTION LIKE '%MONEYLINK%' OR DESCRIPTION LIKE '%AUTO DRAFT%' THEN 'TRANSFER'
        WHEN DESCRIPTION LIKE 'INT ON SB%' OR DESCRIPTION LIKE 'SB:%' OR DESCRIPTION LIKE '%INT.PD:%' THEN 'BANK INTEREST/TAX'
        ELSE 'OTHERS'
    END AS PAYMENT_CHANNEL,
    TRIM(CASE 
        WHEN DESCRIPTION LIKE 'POS/%' OR DESCRIPTION LIKE 'ECOM PUR/%' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(DESCRIPTION, '/', 2), '/', -1)
        WHEN DESCRIPTION LIKE 'UPI/%' OR DESCRIPTION LIKE 'IMPS/%' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(DESCRIPTION, '/', 4), '/', -1)
        WHEN DESCRIPTION LIKE 'MOB/%' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(DESCRIPTION, '/', 3), '/', -1)
        WHEN DESCRIPTION LIKE 'ECS/%' OR DESCRIPTION LIKE 'NBSM/%' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(DESCRIPTION, '/', 3), '/', -1)
        WHEN DESCRIPTION LIKE 'INT ON SB%' OR DESCRIPTION LIKE 'SB:%' OR DESCRIPTION LIKE '%INT.PD:%' THEN 'BANK SYSTEM'
        WHEN DESCRIPTION LIKE 'ATM-%' THEN 'SELF'
        WHEN DESCRIPTION LIKE 'PUR-REV/%' THEN 'REVERSAL'
        ELSE 'UNKNOWN'
    END) AS ENTITY_NAME,
    TRIM(CASE 
        WHEN DESCRIPTION LIKE 'POS/%' OR DESCRIPTION LIKE 'ECOM PUR/%' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(DESCRIPTION, '/', 3), '/', -1)
        WHEN DESCRIPTION LIKE 'ATM-%' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(DESCRIPTION, '/', 3), '/', -1)
        ELSE 'ONLINE/TRANSFER'
    END) AS GEOGRAPHY,
    TRIM(CASE 
        WHEN DESCRIPTION LIKE 'UPI/%' OR DESCRIPTION LIKE 'IMPS/%' THEN 
            CASE 
                WHEN SUBSTRING_INDEX(TRIM(BOTH '/' FROM DESCRIPTION), '/', -1) IN ('PAYMENT', 'PAYMEN', 'TRANSFER', 'OTHERS', 'NULL', 'NA', '')
                THEN SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(BOTH '/' FROM DESCRIPTION), '/', -2), '/', 1)
                ELSE SUBSTRING_INDEX(TRIM(BOTH '/' FROM DESCRIPTION), '/', -1)
            END
        ELSE NULL
    END) AS ASSOCIATED_BANK
FROM (
    select 
        t.transaction_id
        ,t.transaction_date
        ,t.transaction_type
        ,t.transaction_amount
        ,t.description
        ,mc.catg_name
        ,msc.sub_catg_name
    from transactions t
    left join mcc_category mc on mc.catg_id = t.catg_id
    left join mcc_sub_category msc on msc.sub_catg_id = t.sub_catg_id
    where bank_id = 11 -- Axis Bank ID
) AS axis_data;

-- =============================================================================
-- 2. WELLS FARGO PATTERN (Keyword/Space-Delimited)
-- =============================================================================
SELECT 
    DESCRIPTION AS RAW_DESC,
    CASE 
        WHEN DESCRIPTION LIKE 'PURCHASE AUTHORIZED ON %' THEN 'DEBIT CARD'
        WHEN DESCRIPTION LIKE 'ZELLE %' THEN 'ZELLE'
        WHEN DESCRIPTION LIKE 'ATM CASH %' THEN 'CASH WITHDRAWAL'
        WHEN DESCRIPTION LIKE '%PAYMENT%' OR DESCRIPTION LIKE '% PMT%' OR DESCRIPTION LIKE '% PAY %' 
             OR DESCRIPTION LIKE '% PREM %' OR DESCRIPTION LIKE '% ACH %' THEN 'BILL PAY/ACH'
        WHEN DESCRIPTION LIKE '%PAYROLL%' OR DESCRIPTION LIKE '%MOSTTAXRFD%' THEN 'INCOME/REFUND'
        WHEN DESCRIPTION LIKE '%USATAXPYMT%' THEN 'TAX PAYMENT'
        WHEN DESCRIPTION LIKE '%MONEYLINK%' OR DESCRIPTION LIKE '%AUTO DRAFT%' THEN 'TRANSFER'
        ELSE 'OTHERS'
    END AS PAYMENT_CHANNEL,
    TRIM(CASE 
        WHEN DESCRIPTION LIKE 'ZELLE FROM %' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(DESCRIPTION, ' FROM ', -1), ' ON ', 1)
        WHEN DESCRIPTION LIKE 'ZELLE TO %' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(DESCRIPTION, ' TO ', -1), ' ON ', 1)
        WHEN DESCRIPTION LIKE 'PURCHASE AUTHORIZED ON %' THEN SUBSTRING_INDEX(TRIM(SUBSTRING(DESCRIPTION, 26)), ' ', 1)
        WHEN DESCRIPTION LIKE 'ATM CASH %' THEN 'SELF'
        WHEN DESCRIPTION LIKE 'AMERENMO %' THEN 'AMERENMO'
        WHEN DESCRIPTION LIKE 'CITI CARD %' THEN 'CITI'
        WHEN DESCRIPTION LIKE 'CAPITAL ONE %' THEN 'CAPITAL ONE'
        WHEN DESCRIPTION LIKE 'AMERICAN EXPRESS %' THEN 'AMERICAN EXPRESS'
        WHEN DESCRIPTION LIKE 'APPLECARD %' THEN 'APPLE'
        WHEN DESCRIPTION LIKE 'DISCOVER %' THEN 'DISCOVER'
        WHEN DESCRIPTION LIKE 'LIBERTY MUTUAL %' THEN 'LIBERTY MUTUAL'
        WHEN DESCRIPTION LIKE 'PROG COUNTY %' THEN 'PROGRESSIVE'
        WHEN DESCRIPTION LIKE 'IRS %' THEN 'IRS'
        WHEN DESCRIPTION LIKE 'MO DEPT REVENUE %' THEN 'MO DEPT REVENUE'
        WHEN DESCRIPTION LIKE 'RELIABLE SOFTWAR %' THEN 'RELIABLE SOFTWARE'
        WHEN DESCRIPTION LIKE 'SCHWAB %' THEN 'SCHWAB'
        WHEN DESCRIPTION LIKE 'WELLS FARGO %' THEN 'WELLS FARGO'
        WHEN DESCRIPTION LIKE 'PF MCKINNEY %' THEN 'PLANET FITNESS'
        ELSE 'UNKNOWN'
    END) AS ENTITY_NAME,
    -- Improved Geography & Memo Extraction
    TRIM(CASE 
        WHEN DESCRIPTION LIKE 'PURCHASE AUTHORIZED ON %' THEN 
             SUBSTRING_INDEX(SUBSTRING_INDEX(DESCRIPTION, ' ', -4), ' ', 2) -- Extracts "CITY STATE"
        WHEN DESCRIPTION LIKE 'ATM CASH %' THEN 
             SUBSTRING_INDEX(SUBSTRING_INDEX(DESCRIPTION, ' ATM ID ', 1), ' ', -3) -- Address snippet
        ELSE 'ONLINE/TRANSFER'
    END) AS GEOGRAPHY,
    -- Extract Zelle Memo / Purpose
    TRIM(CASE 
        WHEN DESCRIPTION LIKE 'ZELLE % REF # %' THEN 
             CASE 
                WHEN SUBSTRING_INDEX(DESCRIPTION, ' REF # ', -1) LIKE '% %' THEN 
                    SUBSTRING(SUBSTRING_INDEX(DESCRIPTION, ' REF # ', -1), LOCATE(' ', SUBSTRING_INDEX(DESCRIPTION, ' REF # ', -1)) + 1)
                ELSE NULL 
             END
        ELSE NULL 
    END) AS TRANSACTION_MEMO,
    -- Extract Trace/Reference IDs
    TRIM(CASE 
        WHEN DESCRIPTION LIKE '% REF # %' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(DESCRIPTION, ' REF # ', -1), ' ', 1)
        WHEN DESCRIPTION LIKE 'PURCHASE AUTHORIZED ON %' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(DESCRIPTION, ' ', -3), ' ', 1)
        ELSE NULL
    END) AS TRACE_ID,
    TRIM(CASE 
        WHEN DESCRIPTION LIKE 'CITI CARD %' THEN 'CITI'
        WHEN DESCRIPTION LIKE 'AMERICAN EXPRESS %' THEN 'AMEX'
        WHEN DESCRIPTION LIKE 'DISCOVER %' THEN 'DISCOVER'
        WHEN DESCRIPTION LIKE 'APPLECARD %' THEN 'GSBANK'
        WHEN DESCRIPTION LIKE 'CAPITAL ONE %' THEN 'CAPITAL ONE'
        ELSE NULL
    END) AS ASSOCIATED_BANK
FROM (
    select 
        t.transaction_id
        ,t.transaction_date
        ,t.transaction_type
        ,t.transaction_amount
        ,t.description
        ,mc.catg_name
        ,msc.sub_catg_name
    from transactions t
    left join mcc_category mc on mc.catg_id = t.catg_id
    left join mcc_sub_category msc on msc.sub_catg_id = t.sub_catg_id
    where bank_id = 7 -- Wells Fargo ID
) AS wells_data;

-- =============================================================================
-- 3. AMERICAN EXPRESS PATTERN (Hybrid Narrative/Extended)
-- =============================================================================
SELECT 
    DESCRIPTION AS RAW_DESC,
    EXT_DESCRIPTION AS RAW_EXT_DESC,
    -- 1. Identify the Channel
    CASE 
        WHEN DESCRIPTION LIKE 'MOBILE PAYMENT%' THEN 'PAYMENT'
        WHEN DESCRIPTION LIKE 'APLPAY %' THEN 'APPLE PAY'
        WHEN DESCRIPTION LIKE '%.COM%' OR EXT_DESCRIPTION LIKE '%.COM%' THEN 'ONLINE PURCHASE'
        ELSE 'CREDIT CARD'
    END AS PAYMENT_CHANNEL,
    -- 2. Extract Merchant (Prioritize cleaned DESCRIPTION to avoid IDs/Emails in Ext_Description)
    TRIM(CASE 
        WHEN DESCRIPTION LIKE 'MOBILE PAYMENT%' THEN 'AMERICAN EXPRESS'
        ELSE TRIM(REGEXP_REPLACE(
                 REPLACE(REPLACE(DESCRIPTION, 'APLPAY ', ''), 'TST* ', ''),
                 '[0-9]{4,}.*|[ ]{2,}.*', ''
             ))
    END) AS ENTITY_NAME,
    -- 3. Extract Geography (Uses fixed-width gap at the end of Description)
    TRIM(CASE 
        WHEN DESCRIPTION LIKE 'MOBILE PAYMENT%' THEN 'ONLINE'
        ELSE TRIM(CONCAT(
                 TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(DESCRIPTION, '  ', -2), '  ', 1)), 
                 ' ', 
                 TRIM(SUBSTRING_INDEX(DESCRIPTION, '  ', -1))
             ))
    END) AS GEOGRAPHY,
    -- 4. Extract Category (Amex often provides explicit category labels in Ext_Description)
    TRIM(CASE 
        WHEN EXT_DESCRIPTION LIKE '%DESCRIPTION : %' THEN 
            SUBSTRING_INDEX(SUBSTRING_INDEX(EXT_DESCRIPTION, 'DESCRIPTION : ', -1), ' PRICE', 1)
        WHEN EXT_DESCRIPTION LIKE '%FAST FOOD REST%' THEN 'DINING'
        WHEN EXT_DESCRIPTION LIKE '%GROCERY STORE%' THEN 'GROCERIES'
        WHEN EXT_DESCRIPTION LIKE '%DISCOUNT STORE%' THEN 'SHOPPING'
        WHEN EXT_DESCRIPTION LIKE '%LODGING%' THEN 'TRAVEL'
        WHEN EXT_DESCRIPTION LIKE '%PASSENGER TICKET%' THEN 'AIRLINE'
        ELSE 'RETAIL'
    END) AS DETAILED_CATEGORY,
    -- 5. Extra Metadata: Passenger Name / Flight Info
    CASE 
        WHEN EXT_DESCRIPTION LIKE '%PASSENGER NAME : %' THEN 
            SUBSTRING_INDEX(SUBSTRING_INDEX(EXT_DESCRIPTION, 'PASSENGER NAME : ', -1), ' TICKET', 1)
        ELSE NULL 
    END AS FLIGHT_PASSENGER
FROM (
    SELECT 
        t.transaction_id,
        t.transaction_date,
        t.transaction_type,
        t.transaction_amount,
        t.description,
        t.ext_description,
        mc.catg_name,
        msc.sub_catg_name
    FROM BudgetApp.TRANSACTIONS t
    LEFT JOIN BudgetApp.MCC_CATEGORY mc ON mc.catg_id = t.catg_id
    LEFT JOIN BudgetApp.MCC_SUB_CATEGORY msc ON msc.sub_catg_id = t.sub_catg_id
    WHERE t.BANK_ID = (SELECT BANK_ID FROM BudgetApp.BANKS WHERE BANK_NAME = 'American Express')
) AS amex_data;

-- =============================================================================
-- 4. DISCOVER BANK PATTERN (Narrative with Suffix Location)
-- =============================================================================
SELECT 
    DESCRIPTION AS RAW_DESC,
    EXT_DESCRIPTION AS RAW_EXT_DESC,
    -- 1. Identify the Channel
    CASE 
        WHEN DESCRIPTION LIKE 'INTERNET PAYMENT%' OR EXT_DESCRIPTION = 'PAYMENTS AND CREDITS' THEN 'PAYMENT'
        WHEN DESCRIPTION LIKE 'AMAZON%' OR DESCRIPTION LIKE 'AMZN%' THEN 'ONLINE PURCHASE'
        WHEN EXT_DESCRIPTION = 'GASOLINE' THEN 'GAS STATION'
        ELSE 'CREDIT CARD'
    END AS PAYMENT_CHANNEL,
    -- 2. Extract Merchant (Clean up Amazon strings and strip Store IDs, Locations, and Phone Numbers)
    TRIM(REGEXP_REPLACE(
        REGEXP_REPLACE(DESCRIPTION, 'AMAZON MKTPL\\*[^ ]+|AMAZON\\.COM\\*[^ ]+|AMAZON MKTPLACE PMTS', 'AMAZON'), -- Clean Amazon Marketplace
        '([ ]+([A-Z.]{2,}( [A-Z.]{2,})?) [A-Z]{2}([0-9]+)?$)|([ ]+([0-9#]{3,}|[0-9]{2,}-[0-9]+).*)|(AMZN.COM/BILL.*)', ''  -- Strip Location, Store IDs, Phone numbers, and URLs
    )) AS ENTITY_NAME,
    -- 3. Extract Geography (City and State are usually the last two/three words before the state code)
    CASE 
        WHEN DESCRIPTION LIKE '% PAYMENT %' THEN 'ONLINE'
        WHEN DESCRIPTION REGEXP '([A-Z.]{2,}( [A-Z.]{2,})? [A-Z]{2}([0-9]+)?$)' THEN 
            TRIM(REGEXP_SUBSTR(DESCRIPTION, '([A-Z.]{2,}( [A-Z.]{2,})? [A-Z]{2}([0-9]+)?$)'))
        ELSE 'UNKNOWN'
    END AS GEOGRAPHY,
    -- 4. Extract Category (Discover Ext_Description is high quality)
    UPPER(TRIM(EXT_DESCRIPTION)) AS DETAILED_CATEGORY,
    -- 5. Extract Reference/Trace ID (Discover often buries IDs in the Description)
    REGEXP_SUBSTR(DESCRIPTION, '[0-9]{10,}') AS TRACE_ID
FROM (
    SELECT 
        t.transaction_id,
        t.transaction_date,
        t.transaction_type,
        t.transaction_amount,
        t.description,
        t.ext_description,
        mc.catg_name,
        msc.sub_catg_name
    FROM BudgetApp.TRANSACTIONS t
    LEFT JOIN BudgetApp.MCC_CATEGORY mc ON mc.catg_id = t.catg_id
    LEFT JOIN BudgetApp.MCC_SUB_CATEGORY msc ON msc.sub_catg_id = t.sub_catg_id
    WHERE t.BANK_ID = 3
) AS discover_data
;

-- =============================================================================
-- 5. CAPITAL ONE PATTERN (Description & Provided Sub-Category)
-- =============================================================================
SELECT 
    DESCRIPTION AS RAW_DESC,
    SUB_CATG_NAME AS RAW_SUB_CATG,
    -- 1. Identify the Channel
    CASE 
        WHEN DESCRIPTION LIKE 'CAPITAL ONE MOBILE PYMT%' THEN 'PAYMENT'
        WHEN SUB_CATG_NAME = 'AIRFARE' OR DESCRIPTION LIKE '%AIR%' THEN 'AIRLINE'
        WHEN SUB_CATG_NAME = 'GAS/AUTOMOTIVE' OR DESCRIPTION LIKE 'NTTA%' THEN 'TRANSPORT/GAS'
        WHEN DESCRIPTION LIKE 'COT*%' THEN 'TRAVEL PORTAL'
        ELSE 'CREDIT CARD'
    END AS PAYMENT_CHANNEL,

    -- 2. Extract Merchant (Standardize airlines and clean up prefixes)
    TRIM(CASE
        WHEN DESCRIPTION LIKE 'SOUTHWES%' THEN 'SOUTHWEST AIRLINES'
        WHEN DESCRIPTION LIKE 'AMERICAN %' THEN 'AMERICAN AIRLINES'
        WHEN DESCRIPTION LIKE 'UNITED %' THEN 'UNITED AIRLINES'
        WHEN DESCRIPTION LIKE 'ETIHAD %' THEN 'ETIHAD AIRWAYS'
        WHEN DESCRIPTION LIKE 'QATAR AIR%' THEN 'QATAR AIRWAYS'
        WHEN DESCRIPTION LIKE 'KRISHNA INSTITUTE%' THEN 'KRISHNA INSTITUTE OF MEDICAL SCIENCES'
        WHEN DESCRIPTION = 'COT*HTL' THEN 'CAPITAL ONE TRAVEL - HOTEL'
        WHEN DESCRIPTION = 'COT*FLT' THEN 'CAPITAL ONE TRAVEL - FLIGHT'
        ELSE TRIM(REGEXP_REPLACE(
                 REPLACE(REPLACE(DESCRIPTION, 'TST* ', ''), 'COT*', ''),
                 '[0-9]{10,}.*|[ ]{2,}.*', ''
             ))
    END) AS ENTITY_NAME,

    -- 3. Extract Category (Leverage provided Sub_catg_name)
    UPPER(TRIM(SUB_CATG_NAME)) AS DETAILED_CATEGORY,

    -- 4. Extract Reference/Trace ID (10+ digit numbers common in Airlines/Tolls)
    REGEXP_SUBSTR(DESCRIPTION, '[0-9]{10,}') AS TRACE_ID
FROM (
    SELECT 
        t.description,
        msc.sub_catg_name
    FROM BudgetApp.TRANSACTIONS t
    LEFT JOIN BudgetApp.MCC_SUB_CATEGORY msc ON msc.sub_catg_id = t.sub_catg_id
    WHERE t.BANK_ID = (
        SELECT BANK_ID FROM BudgetApp.BANKS 
        WHERE BANK_NAME LIKE '%Capital One%' AND IS_CURRENT = 1 LIMIT 1
    )
) AS capone_data;

-- =============================================================================
-- 6. APPLE CARD PATTERN (Narrative with Full Address & Installments)
-- =============================================================================
SELECT 
    DESCRIPTION AS RAW_DESC,
    SUB_CATG_NAME AS RAW_SUB_CATG,
    -- 1. Identify the Channel
    CASE 
        WHEN DESCRIPTION LIKE 'ACH DEPOSIT%' THEN 'PAYMENT'
        WHEN DESCRIPTION LIKE 'MONTHLY INSTALLMENTS%' THEN 'INSTALLMENT'
        WHEN DESCRIPTION LIKE 'DAILY CASH ADJUSTMENT%' THEN 'REWARDS'
        WHEN DESCRIPTION LIKE '%(RETURN)%' THEN 'RETURN'
        WHEN DESCRIPTION LIKE 'APPLE.COM/BILL%' THEN 'DIGITAL SERVICES'
        ELSE 'CREDIT CARD'
    END AS PAYMENT_CHANNEL,
    -- 2. Extract Merchant (Clean up prefixes and strip address/phone/zip/metadata)
    TRIM(REGEXP_REPLACE(
        REPLACE(REPLACE(REPLACE(REPLACE(DESCRIPTION, 'TST* ', ''), 'GFM*', ''), 'ABC*P ', ''), 'SP AG1 ', ''),
        '([ ]+(#|[0-9]{1,}[A-Z]|[0-9]{2,}|AND |LIMITED|PKWY|STREET|WAY|RD|DR|SUITE|BLVD|ONE APPLE PARK|GOFUNDME.COM|\\().*)', ''
    )) AS ENTITY_NAME,
    -- 3. Extract Geography (City and State before ZIP and USA)
    TRIM(CASE 
        WHEN DESCRIPTION REGEXP '[A-Z. ]+ [0-9]{5} [A-Z]{2} USA' THEN
            REGEXP_REPLACE(REGEXP_SUBSTR(DESCRIPTION, '[A-Z. ]+ [0-9]{5} [A-Z]{2} USA'), '[0-9]{5} | USA', '')
        WHEN DESCRIPTION LIKE 'APPLE.COM/BILL%' THEN 'CUPERTINO CA'
        ELSE 'ONLINE'
    END) AS GEOGRAPHY,
    -- 4. Category
    UPPER(TRIM(SUB_CATG_NAME)) AS DETAILED_CATEGORY,
    -- 5. Extract Reference (Extracting installment info like "2 OF 12")
    CASE 
        WHEN DESCRIPTION LIKE 'MONTHLY INSTALLMENTS%' THEN REGEXP_SUBSTR(DESCRIPTION, '[0-9]+ OF [0-9]+')
        ELSE NULL 
    END AS INSTALLMENT_PLAN
FROM (
    SELECT 
        t.description,
        msc.sub_catg_name
    FROM BudgetApp.TRANSACTIONS t
    LEFT JOIN BudgetApp.MCC_SUB_CATEGORY msc ON msc.sub_catg_id = t.sub_catg_id
    WHERE t.BANK_ID = 5
) AS apple_data;

-- =============================================================================
-- 7. CITI BANK PATTERN (Narrative Description)
-- =============================================================================
SELECT 
    DESCRIPTION AS RAW_DESC,
    -- 1. Identify the Channel
    CASE 
        WHEN DESCRIPTION LIKE '%PAYMENT%THANK YOU%' THEN 'PAYMENT'
        WHEN DESCRIPTION LIKE 'WWW %' OR DESCRIPTION LIKE '%.COM%' THEN 'ONLINE PURCHASE'
        WHEN DESCRIPTION LIKE '%GAS %' OR DESCRIPTION REGEXP 'TEXACO|BUC-EE|EXXON|SHELL|CONOCO|QT|7-ELEVEN|LOVE\'S' THEN 'GAS STATION'
        ELSE 'CREDIT CARD'
    END AS PAYMENT_CHANNEL,
    -- 2. Extract Merchant (Clean up prefixes and strip address/phone/ids)
    TRIM(CASE 
        WHEN DESCRIPTION LIKE '%PAYMENT%THANK YOU%' THEN 'CITI BANK'
        WHEN DESCRIPTION LIKE 'COSTCO GAS %' THEN 'COSTCO GAS'
        WHEN DESCRIPTION LIKE 'COSTCO WHSE%' THEN 'COSTCO'
        WHEN DESCRIPTION LIKE 'WWW COSTCO COM%' THEN 'COSTCO ONLINE'
        WHEN DESCRIPTION LIKE 'USPS.COM%' THEN 'USPS ONLINE'
        WHEN DESCRIPTION LIKE 'PROGRESSIVE *INSURANCE%' THEN 'PROGRESSIVE'
        ELSE TRIM(REGEXP_REPLACE(
                 REPLACE(REPLACE(REPLACE(REPLACE(DESCRIPTION, 'TST* ', ''), 'PY * ', ''), 'SQ * ', ''), 'SP * ', ''),
                 '([ ]+(#|[0-9]{4,}|[0-9]{2,}-[0-9]+|[A-Z. ]+ [A-Z]{2}$|800-|888-|877-).*)', ''
             ))
    END) AS ENTITY_NAME,
    -- 3. Extract Geography (Usually trailing City State)
    TRIM(CASE 
        WHEN DESCRIPTION LIKE '%PAYMENT%THANK YOU%' THEN 'ONLINE'
        WHEN DESCRIPTION REGEXP '[A-Z. ]+ [A-Z]{2}$' THEN 
            REGEXP_SUBSTR(DESCRIPTION, '[A-Z. ]+ [A-Z]{2}$')
        ELSE 'ONLINE/TRANSFER'
    END) AS GEOGRAPHY,
    -- 4. Category
    'MERCHANDISE' AS DETAILED_CATEGORY
FROM (
    SELECT 
        t.description
    FROM BudgetApp.TRANSACTIONS t
    WHERE t.BANK_ID = 6
) AS citi_data;
