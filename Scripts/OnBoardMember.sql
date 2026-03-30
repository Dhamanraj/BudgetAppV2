CALL BudgetApp.Onboard_Member(
    'Dhaman Kumar',         -- p_FirstName: Member's first name
    'Kakke',              	-- p_LastName: Member's last name
    'Male',               	-- p_Gender: Gender (e.g., 'Male', 'Female', 'Other')
    '1996-05-31',         	-- p_DOB: Date of Birth (Format: 'YYYY-MM-DD') -> Will be encrypted
    '2911 Central Expy',    -- p_Address1: Primary street address
    'Apt 5204',          	-- p_Address2: Apartment, suite, or NULL
    'Melissa',             	-- p_City: City name
    'TX',                 	-- p_State: State abbreviation (e.g., 'TX')
    '75454',              	-- p_ZipCode: 5 or 10 digit zip code
    'USA',                	-- p_Country: Country name
    'WellsFargo Bank',     		-- p_BankName: Name of the financial institution
    'Checking Debit',           	-- p_AccountType: Type of account (e.g., 'Checking', 'Savings')
    'Visa',           -- p_CardType: Brand of card (e.g., 'Visa', 'Amex', 'Mastercard')
    '4342580147229124',   	-- p_FullCardNumber: The 15 or 16 digit card number -> Will be encrypted
    '06/28',              	-- p_ExpDate: Expiration date (MM/YY) -> Will be encrypted
    '784',                	-- p_CVV: 3 or 4 digit security code -> Will be encrypted
    '9849050888', 			-- p_EncryptionKey: The secret key used for AES_ENCRYPT logic
    'SYSTEM_ADMIN'        	-- p_AddedUser: The username or system ID performing the insert
);


Select * from CARDS c ;