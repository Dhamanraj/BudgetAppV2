CALL BudgetApp.Load_CapitalOne_Statement(
    'Dhaman Kumar',      -- p_UserName: The First Name of the member (must be IS_CURRENT = 1 in MEMBERS table)
    'CapitalOne Bank',  -- p_BankName: The name of the bank (must be IS_CURRENT = 1 in BANKS table)
    '2548'         -- p_CardLast4: The last 4 digits of the card (must be IS_CURRENT = 1 in CARDS table)
);


CALL BudgetApp.Load_Amex_Statement(
    'Dhaman Kumar',      	-- p_UserName: The First Name of the member (must be IS_CURRENT = 1 in MEMBERS table)
    'American Express', 	-- p_BankName: The name of the bank (must be IS_CURRENT = 1 in BANKS table)
    '1004'         			-- p_CardLast4: The last 4 digits of the card (must be IS_CURRENT = 1 in CARDS table)
);


CALL BudgetApp.Load_Discover_Statement(
    'Dhaman Kumar',      	-- p_UserName: The First Name of the member (must be IS_CURRENT = 1 in MEMBERS table)
    'Discover Bank', 	-- p_BankName: The name of the bank (must be IS_CURRENT = 1 in BANKS table)
    '2124'         			-- p_CardLast4: The last 4 digits of the card (must be IS_CURRENT = 1 in CARDS table)
);


CALL BudgetApp.Load_Apple_Statement(
    'Dhaman Kumar',      	-- p_UserName: The First Name of the member (must be IS_CURRENT = 1 in MEMBERS table)
    'Apple Bank', 	-- p_BankName: The name of the bank (must be IS_CURRENT = 1 in BANKS table)
    '4708'         			-- p_CardLast4: The last 4 digits of the card (must be IS_CURRENT = 1 in CARDS table)
);


CALL BudgetApp.Load_Citi_Statement(
    'Dhaman Kumar',      	-- p_UserName: The First Name of the member (must be IS_CURRENT = 1 in MEMBERS table)
    'Citi Bank', 	-- p_BankName: The name of the bank (must be IS_CURRENT = 1 in BANKS table)
    '7610'         			-- p_CardLast4: The last 4 digits of the card (must be IS_CURRENT = 1 in CARDS table)
);

CALL BudgetApp.Load_WellsFargo_Debit_Statement(
    'Dhaman Kumar',      	-- p_UserName: The First Name of the member (must be IS_CURRENT = 1 in MEMBERS table)
    'WellsFargo Bank', 	-- p_BankName: The name of the bank (must be IS_CURRENT = 1 in BANKS table)
    '9124'         			-- p_CardLast4: The last 4 digits of the card (must be IS_CURRENT = 1 in CARDS table)
);


Select * from banks t ;

Select * FROM TRANSACTIONS t 
order by ADDED_DATETIME desc;

Commit;