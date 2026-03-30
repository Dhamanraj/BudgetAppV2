Select 
	COUNT(*)
	,DATE_FORMAT(TRANSACTION_DATE,'%Y-%m-01') Mnth
	,t.Card_id
	,c.LAST_4 
	,b.BANK_NAME 
from TRANSACTIONS t 
join CARDS c 
	on c.CARD_ID = t.CARD_ID 
join BANKS b 
	on b.BANK_ID = c.BANK_ID 
group by DATE_FORMAT(TRANSACTION_DATE,'%Y-%m-01') 
	,t.Card_id
	,c.LAST_4 
	,b.BANK_NAME 
order by t.Card_id
	,c.LAST_4 
	,b.BANK_NAME 
	,DATE_FORMAT(TRANSACTION_DATE,'%Y-%m-01') 
	;